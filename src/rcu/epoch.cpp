#include "epoch.h"

#include <map>
#include <type_traits>

/* A traditional epoch design tracks up to three epochs at a time:

   active -- new transactions always join the active epoch
   
   closed -- no new transactions can join, but the epoch may still
   contain existing transactions.
   
   quiesced -- all transactions have left and the epoch---along with
   all resources freed during it---can be reclaimed safely.
   
   When a thread decides resources should be reclaimed, it requests to
   advance the epoch number. If there are no active transactions in
   epoch N when epoch N+1 begins, the epoch N transitions directly to
   "quiesced" status and is reclaimed immediately. Otherwise, epoch N
   transitions to "closed" until the last straggler leaves.

   Only one closed epoch is allowed to exist at a time, so epoch N+1
   cannot begin until the last straggler leaves epoch N-1.

   This approach provides the desired semantics, but has the
   disadvantage that epoch changes tend to flag all active threads as
   stragglers---even if they quiesce frequently---because they will
   usually be in an transaction when each epoch ends.

   We can improve the situation by allowing two closed epochs instead
   of one. Everything stays the same except that the thread which
   opens epoch N+1 attempts to reclaim epoch N-1 instead of epoch
   N. By then, only true stragglers will remain in epoch N-1, because
   active threads will have moved to epoch N. In the unlikely event
   that stragglers do prevent direct reclamation of epoch N-1, epoch
   N+2 cannot begin until the last straggler finishes.

   This approach virtually eliminates the communication overhead of
   ending an epoch, at the cost of potentially introducing extra
   latency between the time resources are marked for release and the
   time when they are actually reclaimed. However, this should not be
   a problem in practic: the reduced cost of epoch changes allows them
   to occur more frequently, and the worst case is actually the same:
   epoch N-1 cannot be reclaimed until all transactions, in flight at
   the time epoch N opened, have ended.
*/

#ifdef EPOCH_LOGGING
#define ELOG(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__)
#else
#define ELOG(msg, ...)
#endif

struct epoch_mgr::thread_state {
    epoch_num begin;
    epoch_num end;
    bool straggler;
    bool initialized;
    void *cookie;
};

static_assert(std::is_pod<epoch_mgr::thread_state>::value,
              "epoch_mgr::thread_state must be POD");

struct epoch_mgr::private_state {
    void *operator new(size_t sz) {
        void *ptr;
        int err = posix_memalign(&ptr, __alignof__(private_state), sz);
        THROW_IF(err, os_error, errno, "posix_memalign failed");
        return ptr;
    }
    void operator delete(void* ptr) {
        ::free(ptr);
    }
    
    /* these are read frequently, updated rarely. Make sure they go in
       their own cache line to avoid false sharing.
     */
    aligner<64> _align1;
    epoch_num begin = 1;
    epoch_num end = 1;
    aligner<64> _align2;

    bool ready_for_safe_point = true;
    
    uint64_t nstragglers = 0;
    std::map<pthread_t, thread_state*> threads;
    void *cooling_cookie = 0;
    void *cold_cookie = 0;
    
    /* Every thread registered with the epoch manager must be
       automatically deregistered when it dies, lest we allow a
       careless user to leak resources.
    */
    pthread_key_t pthread_key;
};

static
epoch_mgr::thread_state *
get_tls(epoch_mgr *em)
{
    return (epoch_mgr::thread_state*) em->cb.get_tls(em->cb.cookie)->data;    
}

/* Free all resources that became unreachable before the last safe
   point. It is safe to install a new safe point any time after this
   function returns.

   NOTE: this function does *not* acquire the rcu_mutex. Only one
   thread (the last straggler) should ever call it at any given time,
   because ready_for_safe_point will remain false until this function
   has been called.
 */
static
void
global_quiesce(epoch_mgr *em)
{
    auto *s = em->state;
    void *cookie = 0;
    std::swap(cookie, s->cold_cookie);
    if (cookie)
        em->cb.epoch_reclaimed(em->cb.cookie, cookie);
    
    s->ready_for_safe_point = true;
}

static
void
straggler_ended(epoch_mgr *em)
{
    ASSERT (volatile_read(em->state->nstragglers) > 0);
    if (not __sync_add_and_fetch(&em->state->nstragglers, -1)) {
        ELOG("Quiesced: last straggler finished\n");
        global_quiesce(em);
    }
}

static
void
straggler_check(epoch_mgr *em, epoch_mgr::thread_state *self, epoch_mgr::epoch_num tmp)
{
    if (volatile_read(em->state->end) < tmp) {
        /* The epoch change is still in progress. We may have
           placed our end timestamp in time to avoid being flagged
           as a straggler. Or maybe not. Wait for the epoch change
           to complete before continuing.
        */
        em->mutex.lock();
        em->mutex.unlock();
    }

    if (volatile_read(self->straggler)) {
        self->straggler = false;
        straggler_ended(em);
    }
}

epoch_mgr::epoch_num
epoch_mgr::get_cur_epoch()
{
    return state->begin;
}

bool
epoch_mgr::new_epoch()
{
    bool reclaim_directly = false;
    XDEFER_IF(reclaim_directly, global_quiesce(this));
    
    mutex.lock();
    DEFER(mutex.unlock());
    if (not state->ready_for_safe_point) 
        return false;

    ASSERT(not state->cold_cookie);
    
    // for straggler tracking...
    thread_state *stragglers[state->threads.size()];
    size_t nstragglers = 0;

    /* Epoch N+1 is opening.
       
       Epoch N is transitioning to "cooling" status
       
       Epoch N-1 is transitioning to "cold" status
       
       Epoch N-2 is already dead and has been reclaimed, which is why
       we were allowed to open a new epoch.
     */
    auto N = state->begin;
    
    volatile_write(state->begin, N+1);
    
    state->ready_for_safe_point = false;    
    void *cookie = cb.epoch_ended(cb.cookie, N);

    /* NOTE: it would be tempting to check for the case where no
       threads are active, in hopes of reclaiming all three epochs
       (active, cooling, cold) immediately. However, we could be
       racing with threads in thread_enter(), who will eventually join
       the epochs. Because of this race, and because we expect that
       there will usually be at least one active thread in the system
       at each epoch change, we leave the epoch alone until it reaches
       cold status naturally. 
     */
    for (auto &entry : state->threads) {
        auto *t = entry.second;
        cookie = cb.epoch_ended_thread(cb.cookie, cookie, t->cookie);
        
        /* Epoch N-1 is transitioning to cold status. Flag any
           stragglers that would prevent its reclamation.
           
           Straggler threads have active transactions that started
           during epoch N-1. Anything older than that is due to a
           race with thread_enter() and can be ignored (the other
           thread is responsible to detect the race and retry).

           Note that a race could cause us to flag a thread as a
           straggler even though it has not actually finished starting
           its transaction (and therefore cannot have accessed any
           resources). It has no way to detect that situation,
           however, and the only impact of the false positive is to
           slow things down a bit.
        */
        auto begin = volatile_read(t->begin);
        auto end = volatile_read(t->end);
        if (begin == N-1 and begin > end) 
            stragglers[nstragglers++] = t;
    }

    volatile_write(state->cold_cookie, state->cooling_cookie);
    state->cooling_cookie = cookie;
    if (nstragglers) {
        // notify stragglers
        volatile_write(state->nstragglers, nstragglers);
        for (size_t i=0; i < nstragglers; i++)
            volatile_write(stragglers[i]->straggler, true);
    }

    volatile_write(state->end, N+1);
    reclaim_directly = not nstragglers;
    return true;
}

bool
epoch_mgr::new_epoch_possible()
{
    return volatile_read(state->ready_for_safe_point);
}

void
epoch_mgr::thread_init()
{
    auto *self = get_tls(this);
    DIE_IF (self->initialized, "Thread already registered");
    
    auto tid = pthread_self();
    ELOG("Registering thread %ld with epoch manager %p\n",
         (long) (uintptr_t) tid, this);

    self->initialized = true;
    ASSERT(not self->begin);
    ASSERT(not self->end);
    ASSERT(not self->straggler);

    mutex.lock();
    DEFER(mutex.unlock());

    // first time?
    if (not state) {
        auto *tmp = new private_state;
        DEFER_UNLESS(success, delete tmp);
        
        auto fini = [](void *arg)->void {
            auto *em = (epoch_mgr*) arg;
            em->thread_fini();
        };
        
        cb.global_init(cb.cookie);
        state = tmp;
        success = true;
        
        int err = pthread_key_create(&state->pthread_key, fini);
        DIE_IF(err, "pthread_key_create failed with errno=%d", errno);
    }
    
    // make the system aware of us
    state->threads.insert(std::make_pair(tid, self));
    DEFER_UNLESS(success, state->threads.erase(tid));
        
    self->cookie = cb.thread_registered(cb.cookie);
    XDEFER_UNLESS(success, cb.thread_deregistered(cb.cookie, self->cookie));
    
    // automatically deregister on thread exit
    int err = pthread_setspecific(state->pthread_key, this);
    THROW_IF(err, os_error, errno, "pthread_setspecific failed");
    success = true;
}

void
epoch_mgr::thread_fini()
{
    DIE_IF(not thread_initialized(), "Thread not registered");
    
    ELOG("Deregistering thread %ld with RCU\n", (long) (uintptr_t) pthread_self());
    if (thread_is_active())
        thread_exit();
    
    auto *self = get_tls(this);
    pthread_setspecific(state->pthread_key, NULL);

    /* Defer reclamation of dead epochs until after the mutex has been
       released (DEFER actions execute in reverse order).
     */
    void *active_cookie = NULL;
    void *cooling_cookie = NULL;
    XDEFER_IF(active_cookie, cb.epoch_reclaimed(cb.cookie, active_cookie));
    XDEFER_IF(cooling_cookie, cb.epoch_reclaimed(cb.cookie, cooling_cookie));
    
    mutex.lock();
    DEFER(mutex.unlock());

    cb.thread_deregistered(cb.cookie, self->cookie);
    self->cookie = NULL;
    self->initialized = false;
    
    // remove us from the system
    auto found = state->threads.erase(pthread_self());
    ASSERT(found);
    if (not state->threads.empty())
        return;
    
    /* Nobody left, clean everything up.

       There cannot currently be a cold epoch, because there can be no
       stragglers after the last thread leaves.

       Reclaim the cooling epoch (if present).
           
       Close and reclaim the active epoch.
    */
    ELOG("Global quiescent point: all threads deregistered\n");
    ASSERT(not state->cold_cookie);
    epoch_num N = state->begin;
    state->begin = state->end = N+1;
    active_cookie = cb.epoch_ended(cb.cookie, N);
    std::swap(cooling_cookie, state->cooling_cookie);
}

bool
epoch_mgr::thread_initialized()
{
    return get_tls(this)->initialized;
}

epoch_mgr::epoch_num
epoch_mgr::thread_enter()
{
    DIE_IF(not thread_initialized(), "Thread not initialized");
    DIE_IF(thread_is_active(), "Thread already active");

    /* A thread is considered active if [begin] > [end]. Because this
       may not be our first transaction of the epoch, we may need to
       set [end]=0 before updating [begin]

       If this operation races with an epoch end, it will make this
       thread look like a straggler from a previous epoch.
     */
    auto *self = get_tls(this);
    ASSERT(self->begin <= self->end);

    /* This operation here is racy.

       Any number of epochs could pass after we read state->begin.

       Once we set end=0, an epoch change could see begin > end, with
       begin < tmp.

       Or, we could be delayed reading state->begin the second time,
       causing us to become a straggler the system has accounted for.
       
       We could be smarter about assigning timestamps (e.g. make sure
       state->begin=tmp so we don't appear to be starting in some old
       epoch after setting [end]=0), but that would increase the
       window of vulnerability while not eliminating the corner case.

       Instead, make the change as quickly as possible, then clean up
       the mess if necessary.
     */
 retry:
    auto tmp = volatile_read(state->begin);
    volatile_write(self->end, 0);
    volatile_write(self->begin, tmp);
    
    // raced?
    auto tmp2 = volatile_read(state->begin);
    switch(tmp2 - tmp) {
    case 0:
        // no race!
        break;
        
    case 1:
        /* Our epoch is now cooling. This is normally harmless,
           because the transition to "cold" is guaranteed to notice
           and flag us as a straggler if we haven't finished by then.

           There is one danger, though: if no other threads were
           active when the epoch changed, our epoch might go straight
           from "active" to "dead" and be reclaimed. We have no way to
           detect if this happens; instead, forbid the epoch change
           routine from reclaiming a just-closed epoch directly.
        */
        break;
        
    case 2:
        /* Our epoch is now cold, or becoming so. We want to bail out
           rather than contribute to the straggler problem, but the
           epoch change routine may have already noticed. If so, run
           the straggler protocol before retrying.
         */
        volatile_write(self->end, tmp2);
        straggler_check(this, self, tmp2);
        goto retry;

    default:
        /* We were so slow that the epoch change machinery doesn't
           even consider us a threat. Just retry and hope we're faster
           next time.
         */
        volatile_write(self->end, tmp2);
        goto retry;
    }

    /* All done! */
    __sync_synchronize();
    return tmp;
}

bool
epoch_mgr::thread_is_active()
{
    auto *self = get_tls(this);
    return self->begin > self->end;
}

epoch_mgr::epoch_num
epoch_mgr::thread_quiesce()
{
    DIE_IF(not thread_is_active(), "Thread not currently active");
    auto *self = get_tls(this);

    if (volatile_read(self->straggler)) {
        /* Our status as straggler makes it a lot easier to start the
           new transaction, because we know the current epoch cannot
           close before we call straggler_ended().
         */
        __sync_synchronize();
        self->straggler = false;
        self->end = 0;
        self->begin = volatile_read(state->begin);
        straggler_ended(this);
    }
    
    return self->begin;
}

void
epoch_mgr::thread_exit()
{
    DIE_IF(not thread_is_active(), "Thread not currently active");
    auto *self = get_tls(this);
    
    __sync_synchronize();
    auto tmp = volatile_read(state->begin);
    volatile_write(self->end, tmp);
    if (tmp < self->begin+2) {
        /* If we finished in epoch N or N-1, there is no problem: the
           system only flags transactions in epoch N-2 as stragglers.

           Have to check for races, though: we may have written our
           end mark after the epoch closed.
         */
        tmp = volatile_read(state->end);
        if (tmp < self->begin+2)
            return; // all good!
    }

    // might be a straggler
    straggler_check(this, self, tmp);
}
