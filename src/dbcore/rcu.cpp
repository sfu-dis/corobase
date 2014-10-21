#include "rcu.h"

#include "epoch.h"
#include "sm-exceptions.h"
#include "sm-common.h"
#include "size-encode.h"
#include "sm-gc.h"
#include "sm-defs.h"

#include "../rcu-wrapper.h" // for delete_entry

#include <stdint.h>
#include <pthread.h>
#include <map>
#include <cstdio>
#include <stdlib.h>
#include <new>
#include <algorithm>

namespace RCU {
#if 0
} // for emacs
#endif

#ifdef RCU_LOGGING
#define RCU_LOG(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__)
#else
#define RCU_LOG(msg, ...)
#endif

/* TODO: set up fancy slab allocators using the above pointers...
 */

/* Thread control block for RCU.
 */
struct rcu_tcb {
        
    static rcu_tcb* tls() {
        static __thread rcu_tcb local;
        return &local;
    }

    bool initialized;

    /* All recently-freed memory goes on this list; the RCU subsystem
       will swap out the [pointer_list] at each system-wide quiescent
       point and place the entire list in an internal cool-down
       area. It (and its contents) will be freed at the following
       system-wide quiescent point, when all references have died.
     */
    pointer_list * free_list;

    /* Trigger a collection if the number of (to be) freed objects or
       bytes passes a certain threshold. Track free counts locally to
       avoid contention, and occasionally update the global total.
     */
    size_t free_count;
    size_t free_bytes;
};



pointer_list *rcu_zombie_lists = 0;
rcu_gc_info rcu_global_gc_stats = {0,0,0};

static size_t const RCU_THREAD_GC_THRESHOLD_NOBJ = 1000;
static size_t const RCU_THREAD_GC_THRESHOLD_NBYTES = 10*1024*1024;

size_t rcu_gc_threshold_nobj = 10*RCU_THREAD_GC_THRESHOLD_NOBJ;
size_t rcu_gc_threshold_nbytes = 10*RCU_THREAD_GC_THRESHOLD_NBYTES;

/* How many objects/bytes have been freed, and how many do we need to
   trigger the next collection cycle?
 */
rcu_gc_info rcu_free_counts = {0,0,0};

rcu_gc_info rcu_free_target = {
    1,
    10*RCU_THREAD_GC_THRESHOLD_NOBJ,
    10*RCU_THREAD_GC_THRESHOLD_NBYTES,
};

/* Really delete a pointer that was previously passed to rcu_free.

   The [ptr] is the user-visible address returned by rcu_alloc.
 */
void rcu_delete(void *ptr) {
    rcu_pointer u = {ptr};
    --u.p;
    intptr_t fn = u.p->size >> 8;
    if (fn) {  // get a function to run
        (*reinterpret_cast<deleter_t>(fn))(ptr);
    }

    //std::free(u.v);
}


/***************************************
 * * * Callbacks for the epoch_mgr * * *
 ***************************************/
void
rcu_global_init(void*)
{
    RCU_LOG("Initializing RCU subsystem");
}
epoch_mgr::tls_storage *
rcu_get_tls(void*)
{
    static __thread epoch_mgr::tls_storage s;
    return &s;
}
void *
rcu_thread_registered(void*)
{
    RCU_LOG("Thread %zd registered", (size_t) pthread_self());
    rcu_tcb *self = rcu_tcb::tls();
    ASSERT(not self->initialized);
    
    self->initialized = true;
    self->free_count = 0;
    self->free_bytes = 0;
    
    // allocate a freelist
    self->free_list = rcu_alloc();
    self->free_list->next_list = 0;
    self->free_list->head = 0;
    return self;
}

void
rcu_thread_deregistered(void*, void *thread_cookie)
{
    RCU_LOG("Thread %zd deregistered", (size_t) pthread_self());
    auto *self = (rcu_tcb*) thread_cookie;
    ASSERT(self == rcu_tcb::tls());

    // get rid of the freelist
    self->free_list->next_list = rcu_zombie_lists;
    rcu_zombie_lists = self->free_list;
    self->free_list = NULL; 

    // done
    self->initialized = false;
}

void *
rcu_epoch_ended(void*, epoch_mgr::epoch_num x)
{
    RCU_LOG("Epoch %zd ended", x);
    rcu_free_target.objects_freed = rcu_free_counts.objects_freed + rcu_gc_threshold_nobj;
    rcu_free_target.bytes_freed = rcu_free_counts.bytes_freed + rcu_gc_threshold_nbytes;
    DEFER(rcu_zombie_lists = NULL);
    return rcu_zombie_lists;
}

void *
rcu_epoch_ended_thread(void *, void *epoch_cookie, void *thread_cookie)
{
    /* WARNING: the thread_cookie usually does not belong to the
       current thread
     */
    auto *t = (rcu_tcb*) thread_cookie;
    auto *free_lists = (pointer_list*) epoch_cookie;
    
    /* Swap out a non-empty freelist for cooling. Some readers may
       still hold references to it, but we won't actually modify the
       list until the epoch is reclaimed.
    */
    if (t->free_list->head) {
        pointer_list *x = rcu_alloc();
        x->next_list = 0;
        x->head = 0;
        std::swap(t->free_list, x);
        x->next_list = free_lists;
        free_lists = x;
    }

    return free_lists;
}

void
rcu_epoch_reclaimed(void *, void *epoch_cookie)
{
    auto *free_lists = (pointer_list*) epoch_cookie;
    
    int nobj = 0;
    size_t nbytes = 0;
    while (free_lists) {
        pointer *p = free_lists->head;
        while (p) {
            nobj++;
            nbytes += p->size;
            
            pointer *tmp = p->next;
            rcu_delete(p+1); // pass user-visible address
            p = tmp;
        }
        
        pointer_list *tmp = free_lists->next_list;
        rcu_delete(free_lists);
        free_lists = tmp;
    }

    if (nobj) 
        RCU_LOG("RCU freed %d dead objects (%zd bytes)\n", nobj, nbytes);

    rcu_global_gc_stats.gc_passes++;
    rcu_global_gc_stats.objects_freed += nobj;
    rcu_global_gc_stats.bytes_freed += nbytes;
}

epoch_mgr rcu_epochs{{nullptr, rcu_global_init, rcu_get_tls,
            rcu_thread_registered, rcu_thread_deregistered,
            rcu_epoch_ended, rcu_epoch_ended_thread,
            rcu_epoch_reclaimed}};

# if 0
{ // for emacs
#endif
//}

void rcu_set_gc_threshold(size_t nobj, size_t nbytes) {
    rcu_gc_threshold_nobj = std::max(nobj, RCU_THREAD_GC_THRESHOLD_NOBJ);
    rcu_gc_threshold_nbytes = std::max(nbytes, RCU_THREAD_GC_THRESHOLD_NBYTES);
}

rcu_gc_info rcu_get_gc_info() {
    return rcu_global_gc_stats;
}

void rcu_register() {
    rcu_epochs.thread_init();
}

void rcu_deregister() {
    rcu_epochs.thread_fini();
}

bool rcu_is_registered() {
    return rcu_epochs.thread_initialized();
}

void rcu_enter() {
    rcu_epochs.thread_enter();
}

bool rcu_is_active() {
    return rcu_epochs.thread_is_active();
}

void rcu_quiesce() {
    rcu_epochs.thread_quiesce();
}

#if 0
// ^^^ see commends at declaration in rcu.h
void rcu_pend_global_quiesce() {
    // read this first!
    auto tmp = volatile_read(rcu_quiesce_count);
    
    /* RCU will deadlock unless we exit our transaction; doing so may
       trigger a global quiescent point, or a quiescent point might
       arrive during the time it takes. Either way, we would have no
       need to actually block afterward.

       We have to use a full enter/exit pair, because rcu_quiesce()
       would not force a global quiescent point if we happened to be
       the last thread in the system.
    */
    bool active = rcu_is_active();
    if (active) {
        rcu_exit();
    }
    
    /* Now that we know the caller is not in any RCU transaction,
       check whether a new safe point could be installed (which, in
       turn, will detect the case where no other RCU transactions were
       active, which in turn triggers a global quiescent point so we
       can return immediately from the checks below).
    */
    if (volatile_read(rcu_ready_for_safe_point)) {
        RCU_LOG("New safe point!\n");
        rcu_new_safe_point();
    }
    
    rcu_mutex.lock();
    DEFER(rcu_mutex.unlock());
    while (tmp == volatile_read(rcu_quiesce_count)) {
        rcu_has_waiters = true;
        rcu_cond.wait(rcu_mutex);
    }
    
    if (active)
        rcu_enter();
}
#endif

void rcu_exit() {
    rcu_epochs.thread_exit();
}

// want exact match of decoded size and allocated size
// (assuming posix_memalign will coincide with this with DEFAULT_ALIGNMENT?)
#define __rcu_alloc(nbytes)   \
    nbytes += sizeof(pointer);  \
	uint8_t sz_code = encode_size(nbytes);  \
    DIE_IF(nbytes > 950272, "size %lu to large for encoding", nbytes);  \
    size_t sz_alloc = decode_size(sz_code); \
    rcu_pointer u;  \
    int err = posix_memalign(&u.v, DEFAULT_ALIGNMENT, sz_alloc);    \
    THROW_IF(err, rcu_alloc_fail, sz_alloc);    \
    u.p->size = sz_code;    \
    ++u.p;

// The version used by version GC. Actually this function shouldn't be
// here, it should be in the RCU or GC namespace. But here is more
// convenient for getting the size info etc.
void *rcu_alloc_gc(size_t& nbytes) {
    __rcu_alloc(nbytes);
    // return the real allocated size
    nbytes = sz_alloc;
    return u.v;
}

void *rcu_alloc(size_t nbytes) {
    __rcu_alloc(nbytes);
    return u.v;
}

void rcu_free(void const* ptr) {
    ASSERT (rcu_is_active());
    rcu_tcb *self = rcu_tcb::tls();
    rcu_pointer u = {ptr};
    --u.p;
    pointer_list *free_list = volatile_read(self->free_list);
    u.p->next = free_list->head;
    bool eol = not u.p->next;
    free_list->head = u.p;
    size_t fcount = 1 + (eol? 0 : self->free_count);
    bool too_many = fcount > RCU_THREAD_GC_THRESHOLD_NOBJ;
    //size_t fbytes = u.p->size + (eol? 0 : self->free_bytes);
    uint8_t szcode = u.p->size & (~(1UL << 8));
    size_t fbytes = decode_size(szcode) + (eol? 0 : self->free_bytes);
    bool too_big = fbytes > RCU_THREAD_GC_THRESHOLD_NBYTES;
    if (too_many or too_big) {
        size_t tcount = volatile_read(rcu_free_target.objects_freed);
        size_t tbytes = volatile_read(rcu_free_target.bytes_freed);
        
        size_t gcount = __sync_add_and_fetch(&rcu_free_counts.objects_freed, fcount);
        size_t gbytes = __sync_add_and_fetch(&rcu_free_counts.bytes_freed, fbytes);
        
        too_many = gcount > tcount;
        too_big = gbytes > tbytes;
        if (rcu_epochs.new_epoch_possible() and (too_many or too_big)) {
            // try to install a new safe point
            rcu_epochs.new_epoch();
        }

        // reset the local count
        fcount = 0;
        fbytes = 0;
    }
    
    self->free_count = fcount;
    self->free_bytes = fbytes;
}

char const *rcu_vsprintf(char const *msg, va_list ap) {
    va_list ap2;
    
    // first, find out how long the string will be
    va_copy(ap2, ap);
    int len = vsnprintf(0, 0, msg, ap2);
    va_end(ap2);
    
    THROW_IF(len < 0, illegal_argument, "Unable to determine length of message");
    char *ptr = (char *) rcu_alloc(len+1);
    DEFER_UNLESS(commit, rcu_free(ptr));
    
    int len2 = vsnprintf(ptr, len+1, msg, ap);

    // sanity tests (should never go wrong)
    ASSERT (len2 == len);
    THROW_IF(len2 < 0, illegal_argument, "Unable to create message");
    THROW_IF(len < len2, illegal_argument, "Inconsistent message length found");

    // success!
    commit = true;
    return ptr;
}

char const *rcu_sprintf(char const *msg, ...) {
    va_list ap;

    /* Technically, it is undefined behavior to allow a function that
      has called va_start to return without a matching va_end. See
      http://stackoverflow.com/questions/17731013/is-it-safe-to-use-va-list-in-exception-prone-code

      Usually, va_end() is a no-op and the compiler optimizes away
      both it and the DEFER. However, this will avoid trouble if that
      assumption ever fails.
    */
    va_start(ap, msg);
    DEFER(va_end(ap));
    return rcu_vsprintf(msg, ap);
}

#ifndef NDEBUG
#ifdef RCU_UNWIND
#include <unwind.h>

static
_Unwind_Reason_Code rcu_backtrace_callback(_Unwind_Context *ctx, void*) {
    long ip = (long) _Unwind_GetIP(ctx);
    fprintf(stderr, " %lx", ip);
    return _URC_NO_REASON;
}

void rcu_unwind(char const *msg) {
    rcu_epochs.mutex.lock();
    fprintf(stderr, "%s:", msg);
    _Unwind_Backtrace(rcu_backtrace_callback, 0);
    fprintf(stderr, "\n");
    rcu_epochs.mutex.unlock();
}
#endif
#endif
}
