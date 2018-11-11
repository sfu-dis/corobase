#include <stdint.h>
#include <pthread.h>
#include <map>
#include <new>
#include <algorithm>

#include "../macros.h"

#include "epoch.h"
#include "rcu.h"
#include "size-encode.h"
#include "sm-exceptions.h"
#include "sm-common.h"
#include "sm-defs.h"

namespace ermia {
namespace RCU {

#ifdef RCU_LOGGING
#define RCU_LOG(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__)
#else
#define RCU_LOG(msg, ...)
#endif

struct pointer_stash {
  pointer *head;
  int max_count;
  int cur_count;

  pointer_stash() : head(0), max_count(0), cur_count(0) {}

  bool give(pointer *p);
  pointer *take();
};

/* TODO: set up fancy slab allocators using the above pointers...
 */

/* Thread control block for RCU.
 */
struct rcu_tcb {
  static rcu_tcb *tls() {
    static thread_local rcu_tcb local;
    return &local;
  }

  //    bool initialized;
  int register_count;
  int enter_count;

  /* All recently-freed memory goes on this list; the RCU subsystem
     will swap out the [pointer_list] at each system-wide quiescent
     point and place the entire list in an internal cool-down
     area. It (and its contents) will be freed at the following
     system-wide quiescent point, when all references have died.
   */
  pointer_list *free_list;

  /* Stashed memory makes a progression down through this array.

     cur - released in the current epoch
     cooling - released last epoch, waiting for threads to leave
     cold - released two epochs ago, waiting for stragglers to leave
     reclaimed - ready for the thread to reclaim.
   */
  pointer_list *cur_stash;
  pointer_list *cooling_stash;
  pointer_list *cold_stash;
  pointer_list *reclaimed_stash;

  /* Stashed memory is organized by block size. When thread requests
     tracking of a given size, a new entry is created. Whenever a
     stash size runs out, the thread will drain the reclaimed_stash
     list (which contains blocks of all valid sizes) and will
     scatter its pieces into their respective size stashes for
     reuse. If a size-stash overflows or is destroyed, the extras
     will be released to the system.
  */
  std::map<size_t, pointer_stash> size_stash;

  /* Trigger a collection if the number of (to be) freed objects or
     bytes passes a certain threshold. Track free counts locally to
     avoid contention, and occasionally update the global total.
   */
  size_t free_count;
  size_t free_bytes;
};

pointer_list *rcu_zombie_lists = 0;
rcu_gc_info rcu_global_gc_stats = {0, 0, 0};

static size_t const RCU_THREAD_GC_THRESHOLD_NOBJ = 1000;
static size_t const RCU_THREAD_GC_THRESHOLD_NBYTES = 10 * 1024 * 1024;

size_t rcu_gc_threshold_nobj = 10 * RCU_THREAD_GC_THRESHOLD_NOBJ;
size_t rcu_gc_threshold_nbytes = 10 * RCU_THREAD_GC_THRESHOLD_NBYTES;

/* How many objects/bytes have been freed, and how many do we need to
   trigger the next collection cycle?
 */
rcu_gc_info rcu_free_counts = {0, 0, 0};

rcu_gc_info rcu_free_target = {
    1, 10 * RCU_THREAD_GC_THRESHOLD_NOBJ, 10 * RCU_THREAD_GC_THRESHOLD_NBYTES,
};

void rcu_delete_p(pointer *p) { std::free(p); }

/* Really delete a pointer that was previously passed to rcu_free.

   The [ptr] is the user-visible address returned by rcu_alloc.
 */
void rcu_delete_v(void *ptr) {
  rcu_pointer u = {ptr};
  --u.p;
  //    intptr_t fn = u.p->size >> 8;
  //    if (fn) {  // get a function to run
  //        (*reinterpret_cast<deleter_t>(fn))(ptr);
  //    }

  rcu_delete_p(u.p);
}

/***************************************
 * * * Callbacks for the epoch_mgr * * *
 ***************************************/
void rcu_global_init(void *) { RCU_LOG("Initializing RCU subsystem"); }
epoch_mgr::tls_storage *rcu_get_tls(void *) {
  static thread_local epoch_mgr::tls_storage s;
  return &s;
}

void *rcu_thread_registered(void *) {
  RCU_LOG("Thread %zd registered", (size_t)pthread_self());
  rcu_tcb *self = rcu_tcb::tls();
  //    ASSERT(not self->initialized);

  //    self->initialized = true;
  self->register_count = 1;
  self->free_count = 0;
  self->free_bytes = 0;

  // allocate a freelist
  self->free_list = rcu_new();

  // allocate thread stashes
  self->cur_stash = rcu_new();
  self->cooling_stash = NULL;
  self->cold_stash = NULL;
  self->reclaimed_stash = NULL;
  return self;
}

bool filter_delete_all(pointer *) { return true; }

template <typename Filter>
void delete_list_contents(pointer *p, Filter &do_delete) {
  while (p) {
    pointer *q = p->next;
    if (do_delete(p)) rcu_delete_p(p);
    p = q;
  }
}

void delete_list_contents(pointer *p) {
  auto do_delete = [](pointer *) { return true; };
  delete_list_contents(p, do_delete);
}

void append_stash(pointer_list *free_list, pointer_list *pstash) {
  pointer *q = 0;
  for (pointer *p = pstash->head; p; p = p->next) q = p;

  if (q) {
    q->next = free_list->head;
    free_list->head = pstash->head;
  }

  rcu_free(pstash);
}

void rcu_thread_deregistered(void *, void *thread_cookie) {
  RCU_LOG("Thread %zd deregistered", (size_t)pthread_self());
  auto *self = (rcu_tcb *)thread_cookie;
  ASSERT(self == rcu_tcb::tls());

  auto make_zombie = [&](pointer_list *&plist) {
    if (plist) {
      plist->next_list = rcu_zombie_lists;
      rcu_zombie_lists = plist;
      plist = NULL;
    }
  };

  // add freelist and stashes to the zombie list
  make_zombie(self->free_list);
  make_zombie(self->cur_stash);
  make_zombie(self->cooling_stash);
  make_zombie(self->cold_stash);
  make_zombie(self->reclaimed_stash);

  // done
  //    self->initialized = false;
  self->register_count = 0;
}

void *rcu_epoch_ended(void *, epoch_mgr::epoch_num x) {
  MARK_REFERENCED(x);
  RCU_LOG("Epoch %zd ended", x);
  rcu_free_target.objects_freed =
      rcu_free_counts.objects_freed + rcu_gc_threshold_nobj;
  rcu_free_target.bytes_freed =
      rcu_free_counts.bytes_freed + rcu_gc_threshold_nbytes;

  DEFER(rcu_zombie_lists = NULL);
  return rcu_zombie_lists;
}

void *rcu_epoch_ended_thread(void *, void *epoch_cookie, void *thread_cookie) {
  /* WARNING: the thread_cookie usually does not belong to the
     current thread
   */
  auto *t = (rcu_tcb *)thread_cookie;
  auto *free_lists = (pointer_list *)epoch_cookie;

  /* Swap out a non-empty freelist for cooling. Some readers may
     still hold references to it, but we won't actually modify the
     list until the epoch is reclaimed.
  */
  if (t->free_list->head) {
    pointer_list *x = rcu_new();
    std::swap(t->free_list, x);
    x->next_list = free_lists;
    free_lists = x;
  }

  pointer_list *x = 0;
  if (t->cur_stash->head) {
    x = rcu_new();
    std::swap(t->cur_stash, x);
  }

  // now bubble it down...
  std::swap(t->cooling_stash, x);
  std::swap(t->cold_stash, x);

  // update stats
  if (x) {
    rcu_global_gc_stats.objects_stashed += x->nobj;
    rcu_global_gc_stats.bytes_stashed += x->nbytes;
  }

  // do we need to reclaim this?
  x = __sync_lock_test_and_set(&t->reclaimed_stash, x);
  if (x) {
    //        SPAM("TLS overflow to be freed");
    x->next_list = free_lists;
    free_lists = x;
  }
  return free_lists;
}

void rcu_epoch_reclaimed(void *, void *epoch_cookie) {
  auto *free_lists = (pointer_list *)epoch_cookie;

  int nobj = 0;
  size_t nbytes = 0;
  while (free_lists) {
    pointer *p = free_lists->head;
    nobj += free_lists->nobj;
    nbytes += free_lists->nbytes;

    while (p) {
      pointer *tmp = p->next;
      rcu_delete_p(p);
      p = tmp;
    }

    pointer_list *tmp = free_lists->next_list;
    rcu_delete_v(free_lists);
    free_lists = tmp;
  }

  if (nobj) RCU_LOG("RCU freed %d dead objects (%zd bytes)\n", nobj, nbytes);

  rcu_global_gc_stats.gc_passes++;
  rcu_global_gc_stats.objects_freed += nobj;
  rcu_global_gc_stats.bytes_freed += nbytes;
}

epoch_mgr rcu_epochs{{nullptr, rcu_global_init, rcu_get_tls,
                      rcu_thread_registered, rcu_thread_deregistered,
                      rcu_epoch_ended, rcu_epoch_ended_thread,
                      rcu_epoch_reclaimed}};

void rcu_set_gc_threshold(size_t nobj, size_t nbytes) {
  rcu_gc_threshold_nobj = std::max(nobj, RCU_THREAD_GC_THRESHOLD_NOBJ);
  rcu_gc_threshold_nbytes = std::max(nbytes, RCU_THREAD_GC_THRESHOLD_NBYTES);
}

rcu_gc_info rcu_get_gc_info() { return rcu_global_gc_stats; }

void rcu_register() {
  // register? or just increase nesting level?
  rcu_tcb *self = rcu_tcb::tls();
  if (self->register_count)
    self->register_count++;
  else
    rcu_epochs.thread_init();
}

void rcu_deregister() {
  // deregister? or just decrease nesting level?
  rcu_tcb *self = rcu_tcb::tls();
  if (self->register_count > 1)
    self->register_count--;
  else
    rcu_epochs.thread_fini();
}

bool rcu_is_registered() { return rcu_epochs.thread_initialized(); }

void rcu_enter() {
  // start? or just increase nesting level?
  rcu_tcb *self = rcu_tcb::tls();
  if (not self->enter_count++) rcu_epochs.thread_enter();
}

bool rcu_is_active() { return rcu_epochs.thread_is_active(); }

void rcu_quiesce() {
  //    rcu_epochs.thread_quiesce();
  // quiesce only if we're the top level RCU transaction
  rcu_tcb *self = rcu_tcb::tls();
  if (self->register_count == 1) rcu_epochs.thread_quiesce();
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
  // exit? or just decrease nesting level?
  rcu_tcb *self = rcu_tcb::tls();
  if (not--self->enter_count) rcu_epochs.thread_exit();
}

bool pointer_stash::give(pointer *p) {
  ASSERT(p);
  if (cur_count == max_count) return false;

  p->next = head;
  head = p;
  cur_count++;
  return true;
}

pointer *pointer_stash::take() {
  if (not cur_count) return 0;

  auto *p = head;
  head = p->next;
  cur_count--;
  return p;
}

void rcu_start_tls_cache(size_t nbytes, size_t nentries) {
  ASSERT(rcu_is_registered());
  rcu_tcb *self = rcu_tcb::tls();
  auto &pstash = self->size_stash[nbytes + sizeof(pointer)];
  pstash.max_count += nentries;
}

void rcu_stop_tls_cache(size_t nbytes) {
  ASSERT(rcu_is_registered());
  rcu_tcb *self = rcu_tcb::tls();
  auto it = self->size_stash.find(nbytes + sizeof(pointer));
  if (it != self->size_stash.end()) {
    delete_list_contents(it->second.head);
    self->size_stash.erase(it);
  }
}

void *rcu_alloc(size_t nbytes) {
  nbytes += sizeof(pointer);

  // available from thread-local stash?
  rcu_tcb *self = rcu_tcb::tls();
  auto end = self->size_stash.end();
  auto it = self->size_stash.find(nbytes);
  if (it != end) {
    bool first_time = true;
  retry:
    if (pointer *p = it->second.take()) {
      ASSERT(p->size == nbytes);
      return p + 1;
    }

    // can we reclaim a stash?
    if (first_time) {
      if (pointer_list *reclaimed =
              __sync_lock_test_and_set(&self->reclaimed_stash, 0)) {
        auto do_delete = [&](pointer *p) {
          auto it2 = self->size_stash.find(p->size);
          return (it2 == end or not it2->second.give(p));
        };

        delete_list_contents(reclaimed->head, do_delete);
        rcu_free(reclaimed);

        first_time = false;
        goto retry;
      }
    }
  }

  // fall back to the slow way, then...
  rcu_pointer u;
  int err = posix_memalign(&u.v, DEFAULT_ALIGNMENT, nbytes);
  THROW_IF(err, rcu_alloc_fail, nbytes);
  u.p->size = nbytes;
  ++u.p;
  return u.v;
}

void rcu_free(void const *ptr) {
  ASSERT(rcu_is_active());
  rcu_tcb *self = rcu_tcb::tls();
  rcu_pointer u = {ptr};
  --u.p;

  // where to put it?
  pointer_list *free_list;
  if (self->size_stash.find(u.p->size) != self->size_stash.end()) {
    // this is a size we like
    free_list = volatile_read(self->cur_stash);
  } else {
    // to the global free list, then
    free_list = volatile_read(self->free_list);
  }

  free_list->nobj++;
  free_list->nbytes += u.p->size;
  u.p->next = free_list->head;
  bool eol = not u.p->next;
  free_list->head = u.p;
  size_t fcount = 1 + self->free_count;
  bool too_many = fcount > RCU_THREAD_GC_THRESHOLD_NOBJ;
  size_t fbytes = u.p->size + (eol ? 0 : self->free_bytes);
  //    uint8_t szcode = u.p->size & (~(1UL << 8));
  //    size_t fbytes = decode_size(szcode) + self->free_bytes;
  bool too_big = fbytes > RCU_THREAD_GC_THRESHOLD_NBYTES;
  if (too_many or too_big) {
    size_t tcount = volatile_read(rcu_free_target.objects_freed);
    size_t tbytes = volatile_read(rcu_free_target.bytes_freed);

    size_t gcount =
        __sync_add_and_fetch(&rcu_free_counts.objects_freed, fcount);
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
  char *ptr = (char *)rcu_alloc(len + 1);
  DEFER_UNLESS(commit, rcu_free(ptr));

  int len2 = vsnprintf(ptr, len + 1, msg, ap);

  // sanity tests (should never go wrong)
  ASSERT(len2 == len);
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

static _Unwind_Reason_Code rcu_backtrace_callback(_Unwind_Context *ctx,
                                                  void *) {
  long ip = (long)_Unwind_GetIP(ctx);
  fprintf(stderr, " %lx", ip);
  return _URC_NO_REASON;
}

void rcu_unwind(char const *msg) {
  CRITICAL_SECTION(cs, rcu_epochs.mutex);
  fprintf(stderr, "%s:", msg);
  _Unwind_Backtrace(rcu_backtrace_callback, 0);
  fprintf(stderr, "\n");
}
#endif
#endif
}  // namespace RCU
}  // namespace ermia
