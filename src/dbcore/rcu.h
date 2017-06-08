// -*- mode: c++ -*-
#ifndef __RCU_H
#define __RCU_H

#include "sm-defs.h"

#include <cstddef>
#include <cstdarg>

namespace RCU {
#define RCU_UNWIND

/***
    A lightweight user-space RCU implementation.

    RCU (read-copy-update) is a resource management scheme designed to
    simplify some concurrency control problems significantly. In
    particular, it solves the "dangling pointer" problem, where a
    writer wishes to change or delete some object that readers might
    be in the middle of using. The RCU protocol generally forbids
    in-place updates, and instead requires that writers install a
    modified copy of an object while leaving the original usable in
    case readers are using it. Once a new version is installed, the
    old version becomes unreachable (to new readers) and will
    eventually become unreferenced (by current readers) as well. The
    system can then reclaim any object that it can prove is both
    unreachable and unreferenced.

    RCU makes no attempt to protect writers from other writers; these
    should synchronize using a suitable concurrency control mechanism
    (locking, atomic ops, lock-free algorithms, etc.).

    Readers access RCU-protected state in "transactions." An RCU
    transaction begins when the reader announces its intention to
    access RCU-protected resources and ends when the reader announces
    that it has finished. These announcements can be quite
    coarse-grained: the system is primarily concerned with knowing
    which threads are "busy" vs. "idle" more than which are "inside"
    or "outside" a particular critical section. Threads with long gaps
    between idle periods (or which never become idle) can also
    announce "quiescent" points any time they hold no references to
    RCU-protected resources. Quiescent points are extremely
    lightweight, and have virtually no overhead unless the calling
    thread's transaction is delaying a garbage collection cycle; even
    in the latter case, the overhead is no worse than ending the
    transaction and beginning a new one.

    Note that RCU "transactions" have nothing to do with application
    semantics, but rather identify contiguous spans of time where
    garbage collection is unsafe because the reader might hold
    references to unreachable objects. Thus, a "transaction" can end
    any time the reader holds no RCU references, even if the reader's
    current operation is far from complete.

    This implementation is based on a notion of "safe points." At the
    moment one safe point is installed, there exists some set of
    in-flight RCU transactions (called "stragglers") who might still
    hold references to objects that became unreachable before the safe
    point. Those objects cannot be reclaimed until all stragglers end,
    at which point a new safe point can be installed and the cycle
    repeats. The tricky part is identifying stragglers in scalable
    fashion and with low overhead, which we achieve by ensuring that
    threads can easily identify if they are stragglers, and if so,
    whether they are the last straggler; in most cases the thread can
    determine this by a simple lookup in thread-local storage.
 */

/* Instruct the RCU runtime to reclaim resources whenever the number
   of unreachable objects is more than [nobj] or the number of
   unreachable bytes is more than [nbytes]. In general, these numbers
   should be large enough that most RCU transactions do not become
   stragglers.
 */
void rcu_set_gc_threshold(size_t nobj, size_t nbytes);

struct rcu_gc_info {
  size_t gc_passes;
  size_t objects_freed;
  size_t bytes_freed;

  size_t objects_stashed;
  size_t bytes_stashed;
};

// tzwang: moved here for silo side to use
struct __attribute__((aligned(DEFAULT_ALIGNMENT))) pointer {
  pointer *next;
  size_t size;
};

static_assert(sizeof(pointer) == 16, "Yikes");

struct pointer_list {
  pointer *head;
  pointer_list *next_list;
  uint64_t nobj;
  uint64_t nbytes;

  pointer_list() : head(0), next_list(0), nobj(0), nbytes(0) {}
};

union rcu_pointer {
  void const *vc;
  void *v;
  pointer *p;
};

/* Return the number of GC passes, plus total number of objects freed,
   since RCU was initialized.
*/
rcu_gc_info rcu_get_gc_info();

/* Registers the calling thread with the RCU system.

   This function must be called before using RCU infrastructure.
 */
void rcu_register();

/* Deregisters the calling thread from the RCU system.

   The thread may no longer use RCU infrastructure.

   NOTE: registered threads call this function automatically when they
   die, so it only needs to be called manually if a thread has stopped
   using RCU but will not die any time soon (e.g. because it changed
   roles or re-entered a thread pool).
*/
void rcu_deregister();

/* Return true if the calling thread is currently registered with the
   RCU system.
 */
bool rcu_is_registered();

/* Request that allocations of the indicated size be cached
   thread-locally when possible, up to the indicated capacity.
 */
void rcu_start_tls_cache(size_t sz, size_t cap);

/* Request that allocations of the indicated size no longer be cached
   thread-locally (all currently-cached objects of that size will be
   released to the system).
 */
void rcu_stop_tls_cache(size_t n);

/* Start an RCU transaction. No RCU-protected resources freed while
   the transaction is in flight can be reclaimed until the transaction
   quiesces or ends.
 */
void rcu_enter();

/* Notify the RCU system that the calling thread has reached a
   quiescent point (holds no RCU-protected pointers).

   If the thread's RCU transaction is straggler, this call will allow
   the system to reclaim resources. Otherwise, the thread will
   continue uninterrupted.

   This function is designed to be inexpensive, and threads executing
   long RCU transactions should call this function reasonably often to
   reduce the system's resource footprint.
 */
void rcu_quiesce();

#if 0
/* ^^^ This function is not currently supported because it's unclear
   whether it can be implemented efficiently, and also unclear whether
   there are any compelling use cases.
 */
/* Block the calling thread until the next global quiescent point.

   This function implicitly calls rcu_quiesce if the caller is
   currently part of an RCU transaction.
 */
void rcu_pend_global_quiesce();
#endif

/* Notify the RCU system that the calling thread has ended the current
   RCU transaction. RCU-protected resources that were freed while the
   transaction was in flight can now be reclaimed (once all other
   in-flight transactions also end).
 */
void rcu_exit();

/* Return true if the calling thread is currently part of an RCU
   transaction (which implies rcu_is_registered() also returns true)
 */
bool rcu_is_active();

/* Allocate and return [nbytes] of memory.

   Throw rcu_alloc_fail if no memory is available
*/
void *rcu_alloc(size_t nbytes);
void *rcu_alloc_gc(size_t &nbytes);

/* Free a pointer previously returned by rcu_alloc */
void rcu_free(void const *);

void rcu_delete(void *ptr);

/* Convenience function...
 */
template <typename T>
static inline void rcu_alloc_into(T *&ptr) {
  ptr = (T *)rcu_alloc(sizeof(T));
}

struct rcu_alloc_on_decay {
  template <typename T>
  operator T *() {
    return (T *)rcu_alloc(sizeof(T));
  }
};

/* Exploit C++ type decay to make a type-safe allocator.

   When assigned to a pointer of any type, the return value of this
   function decays to a new allocation large enough to hold an object
   of that type.

   WARNING: this function is suitable only for allocating single
   instances of an object; it does *not* work for arrays unless
   assigned to a pointer-to-array (implying a fixed-size array).
 */
static inline rcu_alloc_on_decay rcu_alloc() { return rcu_alloc_on_decay(); }

class rcu_new_sentinel {
 protected:
  rcu_new_sentinel() {}
};

template <typename Tuple, size_t... i>
struct rcu_new_decay : rcu_new_sentinel {
  Tuple tup;
  rcu_new_decay(Tuple t) : tup(std::move(t)) {}

  template <typename T>
  operator T *() {
    static_assert(std::is_trivially_destructible<T>::value,
                  "Non-trivial destructors don't work with RCU storage");
    return new (*this) T{std::get<i>(tup)...};
  }
};

template <size_t N, typename Tuple, size_t... i>
struct rcu_new_helper : rcu_new_helper<N - 1, Tuple, N - 1, i...> {};

template <typename Tuple, size_t... i>
struct rcu_new_helper<0, Tuple, i...> {
  typedef rcu_new_decay<Tuple, i...> type;
};

/* Create an RCU version of make_new for allocating and initializing
   new objects in a single operation.

   We only allow to construct objects with trivial destructors in this
   way, because there is no safe way to call the destructor of an
   object passed to rcu_free (can't safely call it at the time of the
   free, and RCU has no way to call it later).

   The code is virtually identical to the implementation of make_new,
   except it uses placement new with RCU-allocated storage instead of
   calling the default operator new.
 */
template <typename... Args>
auto rcu_new(Args &&... args) ->
    typename rcu_new_helper<sizeof...(Args),
                            decltype(std::forward_as_tuple(args...))>::type

{
  return std::forward_as_tuple(args...);
}

/* Format the given message into string that occupies space newly
   obtained from rcu_alloc and return the pointer. The caller is
   responsible to ensure that the message is freed when no longer
   needed. This is similar to asprintf, but using RCU instead of
   malloc, and throwing an exception if anything goes wrong, rather
   than returning an error code.
 */
char const *rcu_sprintf(char const *msg, ...)
    __attribute__((format(printf, 1, 2)));
char const *rcu_vsprintf(char const *msg, va_list ap);

/* Although RCU requires no effort on the reader's part to dereference
   an RCU-protected pointer, it is useful to identify all such reads
   (for code quality/debuggability/maintainabilitY) and also makes a
   convenient place to do some basic sanity checks (like verifying
   that an RCU transaction is actually in progress).
 */
#ifndef NDEBUG
extern void rcu_unwind(char const *);

template <typename T>
static inline T *rcu_read(T *ptr) {
#ifdef RCU_UNWIND
  if (not rcu_is_active())
    rcu_unwind("RCU access detected outside RCU transaction!");
#endif
  ASSERT(rcu_is_active());
  return ptr;
}
#define RCU_READ(ptr) rcu_read(ptr)
#else
#define RCU_READ(ptr) (ptr)
#endif
}

inline void *operator new(size_t nbytes, RCU::rcu_new_sentinel const &) {
  return RCU::rcu_alloc(nbytes);
}

inline void operator delete(void *ptr, RCU::rcu_new_sentinel const &) {
  RCU::rcu_free(ptr);
}

#endif
