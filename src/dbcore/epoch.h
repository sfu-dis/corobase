// -*- mode:c++ -*-
#pragma once

#include "mcs_lock.h"
#include "sm-common.h"

#include <type_traits>

/* A customizable epoch manager, suitable for implementing services
   such as RCU. This class is POD, and so can be placed in statically
   initialized storage.
 */
struct epoch_mgr {
  typedef uint64_t epoch_num;

  struct __attribute__((aligned(8))) tls_storage {
    enum { SIZE = 32 };
    char data[SIZE];
  };

  static_assert(std::is_pod<tls_storage>::value,
                "epoch_mgr::tls_storage must be POD");

  struct callbacks {
    /* A pointer to state the callbacks need access to, passed to
       all callbacks as the first argument.
     */
    void *cookie;

    /* Called exactly once, when the epoch manager is first
       initialized.

       NOTE: epoch_mgr::mutex held while calling this function
     */
    void (*global_init)(void *);

    /* Return a pointer to a block of thread-local storage the
       epoch_mgr can use.

       NOTE: The storage must be zero-filled and remain valid as
       long as the thread exists. Statically allocated
       thread-local storage fills these requirements and is
       recommended. This function will always be called by the
       thread to which the storage belongs, so it suffices to
       return the address of a thread-local variable.
    */
    tls_storage *(*get_tls)(void *);

    /* Called whenever a thread registers itself with this epoch
       manager. The return value is an opaque handle belonging to
       the user (e.g. for thread-local resource tracking). It will
       be passed to epoch_ended_thread whenever an epoch ends and
       to thread_deregistered for disposal.

       NOTE: epoch_mgr::mutex held while calling this function
    */
    void *(*thread_registered)(void *);

    /* Called whenever a thread deregisters itself from this epoch
       manager. The argument is the handle returned by the
       previous call to thread_registered and will be forgotten by
       the epoch manager after this call returns.

       NOTE: epoch_mgr::mutex held while calling this function
    */
    void (*thread_deregistered)(void *cookie, void *thread_cookie);

    /* This function is called when an epoch ends. The return
       value is an opaque handle belonging to the user (e.g. for
       global resource tracking). The handle will be passed to
       epoch_ended_thread (for each registered thread) and to
       epoch_reclaimed (when the epoch is reclaimed).

       NOTE: epoch_mgr::mutex held while calling this function
    */
    void *(*epoch_ended)(void *, epoch_num);

    /* Called once for each registered thread when an epoch
       ends. The first argument is the epoch-specific handle
       returned by epoch_ended(), and the second is the
       thread-specific handle rerturned by
       thread_registered(). Return a replacement for the epoch
       cookie.

       NOTE: epoch_mgr::mutex held while calling this function
    */
    void *(*epoch_ended_thread)(void *cookie, void *epoch_cookie,
                                void *thread_cookie);

    /* Called once the last straggler in a closed epoch is
       confirmed to have left. The argument is the epoch-specific
       handle returned by epoch_ended() and will be forgotten by
       the epoch manager after this function returns.

       NOTE: this function will never be called with a NULL
       cookie. If epoch_ended() returns NULL, and no call to
       epoch_ended_thread() changes that, then epoch_reclaimed
       will not be called for that epoch.

       NOTE: epoch_mgr::mutex is *not* held while calling this
       function, but calls to this function are serialized.
    */
    void (*epoch_reclaimed)(void *cookie, void *epoch_cookie);
  };

  static_assert(std::is_pod<callbacks>::value,
                "epoch_mgr::callbacks must be POD");

  /* Use a constexpr constructor so that this object can still be
     statically initialized.
   */
  constexpr epoch_mgr(callbacks const &c) : cb(c) {}

  epoch_num get_cur_epoch();

  /* Open a new epoch, closing the current one. If the call is
     successful, all transactions that start after this point will
     be part of the new epoch. The current epoch will not be
     reclaimed until the last straggler leaves, however.

     Return false if stragglers are preventing the epoch change.
   */
  bool new_epoch();

  /* Query whether a new epoch can potentially be opened. If this
     function returns true, a call to new_epoch() might have
     succeeded. If this function returns false, a call to
     new_epoch() would have certainly failed.

     WARNING: this function is advisory at best. Even if the
     function returns true, other threads might manage to install
     the new epoch first. Conversely, even if the function returns
     false, 1+ epochs might have been reclaimed by the time the
     caller acts, allowing for a new epoch. Nevertheless, this
     function provides a cheap and (usually) helpful hint about
     whether a call to new_epoch() might be warranted.
   */
  bool new_epoch_possible();

  void thread_init();
  void thread_fini();
  bool thread_initialized();

  epoch_num thread_enter();
  bool thread_is_active();
  epoch_num thread_quiesce();
  void thread_exit();

  struct __attribute__((aligned(64))) private_state;
  struct thread_state;

  private_state *state = 0;
  callbacks cb;
  mcs_lock mutex;
};
