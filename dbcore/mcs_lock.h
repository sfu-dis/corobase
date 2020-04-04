#ifndef MCS_LOCK_H
#define MCS_LOCK_H
/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

// -*- mode:c++; c-basic-offset:4 -*-

/**\cond skip */

/**\brief An MCS queuing spinlock.
 *
 * Useful for short, contended critical sections.
 * If contention is expected to be rare, use a
 * tatas_lock;
 * if critical sections are long, use pthread_mutex_t so
 * the thread can block instead of spinning.
 *
 * Tradeoffs are:
   - test-and-test-and-set locks: low-overhead but not scalable
   - queue-based locks: higher overhead but scalable
   - pthread mutexes : high overhead and blocks, but frees up cpu for other
 threads
*/
#define CRITICAL_SECTION(name, lock) critical_section name(lock)

struct mcs_lock {
  struct qnode {
    qnode* _next;
    int _waiting;
    int _padding;
    // qnode() : _next(NULL), _waiting(false) { }
    qnode volatile* vthis() { return this; }
  };
  struct ext_qnode {
    qnode _node;
    operator qnode*() { return &_node; }
    mcs_lock* _held;
  };
#define MCS_EXT_QNODE_INITIALIZER \
  {                               \
    { 0, false, 0 }               \
    , 0                           \
  }
  qnode* volatile _tail = 0;
  constexpr mcs_lock() : _tail(0) {}

  /* This spinning occurs whenever there are critical sections ahead of us.
     CC mangles this as __1cImcs_lockPspin_on_waiting6Mpon0AFqnode__v_
  */
  void spin_on_waiting(qnode* me);  // no-inline.cpp

  /* Only acquire the lock if it is free... */
  inline bool attempt(ext_qnode* me) {
    if (attempt((qnode*)me)) {
      me->_held = this;
      return true;
    }
    return false;
  }
  inline bool attempt(qnode* me) {
    me->_next = 0;
    me->_waiting = true;
    __sync_synchronize();
    qnode* pred = (qnode*)__sync_val_compare_and_swap(&_tail, 0, me);
    // lock held?
    if (pred) return false;
    __sync_synchronize();
    return true;
  }
  // return true if the lock was free
  inline void* acquire(ext_qnode* me) {
    me->_held = this;
    return acquire((qnode*)me);
  }
  inline void* acquire(qnode* me) {
    return __unsafe_end_acquire(me, __unsafe_begin_acquire(me));
  }

  inline qnode* __unsafe_begin_acquire(qnode* me) {
    me->_next = 0;
    me->_waiting = true;
    __sync_synchronize();
    qnode* pred = (qnode*)__sync_lock_test_and_set(&_tail, me);
    if (pred) {
      pred->_next = me;
    }
    return pred;
  }
  inline void* __unsafe_end_acquire(qnode* me, qnode* pred) {
    if (pred) {
      spin_on_waiting(me);
    }
    __sync_synchronize();
    return (void*)pred;
  }

  /* This spinning only occurs when we are at _tail and catch a
     thread trying to enqueue itself.

     CC mangles this as __1cImcs_lockMspin_on_next6Mpon0AFqnode__3_
  */
  qnode* spin_on_next(qnode* me);  // no-inline.cpp

  inline void release(ext_qnode* me) {
    me->_held = 0;
    release((qnode*)me);
  }
  inline void release(ext_qnode& me) { release(&me); }
  inline void release(qnode& me) { release(&me); }
  inline void release(qnode* me) {
    __sync_synchronize();
    qnode* next;
    if (!(next = me->_next)) {
      if (me == _tail &&
          me == (qnode*)__sync_val_compare_and_swap(&_tail, me, 0))
        return;
      next = spin_on_next(me);
    }
    next->_waiting = false;
  }
  // bool is_mine(qnode* me) { return me->_held == this; }
  inline bool is_mine(ext_qnode* me) { return me->_held == this; }
};

struct critical_section {
  critical_section(mcs_lock& mutex) : _mutex(&mutex) {
    _me._held = 0;
    _mutex->acquire(&_me);
  }

  ~critical_section() {
    if (_mutex) _mutex->release(&_me);
    _mutex = 0;
  }

 private:
  mcs_lock* _mutex;
  mcs_lock::ext_qnode _me;
  critical_section(critical_section const&);
};

/**\endcond skip */
#endif
