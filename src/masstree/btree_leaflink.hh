/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2013 President and Fellows of Harvard College
 * Copyright (c) 2012-2013 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef BTREE_LEAFLINK_HH
#define BTREE_LEAFLINK_HH 1
#include "compiler.hh"

/** @brief Operations to manage linked lists of B+tree leaves.

    N is the type of nodes. CONCURRENT is true to make operations
    concurrency-safe (e.g. compare-and-swaps, fences), false to leave them
    unsafe (only OK on single threaded code, but faster). */
template <typename N, bool CONCURRENT = N::concurrent> struct btree_leaflink {};


// This is the normal version of btree_leaflink; it uses lock-free linked list
// operations.
template <typename N> struct btree_leaflink<N, true> {
  private:
    static inline N *mark(N *n) {
	return reinterpret_cast<N *>(reinterpret_cast<uintptr_t>(n) + 1);
    }
    static inline bool is_marked(N *n) {
	return reinterpret_cast<uintptr_t>(n) & 1;
    }
#ifdef HACK_SILO
    template <typename SF>
    static inline N *lock_next(N *n, SF spin_function) {
	while (1) {
		bool locked = n->next_lock_;
	    if (!n->next_oid_
		|| (!locked
		    && bool_cmpxchg(&n->next_lock_, locked, true)))			// locking n node
		return reinterpret_cast<N*>(n->fetch_node(n->next_oid_));
	    spin_function();
	}
    }
#else
    template <typename SF>
    static inline N *lock_next(N *n, SF spin_function) {
	while (1) {
	    N *next = n->next_.ptr;
	    if (!next
		|| (!is_marked(next)
		    && bool_cmpxchg(&n->next_.ptr, next, mark(next))))
		return next;
	    spin_function();
	}
    }
#endif

  public:
    /** @brief Insert a new node @a nr at the right of node @a n.
	@pre @a n is locked.

	Concurrency correctness: Ensures that all "next" pointers are always
	valid, even if @a n's successor is deleted concurrently. */
    static void link_split(N *n, N *nr) {
	link_split(n, nr, relax_fence_function());
    }
    /** @overload */
#ifdef HACK_SILO
    template <typename SF>
    static void link_split(N *n, N *nr, SF spin_function) {
	nr->prev_oid_ = n ? n->oid : 0;

	N *next = lock_next(n, spin_function);
	nr->next_oid_ = next ? next->oid : 0;

	if (next)
		next->prev_oid_ = next ? nr->oid : 0;

	fence();
	n->next_oid_ = nr ? nr->oid : 0;
	n->next_lock_ = false;				// unlocking n node ( locked in lock_next )
    }
#else
    template <typename SF>
    static void link_split(N *n, N *nr, SF spin_function) {
	nr->prev_ = n;
	N *next = lock_next(n, spin_function);
	nr->next_.ptr = next;
	if (next)
	    next->prev_ = nr;
	fence();
	n->next_.ptr = nr;
    }
#endif

    /** @brief Unlink @a n from the list.
	@pre @a n is locked.

	Concurrency correctness: Works even in the presence of concurrent
	splits and deletes. */
    static void unlink(N *n) {
	unlink(n, relax_fence_function());
    }
    /** @overload */
    template <typename SF>
    static void unlink(N *n, SF spin_function) {
	// Assume node order A <-> N <-> B. Since n is locked, n cannot split;
	// next node will always be B or one of its successors.
#ifdef HACK_SILO
	N *next = lock_next(n, spin_function);				// locking n node
	N *prev;
	while (1) {
	    prev = reinterpret_cast<N*>(n->fetch_node(n->prev_oid_));
		bool locked = prev->next_lock_;
	    if (bool_cmpxchg(&prev->next_lock_, locked, true))			// locking prev node
		break;
	    spin_function();
	}
	if (next)
		next->prev_oid_ = prev->oid;
	fence();
	prev->next_oid_ = next ? next->oid : 0;
	prev->next_lock_ = false;			// unlocking prev node
    }
#else
	N *next = lock_next(n, spin_function);
	N *prev;
	while (1) {
	    prev = n->prev_;
	    if (bool_cmpxchg(&prev->next_.ptr, n, mark(n)))
		break;
	    spin_function();
	}
	if (next)
	    next->prev_ = prev;
	fence();
	prev->next_.ptr = next;
    }
#endif
};


// This is the single-threaded-only fast version of btree_leaflink.
template <typename N> struct btree_leaflink<N, false> {
    static void link_split(N *n, N *nr) {
	link_split(n, nr, do_nothing());
    }
#ifdef HACK_SILO
    template <typename SF>
    static void link_split(N *n, N *nr, SF) {
	nr->prev_oid_ = n ? n->oid : 0;
	nr->next_oid_ = n->next_oid_;
	n->next_oid_ = nr ? nr->oid : 0;

	if (nr->next_oid_)
	    reinterpret_cast<N*>(nr->fetch_node(nr->next_oid_))->prev_oid_ = nr->oid;
    }
    static void unlink(N *n) {
	unlink(n, do_nothing());
    }
    template <typename SF>
    static void unlink(N *n, SF) {
	if (n->next_oid_)
	{
		reinterpret_cast<N*>(n->fetch_node( n->next_oid_ ))->prev_oid_ = n->prev_oid_;
	}
	reinterpret_cast<N*>(n->fetch_node(n->prev_oid_))->next_oid_ = n->next_oid_;
    }
#else
    template <typename SF>
    static void link_split(N *n, N *nr, SF) {
	nr->prev_ = n;
	nr->next_.ptr = n->next_.ptr;
	n->next_.ptr = nr;
	if (nr->next_.ptr)
	    nr->next_.ptr->prev_ = nr;
    }
    static void unlink(N *n) {
	unlink(n, do_nothing());
    }
    template <typename SF>
    static void unlink(N *n, SF) {
	if (n->next_.ptr)
	    n->next_.ptr->prev_ = n->prev_;
	n->prev_->next_.ptr = n->next_.ptr;
    }
#endif
};

#endif
