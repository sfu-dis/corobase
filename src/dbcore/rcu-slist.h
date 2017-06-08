// -*- mode:c++ -*-
#ifndef __RCU_SLIST_H
#define __RCU_SLIST_H

#include "rcu.h"

#include <stdint.h>

using namespace RCU;

/***
    A singly-linked list that is latch-free and RCU-aware.

    Thanks to RCU, readers may freely traverse the list without fear
    of dangling pointers.

    Writers who perform an insert or delete use a wait-free protocol,
    based around the a notion of "live" and "dead" nodes. It is
    forbidden to change the "next" pointer of a dead list node, which
    avoids all sorts of nasty edge cases that could lead to
    double-free errors or memory leaks (e.g. inserting a new node into
    a sublist that has been unlinked during the insert process).

    Insertion is straightforward. The inserting thread traverses the
    list until it finds a suitable insertion point (often head of
    list, which requires no traversal), and then links the new node
    into the list. We maintain the invariant that a newly-inserted
    node cannot have a dead predecessor or dead successor, which may
    necessitate unlinking a dead sub-list as part of the insertion
    process.

    Removing an element is also simple: just flag the node as dead and
    all readers who encounter it from then on will ignore it. To keep
    dead nodes from accumulating, we impose the invariant that a node
    cannot be marked dead if its successor is also dead; if the
    successor is dead, the thread must unlink the dead sublist before
    continuing. If the last node in the list dies (after cleaning up
    dead successors), it must test for and remove the any streak of
    dead nodes (possibly including itself) from the head of the
    list. This ensures that the last node to die always leaves a list
    truly empty. Dead nodes, once unlinked, can be passed to rcu_free
    and will be reclaimed at the next quiescent point; readers unlucky
    enough to land on a dead node can still finish their list
    traversal and will still see any live nodes that succeed the
    deleted node.

    Optionally, a list can be marked as "dead" in order to prevent any
    new insertions. Only an empty list can be killed, however. That
    way, any in-progress inserts will either complete successfully
    (landing in a provably live list), or will fail cleanly (and the
    thread can retry or abort the insertion attempt, as
    appropriate). It is also possible to remove a node and mark the
    list as dead in a single atomic operation (a feature exploited
    internally by RCU).
*/
struct _rcu_slist {
  /* The "next" pointer of each list node doubles as a "dead" flag
     for the owning node.

     Deleting a node takes three steps:

     1. Marking the node as "dead" by setting the lowest-order bit
     of the address. List nodes are always word-aligned, so this bit
     is otherwise unused.

     2. Unlinking the node from the list, RCU-style. A dead node may
     still be referenced by readers, so its next pointer must remain
     valid until the next quiescent point. We can, however, update
     the dead node's predecessor to unlink the dead node so arriving
     threads no longer encounter it. We maintain the invariant that
     only a live node may change its successor, to prevent nasty
     edge cases, such as double-free and inserting new nodes into
     dead/unlinked sub lists.

     3. Once a node has been unlinked, it can be marked for deletion
     by the RCU infrastructure. As usual with RCU, the actual
     deletion will occur at the next quiescent point and in-flight
     readers may continue accessing the node until they quiesce.
  */
  struct node;

  enum owner_status { OWNER_LIVE = 0x0, OWNER_DEAD = 0x1 };

  struct next_ptr {
    uintptr_t val;

    next_ptr() { val = 0; }
    next_ptr(node *n, owner_status s = OWNER_LIVE);

    next_ptr operator=(next_ptr const &other) volatile {
      this->val = other.val;
      return other;
    }
    next_ptr &operator=(next_ptr const &other) {
      val = other.val;
      return *this;
    }

    bool owner_is_dead();

    void set_owner_status(owner_status s);

    node *get();

    node *operator->() { return get(); }

    next_ptr copy() volatile {
      next_ptr rval;
      rval.val = volatile_read(RCU_READ(this)->val);
      return rval;
    }
    next_ptr copy() { return *this; }
    bool operator==(next_ptr const &other) { return val == other.val; }
    bool operator!=(next_ptr const &other) { return val != other.val; }
  };

  struct node {
    next_ptr volatile next;
  };

  template <typename Node = node>
  struct iterator {
    node *cur;
    next_ptr next;
    iterator() : cur(0), next(0, OWNER_LIVE) {}
    iterator(next_ptr volatile *head) : next(head->copy()) { ++*this; }

    Node *get() { return (Node *)cur; }
    Node &operator*() { return *get(); }
    Node *operator->() { return get(); }
    iterator &operator++() {
      while ((cur = next.get())) {
        next = cur->next.copy();
        if (not next.owner_is_dead()) break;
      }
      return *this;
    }

    bool operator==(iterator const &other) const { return cur == other.cur; }
    bool operator!=(iterator const &other) const { return cur != other.cur; }
  };

  /* Visit every node in the list, including dead nodes if
     [show_dead] is true. Pass each node and its live/dead status to
     the visitor, stopping the traversal early if it returns true.
   */
  template <typename Visitor>
  void _visit(Visitor v) {
    next_ptr it = this->head.copy();
    while (node *n = it.get()) {
      next_ptr other = n->next.copy();
      if (v(n, other.owner_is_dead())) break;

      it = other;
    }
  }

  template <typename Visitor>
  void visit(Visitor &v) {
    _visit<Visitor &>(v);
  }
  template <typename Visitor>
  void visit(Visitor const &v) {
    _visit<Visitor const &>(v);
  }

  next_ptr volatile head;

  _rcu_slist() : head(0, OWNER_LIVE) {}

  template <typename Node>
  iterator<Node> begin() {
    return iterator<Node>(&this->head);
  }

  iterator<node> begin() { return begin<node>(); }

  template <typename Node>
  iterator<Node> end() {
    return iterator<Node>();
  }

  iterator<node> end() { return end<node>(); }

  /* Return the node at the head of the list, or NULL if the list is empty.
   */
  node *peek() { return begin().get(); }

  /* Return the node that is physically at the head of the list, or
     NULL if the list is truly empty. Set non-null [valid] to true
     if the returned node is live.
   */
  node *peek_raw(bool *valid);

  /* Insert the given node at the head of the list. Its value should
     have already been set by the caller, and its next pointer
     should be uninitialized. A return value of false indicates that
     the list has been killed and the insert failed.
  */
  bool push(node *n);

  /* Same as push, but invoke cb(n, succ) just before attempting to
     install the new node. The second argument, [succ], is the
     would-be successor to [n] via n->next, and it presumably
     contains information needed to finish initializing [n].

     WARNING: The callback will be invoked before each [succ] it
     attempts to install as successor to [n]. Further, [succ] could
     die any time before, during, or after the callback (this *is* a
     latch-free list, after all).
   */
  typedef void(push_callback_fn)(void *x, node *n, node *succ);
  bool push_callback(node *n, push_callback_fn *cb, void *x);

  /* Insert the given node into the list in ascending order, using
     the given comparator to determine "less than" status.
   */
  template <typename Less>
  void insert_sorted(node *n);

  /* Remove the node from the list, performing any necessary cleanup
     to avoid resource leaks.
  */
  bool remove(node *n);

  /* Remove the node from the list without cleanup actions. This is
     cheaper, but only safe if the caller can prove some other
     agent(s) will clean up the list.
   */
  bool remove_fast(node *n);

  /* Attempt to mark the list as dead, failing if it contains any
     live nodes.

     Nodes cannot be added to a dead list; presumably the list has
     become unreachable and its replacement (if any) should be used
     instead.
   */
  bool kill();

  /* Remove the given node from the list and kill the list if the
     removal left it empty. Return true if the list was killed.

     This is equivalent to executing the following atomically:

     if(list.remove(n)) { list.kill(); }
   */
  bool remove_and_kill(node *n);
};

template <typename Node>
struct rcu_slist {
  static_assert(offsetof(Node, _node) == 0,
                "_node must be the first member of the Node class");

  _rcu_slist self;

  typedef typename _rcu_slist::iterator<Node> iterator;
  typedef _rcu_slist::push_callback_fn push_callback_fn;

  template <typename Visitor>
  struct visitor_helper {
    Visitor &v;
    bool operator()(_rcu_slist::node *n, bool is_dead) const {
      return v((Node *)n, is_dead);
    }
  };

  /* Return the node at the head of the list, or NULL if the list is empty.
   */
  Node *peek() { return begin().get(); }

  /* Return the node that is physically at the head of the list, or
     NULL if the list is truly empty. Set non-null [valid] to true
     if the returned node is live.
   */
  Node *peek_raw(bool *valid) { return (Node *)self.peek_raw(valid); }

  /* Insert the given node at the head of the list. Its value should
     have already been set by the caller, and its next pointer
     should be uninitialized. A return value of false indicates that
     the list has been killed and the insert failed.
  */
  bool push(Node *n) { return self.push(&n->_node); }

  /* Same as push, but invoke cb(n) just before attempting to
     install the new node. The callback can examine the potential
     successor to n via n->next, and will presumably initialize some
     part of n's state based on that successor.

     WARNING: The callback will be invoked before each (of
     potentially several) attempts to install [n]. Further, the
     successor could die any time before, during, or after the
     callback (this *is* a latch-free list, after all).
   */
  template <typename T>
  bool push_callback(Node *n, void (*cb)(T *, Node *, Node *), T *ptr) {
    return self.push_callback(&n->_node, (push_callback_fn *)cb, ptr);
  }
  template <typename Lambda, typename T>
  bool push_callback(Node *n, Lambda x, T *ptr) {
    void (*cb)(T *, Node *, Node *) = x;
    return push_callback(n, cb, ptr);
  }

  /* Remove the node from the list, performing any necessary cleanup
     to avoid resource leaks.
  */
  bool remove(Node *n) { return self.remove(&n->_node); }

  /* Remove the node from the list without cleanup actions. This is
     cheaper, but only safe if the caller can prove some other
     agent(s) will eventually clean up the list. Return true if
     cleanup actions should have been performed but were not.

     NOTE: to obey RCU semantics, we must mark a node as logically
     "removed" rather than physically unlinking it from the
     list. Without cleanup, the node would remain in the list until
     some action (push, remove, kill) affects the list ahead of it;
     those actions all clean up dead nodes as part of their work.
   */
  bool remove_fast(Node *n) { return self.remove_fast(&n->_node); }

  /* Attempt to mark the list as dead, failing if it contains any
     live nodes.

     Nodes cannot be added to a dead list; presumably the list has
     become unreachable and its replacement (if any) should be used
     instead.
   */
  bool kill() { return self.kill(); }

  /* Remove the given node from the list and kill the list if the
     removal left it empty. Return true if the list was killed.

     This is equivalent to executing the following atomically:

     if(list.remove(n)) { list.kill(); }
   */
  bool remove_and_kill(Node *n) { return self.remove_and_kill(&n->_node); }

  iterator begin() { return self.begin<Node>(); }
  iterator end() { return self.end<Node>(); }

  /* Visit every node in the list, including dead nodes if
     [show_dead] is true. Pass each node and its live/dead status to
     the visitor, stopping the traversal early if it returns true.
   */
  template <typename Visitor>
  void _visit(Visitor v) {
    visitor_helper<Visitor> vh = {v};
    self.visit(vh);
  }

  template <typename Visitor>
  void visit(Visitor &v) {
    _visit<Visitor &>(v);
  }
  template <typename Visitor>
  void visit(Visitor const &v) {
    _visit<Visitor const &>(v);
  }
};

#endif
