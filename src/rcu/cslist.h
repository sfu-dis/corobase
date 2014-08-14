// -*- mode:c++ -*-
#ifndef __CSLIST_H
#define __CSLIST_H

/* A simple circular singly-linked list.

   Pushing at either end of the list costs O(1) time, as is popping
   from the head of the list. Popping any node other than the head
   requires time linear in the list size. FIFO and LIFO behavior are
   achieved by pushing to head and tail of list, respectively.
   
   Forward iteration (from head to tail) is efficient; backward
   iteration is unsupported.

   TODO: implement list splicing operations. These will take O(1) time
   regardless of the list sizes involved. A list can be spliced in
   before any iterator position, including both begin() and end(),
   which make the target list the new head and tail,
   respectively. Obviously, splicing into the middle of a list
   requires finding the insertion spot---usually with linear cost.

   The list does not track its own size. It also does not track (or
   verify) list membership of a given node before performing a
   requested action. The user should do these things if desired (and
   is responsible for keeping that information up to date).
   
   No concurrency control is implied or assumed; the user is
   responsible to protect the list from concurrent access.

   This is an "embedded" list, meaning that list nodes are part of the
   user's payload structure. This allows maximum efficiency and avoids
   memory fragmentation, at some cost in flexibility to the user.  A
   single user structure can be part of multiple lists, as long as
   each list uses a different set of links.
 */

template <typename T, T* T::*next_ptr>
struct cslist {
    typedef T Node;

    struct iterator {
        Node *prev;
        bool advanced;
        
        Node &operator*() { return *next(prev); }
        Node *operator->() { return next(prev); }

        iterator &operator++() {
            advanced = true;
            prev = next(prev);
            return *this;
        }

        bool operator==(iterator const &other) const {
            return prev == other.prev and advanced == other.advanced;
        }
        bool operator!=(iterator const &other) const {
            return not (*this == other);
        }
    };

    /* Create a new, empty list */
    cslist()
        : _tail(0)
    {
    }
    
    /* As a matter of policy, the list must be empty before being
       discarded. Otherwise we have no way to know if the memory
       leaked. A user can call unsafe_clear() to deliberately "leak"
       the list's contents (perhaps because it was allocated from some
       pool or block allocator and will be freed en masse).

       We can't actually make the assertion, however, because that
       would produce a non-trivial destructor that makes the object
       unusable with rcu_new.
     */
#if 0
    ~cslist() {
        ASSERT(empty());
    }
#endif
    
    /* Push a new node to the head of the list. It will be the first
       node visited by a new iterator, and the first to be popped by a
       call to pop_head.
     */
    void push_head(Node *n) {
        if (_tail) {
            next(n) = next(_tail);
            next(_tail) = n;
        }
        else {
            next(n) = n;
            _tail = n;
        }
    }

    /* Push a new node to the tail of the list. It will be the last
       node visited by a new iterator, or popped by a call to
       pop_head.
     */
    void push_tail(Node *n) {
        push_head(n);
        _tail = n;
    }

    /* Pop the current head from the list.

       Return NULL if the list is currently empty
    */
    Node *pop_head() {
        if (not _tail)
            return 0;

        Node *n = next(_tail);
        if (n == _tail) 
            _tail = 0;
        else
            next(_tail) = next(n);
        return n;
    }
    
    /* Return true if the list currently contains no elements
     */
    bool empty() const {
        return not _tail;
    }

    /* Deliberately clear the list without popping any elements it
       might contain. The list destructor can be invoked safely after
       this call returns, but any elements that were present will leak
       if they are not accounted for by other means.
     */
    void unsafe_clear() {
        _tail = 0;
    }

    /* Iterators! */
    iterator begin() {
        return (iterator) { _tail, not _tail };
    }
    iterator end() {
        return (iterator) { _tail, true };
    }
    
    static Node *&next(Node *n) {
        return n->*next_ptr;
    }

    Node *_tail;
};
#endif
