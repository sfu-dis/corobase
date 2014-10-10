#include "rcu-slist.h"

#include <cstdio>

using namespace RCU;

//#include "rcu.h"
extern void RCU::rcu_free(void const*);

namespace {
    typedef _rcu_slist::node node;
    typedef _rcu_slist::next_ptr next_ptr;
    typedef _rcu_slist::owner_status owner_status;
    struct cached_next {
        next_ptr volatile *ptr;
        next_ptr copy;

        cached_next() { /* uninitialized */ }
        
        cached_next(next_ptr volatile *n)
            : ptr(n), copy(n->copy())
        {
        }

        cached_next &operator=(cached_next const &other) {
            ptr = other.ptr;
            copy = other.copy;
            return *this;
        }

        bool atomic_cas(node *n, owner_status s) {
            next_ptr nv(n, s);
            uintptr_t rval = __sync_val_compare_and_swap(&ptr->val, copy.val, nv.val);
            if (rval == copy.val) {
                copy.val = nv.val;
                return true;
            }
            copy.val = rval;
            return false;
        }
    };

    static
    node* find_live_successor(cached_next const &point) {
        cached_next other = point;
        while (node *succ = other.copy.get()) {
            cached_next tmp = &succ->next;
            if (not tmp.copy.owner_is_dead()) 
                return succ;

            // keep looking
            other = tmp;
        }

        // EOL
        return 0;
    }

    static inline
    bool install_successor(cached_next &point, node *dead_end, node *n, owner_status s) {
        next_ptr tmp = point.copy;
        if (not point.atomic_cas(n, s)) {
            // caller will sort out what went wrong
            return false;
        }
    
        /* Successfully installed [n], free up dead sublist to finish.

           Note that we access x->next after passing it rcu_free. This is
           OK because RCU won't actually free the node (or even alter it)
           until after the next quiescent point.
        */
        while (1) {
            node *x = tmp.get();
            if (x == dead_end)
                break;
        
            tmp = x->next.copy();
            ASSERT (tmp.owner_is_dead());
            rcu_free(x);
        }
    
        return true;
    }
}

_rcu_slist::next_ptr::next_ptr(node *n, owner_status s) {
    union {
        node *p;
        uintptr_t n;
    } u = {n};
    if (s == OWNER_DEAD)
        u.n |= OWNER_DEAD;
    val = u.n;
}

bool _rcu_slist::next_ptr::owner_is_dead() {
    return val & OWNER_DEAD;
}

void _rcu_slist::next_ptr::set_owner_status(owner_status s) {
    if (s == OWNER_DEAD)
        val |= OWNER_DEAD;
    else
        val &= -2;
}
    
_rcu_slist::node *_rcu_slist::next_ptr::get() {
    union {
        uintptr_t n;
        node* v;
    } u = {this->val & -2};
    return u.v;
}

bool _rcu_slist::kill() {
    cached_next point = &this->head;
 retry:
    node *succ = find_live_successor(point);
    bool killed = not succ;
    owner_status s = killed? OWNER_DEAD : OWNER_LIVE;
    if (not install_successor(point, succ, succ, s)) {
        if (point.copy.owner_is_dead()) {
            // somebody else killed the list first
            return false;
        }

        // successor changed, try again
        goto retry;
    }

    // kill succeeds only if there was no successor
    ASSERT (killed == point.copy.owner_is_dead());
    return killed;
}

namespace {
    bool list_push(cached_next &point, node *n, _rcu_slist::push_callback_fn *cb, void *x) {
        node *succ = find_live_successor(point);
        if (cb)
            cb(x, n, succ);

        /* [point] is the correct predecessor to [n], and [succ] is the
           correct successor to [n]. Remember the head of the dead
           sublist, then try to CAS everything into place.

           NOTE: In spite of our scan above, our successor may die before
           we finish linking to it. That's OK, because unlinking dead
           sublists is just a courtesy to other threads (since we had to
           do a CAS either way).
        */
        n->next = next_ptr(succ, _rcu_slist::OWNER_LIVE);
        if (not install_successor(point, succ, n, _rcu_slist::OWNER_LIVE)) {
            if (point.copy.owner_is_dead()) {
                // yikes! list killed!
                return false;
            }

            // somebody else got here first, start over
            return list_push(point, n, cb, x);
        }
    
        return true;
    }
}

_rcu_slist::node *
_rcu_slist::peek_raw(bool *valid)
{
    cached_next p = &this->head;
    if (valid)
        *valid = not p.copy.owner_is_dead();
    return p.copy.get();
}

bool __attribute__((flatten))
_rcu_slist::push(node *n)
{
    cached_next point = &this->head;
    return list_push(point, n, NULL, NULL);
}

bool __attribute__((flatten))
_rcu_slist::push_callback(node *n, push_callback_fn *cb, void *x)
{
    ASSERT (cb != NULL);
    cached_next point = &this->head;
    return list_push(point, n, cb, x);
}

/* Mark [n] as dead and clean up downstream. Return true if an
   upstream cleanup is also necessary (ie because no live nodes
   remain downstream).
*/
bool //__attribute__((flatten))
_rcu_slist::remove_fast(node *n)
{
    /*
      The actual removal is simple enough: just mark the node as dead.
      To avoid leaks, however, we impose the invariant that a node
      with a dead successor may not be marked dead (the dead successor
      must be unlinked first). That forces us to use CAS.
    */
    
    cached_next point = &n->next;
 retry:
    // we haven't been deleted yet...
    ASSERT(not point.copy.owner_is_dead());
    node *succ = find_live_successor(point);
    if (not install_successor(point, succ, succ, OWNER_DEAD)) {
        // hopefully not a double-free attempt
        ASSERT (not point.copy.owner_is_dead());

        goto retry;
    }

    if (succ) {
        node *x = point.copy.get();
        cached_next tmp = &x->next;
        if (not tmp.copy.owner_is_dead()) {
            // live successor confirmed, we're done!
            return false;
        }
    }

    // no live successor
    return true;

}

/* Remove the given node from the list. Return true if the list became
   empty as a result (in case the caller wants to delete the list).

   NOTE: in the event that a list kill arrives just after [n] has been
   removed, but before we can confirm that [n] was the last element of
   the list, then we return false. 

   WARNING: once this function returns, the caller should not access
   the passed-in list node any more.
   
   WARNING: caller is responsible to ensure that only one thread tries
   to delete each node.
 */
bool //__attribute__((flatten))
_rcu_slist::remove(node *n)
{
    
    if (not remove_fast(n))
        return false;

    /* Couldn't confirm a live successor, have to check from list
       head.  Note that the list could be killed at any moment because
       our node is no longer live.
     */
    cached_next point = &this->head;
    node *succ = find_live_successor(point);
    if (not install_successor(point, succ, succ, OWNER_LIVE)) {
        /* The list may have died. If the list did not die, then the
           head's successor must have changed. A successor change
           could only be due to an insert (in which case the list is
           clearly not empty), or a head cleanup similar to ours (in
           which case the other thread either proved the list is
           non-empty or received the blame for emptying it).

           In all three cases, removing [n] did not "cause" the list
           to become empty. 
        */
        return false;
    }

    /* Installation succeeded. The list became empty only if there was
       no successor; otherwise, we have proven the existence of a live
       successor, even if it has since died.
    */
    return not succ;
}

bool //__attribute__((flatten))
_rcu_slist::remove_and_kill(node *n)
{
    /*
      The actual removal is simple enough: just mark the node as dead.
      To avoid leaks, however, we impose the invariant that a node
      with a dead successor may not be marked dead (the dead successor
      must be unlinked first). That forces us to use CAS.
    */
    
    cached_next point = &n->next;
 retry:
    // we haven't been deleted yet...
    ASSERT(not point.copy.owner_is_dead());
    node *succ = find_live_successor(point);
    if (not install_successor(point, succ, succ, OWNER_DEAD)) {
        // hopefully not a double-free attempt
        ASSERT (not point.copy.owner_is_dead());
        goto retry;
    }

    if (succ) {
        node *x = point.copy.get();
        cached_next tmp = &x->next;
        if (not tmp.copy.owner_is_dead()) {
            // live successor confirmed, we're done!
            return false;
        }
    }

    /* Couldn't confirm a live successor, have to check from list
       head.  Note that the list could become (or is already) empty
       and could therefore be killed at any moment.
     */
    point = &this->head;
    succ = find_live_successor(point);
    owner_status s = succ? OWNER_LIVE : OWNER_DEAD;
    if (not install_successor(point, succ, succ, s)) {
        /* The list may have died. If the list did not die, then the
           head's successor must have changed. A successor change
           could only be due to an insert (in which case the list is
           clearly not empty), or a head cleanup similar to ours (in
           which case the other thread either proved the list is
           non-empty or received the blame for emptying it).

           In all three cases, removing [n] did not "cause" the list
           to become empty. 
        */
        return false;
    }

    /* Installation succeeded. The list became empty only if there was
       no successor; otherwise, we have proven the existence of a live
       successor, even if it has since died.
    */
    return not succ;
}


/* Sorted list insertion works in single-threaded tests, but it has
   not been tested in a multi-threaded environment yet. It's not clear
   we need this, so there's no point testing/debugging it for now.
*/
#if 0
/* Insert the given node into the list, in ascending order as
   determined by [Less].
 */
template <typename Less>
void _rcu_slist::insert_sorted(node *n) {
    /* First, find the candidate insertion point. We want [point] to
       be the rightmost node satisfying Less(point->value, n->value)
       and [other] to be either EOL or the leftmost node not
       satisfying Less(other->value, n->value).

       Second, we have to unlink any dead nodes between [point] and [other].

       If we can CAS the new node into place: [point] [n] [other],
       then the dead nodes all become unlinked in one fell swoop and
       we can delete them at our leisure. If the CAS fails, either
       [point] died (and we must start over) or someone else unlinked
       the dead nodes and/or linked in a new successor (and we must
       resume the scan in case some of those new successors should
       precede [n]).

       Invariant: lt(point, n) is always true (assume that the list head has
       implicit value of -inf)
     */
    Less lt;
    
 start_over:
    cached_next point = &this->head;
 resume_scan:
    cached_next other = point;
    node *succ;
    while (1) {
        succ = other.copy.get();
        if (not succ) {
            // insert past-end
            break;
        }

        cached_next tmp = &succ->next;
        if (tmp.copy.owner_is_dead()) {
            // skip dead nodes
        }
        else if (lt(succ->value, n->value)) {
            // [succ] should precede [n], update [point]
            point = tmp;
        }
        else {
            // [n] should precede [succ]
            break;
        }

        // [succ] is either dead or is smaller than [n]
        other = tmp;
    }

    /* [point] is the correct predecessor to [n], and [succ] is the
       correct successor to [n]. Remember the head of the dead
       sublist, then try to CAS everything into place.

       Note that, even if we succeed at the CAS, the successor may
       have become dead. That's OK, because we only care about dead
       nodes to avoid making one our predecessor (which would cause
       our newly inserted node to be freed prematurely). Nobody can
       have reclaimed our dead successor unless they unlinked the
       whole dead sublist, and our successful CAS rules that case
       out. In fact, we only unlinked the dead sublist as a courtesy
       to other threads, since we already had to do a CAS anyway.
     */
    n->next = next_ptr(succ, false);
    next_ptr tmp = point.copy;
    if (not point.atomic_cas(n, false)) {
        if (point.copy.owner_is_dead()) {
            // yikes! we lost our predecessor!
            goto start_over;
        }

        // somebody else inserted here first, resume scan
        goto resume_scan;
    }
    
    /* Successfully installed [n], free up dead list to finish.

       Note that we access x->next after passing it rcu_free. This
       is OK because RCU won't actually free the node (or even
       alter it) until after the next quiescent point.
    */
    while (1) {
        node *x = tmp.get();
        if (x == succ)
            break;
        
        tmp = x->next.copy();
        ASSERT (tmp.owner_is_dead());
        rcu_free(x);
    }
}
#endif
