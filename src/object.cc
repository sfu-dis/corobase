// DISABLE THE OLD TUPLE VECTOR AND TABLE LOCK IMPLEMENTATIONS
#if 0
#include "object.h"

object_vector::object_vector(unsigned long long nelems)
#ifdef PHANTOM_PROT_TABLE_LOCK
    : _lock(0)
#endif
{
    _global_oid_alloc_offset = 0;
    _obj_table = dynarray(std::numeric_limits<unsigned int>::max() * sizeof(fat_ptr),
                          nelems*sizeof(fat_ptr) );
}

bool
object_vector::put(oid_type oid, fat_ptr new_head)
{
#if CHECK_INVARIANTS
    object* new_desc = (object*)new_head.offset();
    ASSERT(not new_desc->_next.offset());
#endif
    return __sync_bool_compare_and_swap(&begin_ptr(oid)->_ptr, 0, new_head._ptr);
}

// The caller of this function (update_version) will return the old
// head, even for in-place update. So the caller of update_version
// (do_tree_put) will need to free that overwritten version
// as the tx needs to copy various stamps from the overwritten version.
bool
object_vector::put(oid_type oid, fat_ptr old_head, fat_ptr new_head, bool overwrite)
{
    // remove uncommitted overwritten version
    // (tx's repetitive updates, keep the latest one only)
    // Note for this to be correct we shouldn't allow multiple txs
    // working on the same tuple at the same time.
    object* new_desc = (object*)new_head.offset();
    if (overwrite) {
        object *old_desc = (object *)old_head.offset();
        volatile_write(new_desc->_next, old_desc->_next);
        // I already claimed it, no need to use cas then
        volatile_write(begin_ptr(oid)->_ptr, new_head._ptr);
        __sync_synchronize();
        return true;
    }
    else {
        volatile_write(new_desc->_next, old_head);
        return __sync_bool_compare_and_swap((uint64_t *)begin_ptr(oid), old_head._ptr, new_head._ptr);
    }
}

void
object_vector::unlink(oid_type oid, void *item)
{
    // Now the head is guaranteed to be the only dirty version
    // because we unlink the overwritten dirty version in put,
    // essentially this function ditches the head directly.
    // Otherwise use the commented out old code.
    fat_ptr head_ptr = begin(oid);
    object *head = (object *)head_ptr.offset();
    INVARIANT(head->payload() == item);
    //ALWAYS_ASSERT(__sync_bool_compare_and_swap(&begin_ptr(oid)->_ptr, head_ptr._ptr, head->_next._ptr));
    // actually the CAS is overkill: head is guaranteed to be the (only) dirty version
    volatile_write(begin_ptr(oid)->_ptr, head->_next._ptr);
    __sync_synchronize();
    // FIXME: tzwang: also need to free the old head during GC
    // Note that a race exists here: some reader might be using
    // that old head in fetch_version while we do the above CAS.
    // So we can't immediate deallocate it here right now.
#if 0
    object* target;
    fat_ptr prev;
    fat_ptr* prev_next;

retry:
    prev_next = begin_ptr( oid );			// constant value. doesn't need to be volatile_read
    prev= volatile_read(*prev_next);
    target = (object*)prev.offset();
    while( target )
    {
        if( target->payload() == (char*)item )
        {
            if( not __sync_bool_compare_and_swap( (uint64_t *)prev_next, prev._ptr, target->_next._ptr ) )
                goto retry;

            //RA::deallocate( (void*)target );
            return;
        }
        prev_next = &target->_next;	// only can be modified by current TX. volatile_read is not needed
        prev = volatile_read(*prev_next);
        target = (object*)prev.offset();
    }

    if( !target )
        ALWAYS_ASSERT(false);
#endif
}

#ifdef PHANTOM_PROT_TABLE_LOCK
bool
object_vector::lock(table_lock_t *lock, table_lock_t rmode)
{
    table_lock_t l = -1;
    table_lock_t counter = -1;
    do {
        l = volatile_read(*lock);
        counter = l & (~TABLE_LOCK_MODE_MASK);

        // counter == 0 is the same as mode = 0... unlock doesn't clear mode bits
        // doomed if requesting SIX and the lock is granted, no matter what type
        // (so it's important the same tx doesn't call lock() twice...)
        if (rmode == TABLE_LOCK_SIX and counter)
            return false;

        table_lock_t mode = l & TABLE_LOCK_MODE_MASK;
        if (counter and mode != rmode and mode != TABLE_LOCK_N)
            return false;
    }
    while (not __sync_bool_compare_and_swap(lock, l, rmode | (counter + 1)));

    // check overflow
    ASSERT(volatile_read(*lock) and volatile_read(*lock) & TABLE_LOCK_MODE_MASK);
    return true;
}

void
object_vector::unlock(table_lock_t *lock)
{
    // no need to worry about MSB, lock() will handle it
    __sync_fetch_and_sub(lock, 1);
}

// upgrade from S/X to SIX if I'm the only user
// for S, wait for other readers to go away
// for X, wait for other inserters to go away
// ^^^^^this could cause deadlock, two threads originally holding S
// lock wait for each other. We sacrifice the tx that has retried 3 times
// IMPORTANT: caller should make sure it owns this lock before calling
// TODO: figure out when should spin, or block?
bool
object_vector::upgrade_lock(table_lock_t *lock)
{
    table_lock_t mode = -1;
    int8_t attempts = 0;
    do {
        if (attempts++ == 3)
            return false;
        table_lock_t l = volatile_read(*lock);
        // no need to do anything if mode is already SIX
        if ((l & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX)
            return true;
        ALWAYS_ASSERT(l & TABLE_LOCK_MODE_MASK and (l << TABLE_LOCK_MODE_BITS));
        mode = l & TABLE_LOCK_MODE_MASK;
    }
    while (not __sync_bool_compare_and_swap(lock, mode | 1, TABLE_LOCK_SIX | 1));
    return true;
}

#endif
#endif // end of [#if 0]
