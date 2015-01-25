#include "object.h"

object_vector::object_vector(unsigned long long nelems)
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
    ALWAYS_ASSERT(head->payload() == item);
    ALWAYS_ASSERT(__sync_bool_compare_and_swap(&begin_ptr(oid)->_ptr, head_ptr._ptr, head->_next._ptr));
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

