#pragma once

#include <cstring>
#include <atomic>
#include <cassert>
#include "macros.h"
#include "dbcore/dynarray.h"
#include <sched.h>
#include <numa.h>
#include <limits>
#include "dbcore/sm-common.h"
#include "dbcore/sm-alloc.h"

#define NR_SOCKETS 4
// each socket requests this many oids a time from global alloc
#define OID_EXT_SIZE 8192

typedef uint64_t oid_type;

struct dynarray;

class object
{
	public:
		object( size_t size ) : _size(size) { _next = fat_ptr::make( (void*)0, INVALID_SIZE_CODE); }
		inline char* payload() { return (char*)((char*)this + sizeof(object)); }

		fat_ptr _next;
		size_t _size;			// contraint on object size( practical enough )
};

template <typename T>
class object_vector
{
public:
	inline unsigned long long size() 
	{
		return _global_oid_alloc_offset;
    }

	object_vector( unsigned long long nelems)
	{
        _global_oid_alloc_offset = 0;
		_obj_table = dynarray(std::numeric_limits<unsigned int>::max() * sizeof(fat_ptr),
                              nelems*sizeof(fat_ptr) );
	}

	bool put( oid_type oid, fat_ptr new_head)
	{
		fat_ptr old_head = begin(oid);
		object* new_desc = (object*)new_head.offset();
		volatile_write( new_desc->_next, old_head);
		uint64_t* p = (uint64_t*)begin_ptr(oid);
		return __sync_bool_compare_and_swap(p, old_head._ptr, new_head._ptr);
	}
	bool put(oid_type oid, fat_ptr old_head, fat_ptr new_head, bool overwrite)
	{
        // remove uncommitted overwritten version
        // (tx's repetitive updates, keep the latest one only)
        // Note for this to be correct we shouldn't allow multiple txs
        // working on the same tuple at the same time.
		object* new_desc = (object*)new_head.offset();
        if (overwrite) {
            object *old_desc = (object *)old_head.offset();
            volatile_write(new_desc->_next, old_desc->_next);
        }
        else {
            volatile_write(new_desc->_next, old_head);
        }

        // The caller of this function (update_version) will return the old
        // head, even for in-place update. So the caller of update_version
        // (do_tree_put) will need to free that overwritten version
        // as the tx needs to copy various stamps from the overwritten version.
        return __sync_bool_compare_and_swap((uint64_t *)begin_ptr(oid), old_head._ptr, new_head._ptr);
	}

	inline fat_ptr begin( oid_type oid )
	{
        ASSERT(oid <= size());
		fat_ptr* ret = begin_ptr(oid);
		return volatile_read(*ret);
	}

    inline fat_ptr* begin_ptr(oid_type oid)
    {
        // tzwang: I guess we don't need volatile_read for this
        return (fat_ptr*)(&_obj_table[oid * sizeof(fat_ptr)]);
    }

	void unlink( oid_type oid, T item )
	{
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

				RA::deallocate( (void*)target );
				return;
			}
			prev_next = &target->_next;	// only can be modified by current TX. volatile_read is not needed
			prev = volatile_read(*prev_next);
			target = (object*)prev.offset();
		}

		if( !target )
			ALWAYS_ASSERT(false);
	}

    // use with caution! this removes the head, the only user now is abort_impl (when removing updates).
    void undo_head_update(oid_type oid)
    {
        fat_ptr old_head = begin(oid);
        object *old_obj = (object *)old_head.offset();
        ALWAYS_ASSERT(__sync_bool_compare_and_swap((uint64_t *)begin_ptr(oid)->_ptr, old_head._ptr, old_obj->_next._ptr));
    }

	inline oid_type alloc()
	{
        if (_core_oid_remaining.my() == 0) {
            _core_oid_offset.my() = alloc_oid_extent();
            _core_oid_remaining.my() = OID_EXT_SIZE;
        }
        if (unlikely(_core_oid_offset.my() == 0))
            _core_oid_remaining.my()--;
        return _core_oid_offset.my() + OID_EXT_SIZE - (_core_oid_remaining.my()--);
    }

    inline uint64_t alloc_oid_extent() {
		uint64_t noffset = __sync_fetch_and_add(&_global_oid_alloc_offset, OID_EXT_SIZE);

		uint64_t obj_table_size = sizeof(fat_ptr) * (_global_oid_alloc_offset);
		_obj_table.ensure_size( obj_table_size + ( obj_table_size / 10) );			// 10% increase

        return noffset;
	}

private:
	dynarray 		_obj_table;
    uint64_t _global_oid_alloc_offset;
    percore<uint64_t, false, false> _core_oid_offset;
    percore<uint64_t, false, false> _core_oid_remaining;
};
