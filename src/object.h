#pragma once

#include <cstring>
#include <atomic>
#include <cassert>
#include "macros.h"
#include "dbcore/dynarray.h"
#include <sched.h>
#include <numa.h>
#include <limits.h>
#include "dbcore/sm-common.h"

#define NR_SOCKETS 4
// each socket requests this many oids a time from global alloc
#define OID_EXT_SIZE 8192

typedef unsigned long long oid_type;

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
		return _global_oid_alloc_offset + 1;
    }

	object_vector( unsigned long long nelems)
	{
        for (int i = 0; i < NR_SOCKETS; i++) {
            _local_oid_alloc_offset[i] = OID_EXT_SIZE * i;
            _local_oid_allocated[i] = 0;
        }
        _start_oid = 1;
        _global_oid_alloc_offset = OID_EXT_SIZE * NR_SOCKETS;
		_obj_table = dynarray<fat_ptr>(  std::numeric_limits<unsigned int>::max(), nelems*sizeof(fat_ptr) );
		_obj_table.sanitize( 0,  nelems* sizeof(fat_ptr));
	}

	bool put( oid_type oid, fat_ptr new_head)
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );
		fat_ptr old_head = begin(oid);
		object* new_desc = (object*)new_head.offset();
		volatile_write( new_desc->_next, old_head);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid]._ptr, old_head._ptr, new_head._ptr) )
			return false;

        // new record, shuold be in cold store, no need to change temp bit
		return true;
	}
	bool put( oid_type oid, fat_ptr old_head, fat_ptr new_head )
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );
		object* new_desc = (object*)new_head.offset();
		volatile_write( new_desc->_next, old_head);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid]._ptr, old_head._ptr, new_head._ptr) )
			return false;

		return true;
	}

	inline fat_ptr begin( oid_type oid )
	{
        ASSERT(oid <= size());
		return volatile_read(_obj_table[oid]);
	}

    inline fat_ptr* begin_ptr(oid_type oid)
    {
        // tzwang: I guess we don't need volatile_read for this
        return (fat_ptr*)(&_obj_table[oid]._ptr);
    }

	void unlink( oid_type oid, T item )
	{
		object* target;
		fat_ptr prev;
		fat_ptr* prev_next;
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );

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

				return;
			}
			prev_next = &target->_next;	// only can be modified by current TX. volatile_read is not needed
			prev = volatile_read(*prev_next);
			target = (object*)prev.offset();
		}

		if( !target )
			ALWAYS_ASSERT(false);
	}

	inline oid_type alloc()
	{
        return alloc_core();

		int node = sched_getcpu() % NR_SOCKETS;
retry:
        uint64_t nallocated = __sync_add_and_fetch(&_local_oid_allocated[node], 1);
        int64_t overflow = nallocated - OID_EXT_SIZE;
        if (overflow == 0) {
            volatile_write(_local_oid_alloc_offset[node], alloc_oid_extent());
            volatile_write(_local_oid_allocated[node], 0);
        }
        else if (overflow > 0)
            goto retry;
        return _local_oid_alloc_offset[node] + nallocated; // oid starts at 1
    }

    inline oid_type alloc_core()
    {
        /*
        static __thread uint64_t _local_alloc_offset;
        static __thread uint64_t _local_allocated = OID_EXT_SIZE;

        if (_local_allocated == OID_EXT_SIZE) {
            _local_alloc_offset = alloc_oid_extent();
            _local_allocated = 0;
        }
        return _local_alloc_offset + (++_local_allocated);
        */
        if (_core_oid_remaining.my() == 0) {
            _core_oid_offset.my() = alloc_oid_extent();
            _core_oid_remaining.my() = OID_EXT_SIZE;
        }
        return _core_oid_offset.my() + OID_EXT_SIZE - (_core_oid_remaining.my()--) + 1;
    }

    inline uint64_t alloc_oid_extent() {
		uint64_t noffset = __sync_fetch_and_add(&_global_oid_alloc_offset, OID_EXT_SIZE);
		_obj_table.ensure_size(sizeof(object*) * (_global_oid_alloc_offset + 1));
        return noffset;
	}

    inline oid_type start_oid() {
        return _start_oid;
    }

    inline void try_update_start_oid(oid_type noid) {
        if (volatile_read(_start_oid) + 1 == noid) {
            volatile_write(_start_oid, noid);
        }
    }

    inline void dealloc_oid(oid_type oid) {
        try_update_start_oid(oid);
    }

private:
	dynarray<fat_ptr> 		_obj_table;
    uint64_t _local_oid_alloc_offset[NR_SOCKETS];
    uint64_t _local_oid_allocated[NR_SOCKETS];
    uint64_t _global_oid_alloc_offset;
    percore_lazy<uint64_t> _core_oid_offset;
    percore_lazy<uint64_t> _core_oid_remaining;
    oid_type _start_oid;
};
