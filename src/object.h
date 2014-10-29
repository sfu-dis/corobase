#pragma once

#include <cstring>
#include <atomic>
#include <cassert>
#include "macros.h"
#include "dbcore/dynarray.h"
#include <sched.h>
#include <numa.h>
#include <limits.h>

#define NR_SOCKETS 4
// each socket requests this many oids a time from global alloc
#define OID_EXT_SIZE 8192

typedef unsigned long long oid_type;

class object
{
	public:
		object( size_t size ) : _next(NULL), _size(size) {}
		inline char* payload() { return (char*)((char*)this + sizeof(object)); }

		object* _next;
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
		_obj_table = dynarray<object*>(  std::numeric_limits<unsigned int>::max(), nelems*sizeof(object*) );
		_obj_table.sanitize( 0,  nelems* sizeof(object*));
	}

	bool put( oid_type oid, object* new_desc )
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );
		object* first = begin(oid);
		volatile_write( new_desc->_next, first);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], first, new_desc) )
			return false;

        // new record, shuold be in cold store, no need to change temp bit
		return true;
	}
	bool put( oid_type oid, object* head,  object* new_desc )
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );
		volatile_write( new_desc->_next, head);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], head, new_desc) )
			return false;

		return true;
	}

	inline object* begin( oid_type oid )
	{
        ASSERT(oid <= size());
		return volatile_read(_obj_table[oid]);
	}

    inline object** begin_ptr(oid_type oid)
    {
        // tzwang: I guess we don't need volatile_read for this
        return &_obj_table[oid];
    }

	void unlink( oid_type oid, T item )
	{
		object* target;
		object** prev;
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );

retry:
		prev = &_obj_table[oid];			// constant value. doesn't need to be volatile_read
		target = *prev;
		while( target )
		{
			if( target->payload() == (char*)item )
			{
				if( not __sync_bool_compare_and_swap( prev, target, target->_next ) )
					goto retry;

				return;
			}
			prev = &target->_next;	// only can be modified by current TX. volatile_read is not needed
			target = volatile_read(*prev);
		}

		if( !target )
			ALWAYS_ASSERT(false);
	}

	inline oid_type alloc()
	{
		int node = sched_getcpu() % NR_SOCKETS;
retry:
        uint64_t nallocated = __sync_add_and_fetch(&_local_oid_allocated[node], 1);
        int64_t overflow = nallocated - OID_EXT_SIZE;
        if (overflow == 0) {
            volatile_write(_local_oid_alloc_offset[node], alloc_oid_extent(node));
            volatile_write(_local_oid_allocated[node], 0);
        }
        else if (overflow > 0)
            goto retry;
        return _local_oid_alloc_offset[node] + nallocated; // oid starts at 1
    }

    inline uint64_t alloc_oid_extent(int socket) {
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
    dynarray<object*> _obj_table;
    uint64_t _local_oid_alloc_offset[NR_SOCKETS];
    uint64_t _local_oid_allocated[NR_SOCKETS];
    uint64_t _global_oid_alloc_offset;
    oid_type _start_oid;
};
