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
		auto max = 0;
		for( auto i = 0; i < NR_SOCKETS; i++ )
		{
			if( max < *_alloc_offset[i] )
				max = *_alloc_offset[i];
		}
		return (max + 1) * NR_SOCKETS;
	}

    inline unsigned long long size(int skt)
    {
        return *_alloc_offset[skt];
    }

	object_vector( unsigned long long nelems)
	{
		for( int i = 0; i < NR_SOCKETS; i ++ )
		{
			_alloc_offset[i] = (unsigned int*)numa_alloc_onnode( sizeof( unsigned int ), i );
			*_alloc_offset[i] = 0;
		}
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
        ASSERT(oid < size());
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
		int cpu = sched_getcpu();
		ALWAYS_ASSERT(cpu >= 0 );

		// TODO. resizing
		int numa_node = cpu % NR_SOCKETS;		// by current topology
		auto local_offset = __sync_add_and_fetch( _alloc_offset[numa_node], 1 );		// on NUMA node CAS
		return local_offset * NR_SOCKETS + numa_node;
	}

	inline void dealloc(object* desc)
	{
		// TODO. 
	}

private:
	dynarray<fat_ptr> 		_obj_table;
	unsigned int* _alloc_offset[NR_SOCKETS];

};
