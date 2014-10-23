#pragma once

#include <cstring>
#include <atomic>
#include <cassert>
#include "macros.h"
#include "dbcore/dynarray.h"
#include <sched.h>
#include <numa.h>

#define NR_SOCKETS 4
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
	unsigned long long size() 
	{
		auto max = 0;
		for( auto i = 0 : NR_SOCKETS )
		{
			if( max < *_nallocated[i] )
				max = *_nallocated[i];
		}
		return max;
	}

	object_vector( unsigned long long capacity)
	{
		for( int i = 0; i < NR_SOCKETS; i ++ )
		{
			_nallocated[i] = (unsigned int*)numa_alloc_onnode( sizeof( unsigned int ), i );
			*_nallocated[i] = 0;
		}
		_obj_table = dynarray<object*>( capacity * sizeof(object*), capacity*sizeof(object*) );
		_obj_table.sanitize( 0,  capacity * sizeof(object*));
	}

	bool put( oid_type oid, object* new_desc )
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );
		object* first = begin(oid);
		volatile_write( new_desc->_next, first);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], first, new_desc) )
			return false;

		return true;
	}
	bool put( oid_type oid, object* head,  object* new_desc )
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );
		volatile_write( new_desc->_next, head);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], head, new_desc) )
			return false;

		return true;
	}

	inline object* begin( oid_type oid )
	{
		return volatile_read(_obj_table[oid]);
	}

	void unlink( oid_type oid, T item )
	{
		object* target;
		object** prev;
//		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );

retry:
		prev = &_obj_table[oid];			// constant value. doesn't need to be volatile_read
		target = *prev;
		while( target )
		{
			if( target->payload() == (char*)item )
			{
				if( not __sync_bool_compare_and_swap( prev, target, target->_next ) )
					goto retry;

				//TODO. dealloc version container
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
		// bump allocator
		// FIXME. resizing is needed
		//_obj_table.ensure_size( sizeof( object*) );	
		int cpu = sched_getcpu();
		ALWAYS_ASSERT(cpu >= 0 );
		int numa_node = cpu % NR_SOCKETS;		// by current topology
		auto local_offset = __sync_add_and_fetch( _nallocated[numa_node], 1 );		// on NUMA node CAS
		return local_offset * NR_SOCKETS + numa_node;
	}

	inline void dealloc(object* desc)
	{
		// TODO. 
	}

private:
	dynarray<object*> 		_obj_table;
	unsigned int* _nallocated[NR_SOCKETS];

};
