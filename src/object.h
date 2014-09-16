#pragma once

#include <cstring>
#include <atomic>
#include <vector>
#include <cassert>
#include "macros.h"

typedef unsigned long long oid_type;

template <typename T>
class object
{
	public:
		object(T data, object* next):  _data(data), _next(next){}

		T _data;
		object* _next;
};

template <typename T>
class object_vector
{
	typedef object<T> object_type;

public:
	object_vector( unsigned long long capacity)
	{
		_nallocated= 0;
		_obj_table.resize( capacity );
		std::fill( _obj_table.begin(), _obj_table.end(), reinterpret_cast<object_type*>(NULL) );
		_size = capacity;
	}

	bool put( oid_type oid, object_type* head, T item )
	{
		ALWAYS_ASSERT( oid > 0 && oid <= _size );
		object_type* new_desc = new object_type( item, head );

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], head, new_desc ) )
		{
			delete new_desc;
			return false;
		}
		return true;
	}
	bool put( oid_type oid, T item )
	{
		ALWAYS_ASSERT( oid > 0 && oid < _size );
		object_type* old_desc = _obj_table[oid];
		object_type* new_desc = new object_type( item, old_desc );

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], old_desc, new_desc ) )
		{
			delete new_desc;
			return false;
		}
		return true;
	}

	oid_type insert( T item )
	{
		oid_type oid = alloc();
		ALWAYS_ASSERT( not _obj_table[oid] );
		if( put( oid, item ) )
			return oid;
		else 
			ALWAYS_ASSERT(false);
		return 0;	// shouldn't reach here
	}

	inline T get( oid_type oid )
	{
		ALWAYS_ASSERT( oid <= _size );
		ALWAYS_ASSERT( oid > 0 && oid <= _size );
		object_type* desc= _obj_table[oid];
		return desc ? desc->_data : 0;
	}

	inline object_type* begin( oid_type oid )
	{
		return volatile_read(_obj_table[oid]);
	}

	void unlink( oid_type oid, T item )
	{
		object_type* target;
		object_type** prev;
		INVARIANT( oid );

retry:
		prev = &_obj_table[oid];			// constant value. doesn't need to be volatile_read
		target = *prev;
		while( target )
		{
			if( target->_data == item )
			{
				if( not __sync_bool_compare_and_swap( prev, target, target->_next ) )
					goto retry;

				//TODO. dealloc version container
				return;
			}
			prev = &target->_next;	// only can be modified by current TX. volatile_read is not needed
			target = *prev;
		}

		if( !target )
			ALWAYS_ASSERT(false);
	}

private:
	std::vector<object_type*> 		_obj_table;
	std::atomic<unsigned long long> 	_nallocated;
	unsigned long long 			_size;

	inline oid_type alloc()
	{
		// TODO. oid allocator
		return ++_nallocated;
	}

	inline bool free(object_type* desc)
	{
		// unlink from chain, update previous node's next field.
		// update free entry info
		// pass to garbage list for rcu?
		return true;
	}
};
