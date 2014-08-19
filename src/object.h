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
		ALWAYS_ASSERT( oid > 0 && oid <= _size );
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
		ALWAYS_ASSERT( oid > 0 && oid <= _size );
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
		return desc->_data;
	}

	inline object_type* begin( oid_type oid )
	{
		return volatile_read(_obj_table[oid]);
	}

	// TODO. delete ( atomic deletion with CAS and pass dummies to RCU? 

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
