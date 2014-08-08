#pragma once

#include <cstring>
#include <atomic>
#include <vector>
#include <cassert>

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

	void put( oid_type oid, T item )
	{
		assert( oid <= _size );

retry:
		object_type* old_desc = _obj_table[oid];
		object_type* new_desc = new object_type( item, old_desc );

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], old_desc, new_desc ) )
		{
			delete new_desc;
			goto retry;
		}
	}

	oid_type insert( T item )
	{
		oid_type oid = alloc();
		assert( not _obj_table[oid] );
		put( oid, item );
		return oid;
	}

	inline T get( oid_type oid )
	{
		assert( oid <= _size );
		object_type* desc= _obj_table[oid];
		return desc->_data;
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
