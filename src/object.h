#pragma once

#include <cstring>
#include <atomic>
#include <cassert>
#include "macros.h"
#include "dbcore/dynarray.h"

typedef unsigned long long oid_type;

class object
{
	public:
		object(char* data, object* next):  _next(next), _data((char*)data){}

		object* _next;
		char* _data;
};

template <typename T>
class object_vector
{

public:
	inline unsigned long long size() { return _nallocated-1; }

	object_vector( unsigned long long capacity)
	{
		_nallocated= 0;
		_obj_table = dynarray<object*>( capacity * sizeof(object*), capacity*sizeof(object*) );
		_obj_table.sanitize( 0, _obj_table.size() );
	}

	bool put( oid_type oid, object* new_desc )
	{
		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );
		object* first = begin(oid);
		volatile_write( new_desc->_next, first);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], first, new_desc) )
			return false;

		return true;
	}
	bool put( oid_type oid, object* head,  object* new_desc )
	{
		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );
		volatile_write( new_desc->_next, head);

		if( not __sync_bool_compare_and_swap( &_obj_table[oid], head, new_desc) )
			return false;

		return true;
	}

	bool put( oid_type oid, T item )
	{
		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );
		object* old_desc = _obj_table[oid];
		object* new_desc = new object( (char*)item, old_desc );

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
		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );
		object* desc= _obj_table[oid];
		return desc ? (T)desc->_data : 0;
	}

	inline object* begin( oid_type oid )
	{
		return volatile_read(_obj_table[oid]);
	}

	void unlink( oid_type oid, T item )
	{
		object* target;
		object** prev;
		ALWAYS_ASSERT( oid > 0 && oid <= _nallocated );

retry:
		prev = &_obj_table[oid];			// constant value. doesn't need to be volatile_read
		target = *prev;
		while( target )
		{
			if( target->_data == (char*)item )
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
		return __sync_add_and_fetch( &_nallocated, 1 );
	}

	inline void dealloc(object* desc)
	{
		// TODO. 
	}

private:
	dynarray<object*> 		_obj_table;
	volatile unsigned int _nallocated;

};
