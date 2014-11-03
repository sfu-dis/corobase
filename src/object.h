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

typedef unsigned long long oid_type;

struct dynarray;

// this really shound't live here...
#define RA_NUM_SEGMENTS 4

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
        _global_oid_alloc_offset = 0;
		_obj_table = dynarray(  std::numeric_limits<unsigned int>::max() * sizeof(fat_ptr), nelems*sizeof(fat_ptr) );

        for (uint i = 0; i < NR_SOCKETS; i++) {
            for (uint j = 0; j < RA_NUM_SEGMENTS; j++) {
                _bitmap[i][j] = dynarray(std::numeric_limits<unsigned int>::max(), (_obj_table.size() / sizeof(fat_ptr) + sizeof(fat_ptr)) / _oids_per_byte);
            }
        }
	}

	bool put( oid_type oid, fat_ptr new_head)
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );
		fat_ptr old_head = begin(oid);
		object* new_desc = (object*)new_head.offset();
		volatile_write( new_desc->_next, old_head);
		uint64_t* p = (uint64_t*)begin_ptr(oid);

		if( not __sync_bool_compare_and_swap( p, old_head._ptr, new_head._ptr) )
			return false;

        // new record, shuold be in cold store, no need to change temp bit
		return true;
	}
	bool put( oid_type oid, fat_ptr old_head, fat_ptr new_head )
	{
//		ALWAYS_ASSERT( oid > 0 && oid <= _alloc_offset );
		object* new_desc = (object*)new_head.offset();
		volatile_write( new_desc->_next, old_head);
		uint64_t* p = (uint64_t*)begin_ptr(oid);

		if( not __sync_bool_compare_and_swap( p, old_head._ptr, new_head._ptr) )
			return false;

		return true;
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
        if (_core_oid_remaining.my() == 0) {
            _core_oid_offset.my() = alloc_oid_extent();
            _core_oid_remaining.my() = OID_EXT_SIZE;
        }
        return _core_oid_offset.my() + OID_EXT_SIZE - (_core_oid_remaining.my()--) + 1;
    }

    inline uint64_t alloc_oid_extent() {
		uint64_t noffset = __sync_fetch_and_add(&_global_oid_alloc_offset, OID_EXT_SIZE);

		uint64_t obj_table_size = sizeof(fat_ptr) * (_global_oid_alloc_offset);
		_obj_table.ensure_size( obj_table_size + ( obj_table_size / 10) );			// 10% increase

        for (uint i = 0; i < NR_SOCKETS; i++) {
            for (uint j = 0; j < RA_NUM_SEGMENTS; j++) {
                _bitmap[i][j].ensure_size((_obj_table.size() / sizeof(fat_ptr) + sizeof(fat_ptr)) / _oids_per_byte);
            }
        }

        return noffset;
	}

    inline void set_temperature(oid_type oid, bool hot, int skt, int seg)
    {
        ASSERT(seg < RA_NUM_SEGMENTS);
        // tzwang: strictly speaking, we need this assert below; and in
        // practice, this assert will actually require these two things
        // to happen atomically as a whole: (1) transition state from
        // PREPARED to IN_PROG; (2) increment _gc_segment. But to do this,
        // we need one more state between PREPARED and IN_PROG, just for the
        // _gc_segment adjusting. Otherwise the evaluation of this assert
        // could first read the old gc_segment (whic is being incremented by
        // reclaim_daemon) and then read the state as IN_PROG (and it's
        // possible by this time the IN_PROG is set and _gc_segment is already
        // adjusted, however this is not the case when it was read in the
        // volatile_read part of the assertion).
        //
        // so I just commented it out.
        //
        //ASSERT(!(seg == volatile_read((RA::ra+skt)->_gc_segment) &&
        //        (RA::ra+skt)->state() == RA_GC_IN_PROG));
    retry:
        uint64_t bit_nr = oid / _oids_per_bit;
        uint64_t idx = bit_nr / 8 / sizeof(uint64_t) * sizeof(uint64_t);
        uint64_t *bitmap = (uint64_t *)&_bitmap[skt][seg][idx];
        uint64_t old_map = *bitmap;
        uint64_t new_map = old_map;

        if (hot)
            new_map = old_map | ((uint64_t){1} << (bit_nr % 64));
        else
            new_map = old_map & (~((uint64_t){1} << (bit_nr % 64)));

        if (new_map != old_map) {
            *bitmap = new_map;
            if (!__sync_bool_compare_and_swap(bitmap, old_map, new_map)) {
                if (hot){
                    goto retry;
                }
            }
            //__sync_synchronize();
            ASSERT(*bitmap == new_map);
            ASSERT(new_map == (uint64_t)_bitmap[skt][seg][idx]);
        }
    }

    inline uint64_t *bitmap_ptr(int socket, int segment, uint64_t offset)
    {
        // offset must be 64-bit size aligned as gc reads in this way
        ASSERT(offset % sizeof(uint64_t) == 0);
        return (uint64_t *)&_bitmap[socket][segment][offset];
    }

private:
	dynarray 		_obj_table;
    uint64_t _global_oid_alloc_offset;
    percore<uint64_t, false, false> _core_oid_offset;
    percore<uint64_t, false, false> _core_oid_remaining;

public:
    // each segment (i.e., gc cycle) has one bitmap for each gc daemon
	dynarray _bitmap[NR_SOCKETS][RA_NUM_SEGMENTS];
	uint64_t _oids_per_bit = 4;
    uint64_t _oids_per_byte = _oids_per_bit * 8;
    uint64_t _oids_per_cacheline = _oids_per_byte * CACHELINE_SIZE;
};
