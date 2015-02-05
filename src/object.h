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
#include "core.h"

// each socket requests this many oids a time from global alloc
#define OID_EXT_SIZE 8192

typedef uint32_t oid_type;

struct dynarray;

class object
{
	public:
		object( size_t size ) : _size(size) { _next = fat_ptr::make( (void*)0, INVALID_SIZE_CODE); }
		inline char* payload() { return (char*)((char*)this + sizeof(object)); }

		fat_ptr _next;
		size_t _size;			// contraint on object size( practical enough )
};

class object_vector
{
public:
    object_vector(unsigned long long nelems);
    bool put(oid_type oid, fat_ptr new_head);
    bool put(oid_type oid, fat_ptr old_head, fat_ptr new_head, bool overwrite);
    void unlink(oid_type oid, void *item);

	inline unsigned long long size() 
	{
		return _global_oid_alloc_offset;
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
        _obj_table.ensure_size(obj_table_size * 2);

        return noffset;
	}

private:
	dynarray 		_obj_table;
    uint64_t _global_oid_alloc_offset;
    percore<uint64_t, false, false> _core_oid_offset;
    percore<uint64_t, false, false> _core_oid_remaining;
};
