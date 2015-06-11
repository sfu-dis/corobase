#pragma once
#include "varstr.h"
#include "dbcore/sm-common.h"
#include "dbcore/sm-alloc.h"

class dbtuple;
class object
{
	public:
        object(size_t size, fat_ptr next) : _size(size), _next(next) {}
        object(size_t size) : _size(size), _next(NULL_PTR) {}

        uint64_t _size;
		fat_ptr _next;
		inline char* payload() { return (char*)((char*)this + sizeof(object)); }
        dbtuple *tuple() { return (dbtuple *)payload(); }
        static object *create_tuple_object(const varstr *tuple_value, bool do_write);
        static fat_ptr create_tuple_object(fat_ptr ptr, fat_ptr nxt);
};

// A place holder for an object that's not stored in memory
// The OID slot could point to an object (in memory) or an
// object_header which means the reader of this version will
// need to call ensure_tuple() to load it from the log.
//
// object_header's next field can point to either an object
// or another object_header. During recovery, it'll point
// to an object_header if fetch_at_recovery==0 (obviously).
// After received some log segment shipped from another
// machine, the receiver will scan the log and install only
// these object_headers. The versions will only be loaded
// if they're needed by the future readers.
struct object_header
{
    object_header(fat_ptr p) : ptr(p), next(NULL_PTR) {}
    object_header(fat_ptr p, fat_ptr n) : ptr(p), next(n) {}
    fat_ptr ptr;    // the object's location in the log
    fat_ptr next;   // next object
};

// DISABLE THE OLD TUPLE VECTOR AND TABLE LOCK IMPLEMENTATIONS
#if 0
#include <cstring>
#include <atomic>
#include <cassert>
#include "macros.h"
#include "dbcore/dynarray.h"
#include <sched.h>
#include <numa.h>
#include <limits>
#include "dbcore/sm-common.h"
#include "core.h"

// each socket requests this many oids a time from global alloc
#define OID_EXT_SIZE 8192

typedef uint32_t oid_type;

struct dynarray;

#ifdef PHANTOM_PROT_TABLE_LOCK
#define TABLE_LOCK_TYPE_BITS    16
#define TABLE_LOCK_MODE_BITS    2
typedef uint16_t table_lock_t;

// 16 bits - 2 bits will allow 2^14 concurrent insert/scan/delete the same table
// most significnat two bits: lock mode - X, S, or SIX (for scanner's insert)
// remaining bits: ref count of inserters/deleters(X)/readers(S)
//
// if SIX is granted, only one inserter is allowed, also the inserter and
// scanner must be the same tx
//
// compatability matrix:
//       X   S  SIX   N
//   X   y   n   n    y
//   S   n   y   n    y
//  SIX  n   n   n    y

// MSBs: 00 - unlocked, 01 - S, 10 - X, 11 - SIX
#define TABLE_LOCK_MODE_SHIFT   (TABLE_LOCK_TYPE_BITS - TABLE_LOCK_MODE_BITS)
#define TABLE_LOCK_MODE_MASK    (table_lock_t(3) << TABLE_LOCK_MODE_SHIFT)
#define TABLE_LOCK_N            table_lock_t(0)
#define TABLE_LOCK_S            (table_lock_t(1) << TABLE_LOCK_MODE_SHIFT)
#define TABLE_LOCK_X            (table_lock_t(2) << TABLE_LOCK_MODE_SHIFT)
#define TABLE_LOCK_SIX          (table_lock_t(3) << TABLE_LOCK_MODE_SHIFT)
#endif

class object_vector
{
public:
    object_vector(unsigned long long nelems);
    bool put(oid_type oid, fat_ptr new_head);
    bool put(oid_type oid, fat_ptr old_head, fat_ptr new_head, bool overwrite);
    void unlink(oid_type oid, void *item);
#ifdef PHANTOM_PROT_TABLE_LOCK
    static bool lock(table_lock_t *lock, table_lock_t rmode);
    static void unlock(table_lock_t *lock);
    static bool upgrade_lock(table_lock_t *lock);

    inline table_lock_t *lock_ptr() { return &_lock; }
#endif

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
        if (obj_table_size >= _obj_table.size())
            _obj_table.ensure_size(obj_table_size * 2);

        return noffset;
	}

private:
#ifdef PHANTOM_PROT_TABLE_LOCK
    table_lock_t _lock;
#endif
	dynarray 		_obj_table;
    uint64_t _global_oid_alloc_offset;
    percore<uint64_t, false, false> _core_oid_offset;
    percore<uint64_t, false, false> _core_oid_remaining;
};
#endif
