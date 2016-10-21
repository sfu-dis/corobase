// -*- mode:c++ -*-
#pragma once

#include "sm-oid.h"
#include "sm-oid-alloc-impl.h"

#include "stub-impl.h"
struct sm_oid_mgr_impl : sm_oid_mgr {
    /* The object array for each file resides in the OID array for
       file 0; allocators go in file 1 (including the file allocator,
       which predictably resides at OID 0). We don't attempt to store
       the file level object array at entry 0 of itself, though.
     */
    static FID const OBJARRAY_FID = 0;
    static FID const ALLOCATOR_FID = 1;
    static FID const METADATA_FID = 2;
    static FID const FIRST_FREE_FID = 3;

    static size_t const MUTEX_COUNT = 256;

    sm_oid_mgr_impl();
    ~sm_oid_mgr_impl();

    FID create_file(bool needs_alloc);
    void destroy_file(FID f);

    sm_allocator *get_allocator(FID f) {
        // TODO: allow allocators to be paged out
        sm_allocator *alloc = oid_get(ALLOCATOR_FID, f);
        THROW_IF(not alloc, illegal_argument,
                 "No allocator for FID %d", f);
        return alloc;
    }
    inline oid_array *get_array(FID f) {
        // TODO: allow allocators to be paged out
        oid_array *oa = files->get(f)->ptr;
        THROW_IF(not oa, illegal_argument,
                 "No such file: %d", f);
        return oa;
    }
    inline void lock_file(FID f) {
        mutexen[f % MUTEX_COUNT].lock();
    }
    inline void unlock_file(FID f) {
        mutexen[f % MUTEX_COUNT].unlock();
    }
    inline fat_ptr *oid_access(FID f, OID o) {
        auto *oa = get_array(f);
        return &oa->get(o)->ptr;
    }
    inline oid_array::entry *oid_entry_access(FID f, OID o) {
        auto *oa = get_array(f);
        return oa->get(o);
    }
    inline bool file_exists(FID f) {
        oid_array *oa = files->get(f)->ptr;
        return oa != NULL;
    }

    void recreate_file(FID f);    // for recovery only
    void recreate_allocator(FID f, OID m);  // for recovery only

    /* And here they all are! */
    oid_array *files;

    /* Plus some mutexen to protect them. We don't need one per
       allocator, but we do want enough that false sharing is
       unlikely.
     */
    os_mutex mutexen[MUTEX_COUNT];
};

/* Make sure things are consistent */
static_assert(oid_array::alloc_size()
              == oid_array::MAX_SIZE,
              "Go fix oid_array::MAX_ENTRIES");
/*
static_assert(oid_array::MAX_ENTRIES
              == sm_allocator::MAX_CAPACITY_MARK,
              "Go fix sm_allocator::MAX_CAPACITY_MARK");

static_assert(sm_allocator::max_alloc_size()
              <= (MAX_ENCODABLE_SIZE << SZCODE_ALIGN_BITS),
              "Need a bigger alignment");
static_assert(oid_array::alloc_size()
              <= (MAX_ENCODABLE_SIZE << SZCODE_ALIGN_BITS),
                  "Need a bigger alignment");
*/

static_assert(sm_oid_mgr::METADATA_FID == sm_oid_mgr_impl::METADATA_FID,
              "Go fix sm_oid_mgr::METADATA_FID");

DEF_IMPL(sm_oid_mgr);
