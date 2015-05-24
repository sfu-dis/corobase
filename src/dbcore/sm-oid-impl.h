// -*- mode:c++ -*-
#pragma once

#include "sm-oid.h"
#include "sm-oid-alloc-impl.h"

#include "stub-impl.h"

struct sm_oid_mgr_impl : sm_oid_mgr {

    /* OID arrays and allocators alike always occupy an integer number
       of dynarray pages, to ensure that we don't hit precision issues
       when saving dynarray contents to (and restoring from)
       disk. It's also more than big enough for the objects to reach
       full size
     */
    static size_t const SZCODE_ALIGN_BITS = dynarray::page_bits();
    
    /* An OID is essentially an array of fat_ptr. The only bit of
       magic is that it embeds the dynarray that manages the storage
       it occupies.
     */
    struct oid_array {
        static size_t const MAX_SIZE = sizeof(fat_ptr) << 32;
        static OID const MAX_ENTRIES = (size_t(1) << 32) - sizeof(dynarray)/sizeof(fat_ptr);
        static size_t const ENTRIES_PER_PAGE = sizeof(fat_ptr) << SZCODE_ALIGN_BITS;
        
        /* How much space is required for an array with [n] entries?
         */
        static constexpr
        size_t
        alloc_size(size_t n=MAX_ENTRIES)
        {
            return OFFSETOF(oid_array, _entries[n]);
        }
        
        static
        fat_ptr make();

        static
        void destroy(oid_array *oa);
        
        oid_array(dynarray &&owner);

        // unsafe!
        oid_array()=delete;
        oid_array(oid_array const&)=delete;
        void operator=(oid_array)=delete;

        /* Return the number of entries this OID array currently holds.
         */
        size_t nentries();

        /* Make sure the backing store holds at least [n] entries.
         */
        void ensure_size(size_t n);

        /* Return a pointer to the given OID's slot.

           WARNING: The caller is responsible for handling races in
           case multiple threads try to update the slot concurrently.

           WARNING: this function does not perform bounds
           checking. The caller is responsible to use nentries() and
           ensure_size() as needed.
         */
        fat_ptr *get(OID o) { return &_entries[o]; }

        dynarray _backing_store;
        fat_ptr _entries[];
    };

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

    static
    dynarray make_oid_dynarray() {
        return dynarray(oid_array::alloc_size(), 1024*1024*128);
    }

    sm_oid_mgr_impl();
    ~sm_oid_mgr_impl();

    FID create_file(bool needs_alloc);
    void destroy_file(FID f);

    sm_allocator *get_allocator(FID f);
    oid_array *get_array(FID f);

    void lock_file(FID f);
    void unlock_file(FID f);

    fat_ptr *oid_access(FID f, OID o);

    bool file_exists(FID f);
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
static_assert(sm_oid_mgr_impl::oid_array::alloc_size()
              == sm_oid_mgr_impl::oid_array::MAX_SIZE,
              "Go fix sm_oid_mgr_impl::oid_array::MAX_ENTRIES");
static_assert(sm_oid_mgr_impl::oid_array::MAX_ENTRIES
              == sm_allocator::MAX_CAPACITY_MARK,
              "Go fix sm_allocator::MAX_CAPACITY_MARK");

static_assert(sm_allocator::max_alloc_size()
              <= (MAX_ENCODABLE_SIZE << sm_oid_mgr_impl::SZCODE_ALIGN_BITS),
              "Need a bigger alignment");
static_assert(sm_oid_mgr_impl::oid_array::alloc_size()
              <= (MAX_ENCODABLE_SIZE << sm_oid_mgr_impl::SZCODE_ALIGN_BITS),
                  "Need a bigger alignment");

static_assert(sm_oid_mgr::METADATA_FID == sm_oid_mgr_impl::METADATA_FID,
              "Go fix sm_oid_mgr::METADATA_FID");

DEF_IMPL(sm_oid_mgr);
