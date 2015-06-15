// -*- mode:c++ -*-
#pragma once

#include "sm-common.h"

#include "sm-heap.h"
#include "sm-log.h"

#include "dynarray.h"

#include "../tuple.h"

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
    dynarray make_oid_dynarray() {
        return dynarray(oid_array::alloc_size(), 1024*1024*128);
    }

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

struct sm_oid_mgr {
    using log_tx_scan = sm_log_scan_mgr::record_scan;
    
    /* Metadata for any allocated file can be stored in this file at
       the OID that matches its FID.
     */
    static FID const METADATA_FID = 2;

    /* Create a new OID manager, recovering its state from
       [chkpt_tx_scan].
       
       NOTE: the scan must be positioned at the first record of the
       checkpoint transaction (or the location where the record would
       be if the checkpoint is empty). When this function returns, the
       scan will be positioned at whatever follows the OID checkpoint
       (or invalid, if there are no more records).
     */
    static
    sm_oid_mgr *create(sm_heap_mgr *hm, log_tx_scan *chkpt_tx_scan);

    /* Record a snapshot of the OID manager's state as part of a
       checkpoint. The data will be durable by the time this function
       returns, but will only be reachable if the checkpoint
       transaction commits and its location is properly recorded.
     */
    void log_chkpt(sm_heap_mgr *hm, sm_tx_log *tx);

    /* Create a new file and return its FID. If [needs_alloc]=true,
       the new file will be managed by an allocator and its FID can be
       passed to alloc_oid(); otherwise, the file is either unmanaged
       or a slave to some other file.
     */
    FID create_file(bool needs_alloc=true);

    /* Destroy file [f] and remove its contents. Its allocator, if
       any, will also be removed.

       The caller is responsible to destroy any "slave" files that
       depended on this one, and to remove the file's metadata entry
       (if any).

       WARNING: the caller is responsible to ensure that this file is
       no longer in use by other threads.
     */
    void destroy_file(FID f);

    /* Allocate a new OID in file [f] and return it.

       Throw fid_is_full if no more OIDs are available for allocation
       in this file.
       
       WARNING: This is a volatile operation. In the event of a crash,
       the OID will be freed unless recorded in some way (usually an
       insert log record).

    */
    OID alloc_oid(FID f);

    /* Free an OID and return its contents (which should be disposed
       of by the caller as appropriate).

       WARNING: This is a volatile operation. In the event of a crash,
       the OID will be freed unless recorded in some way (usually a
       delete log record).
    */
    fat_ptr free_oid(FID f, OID o);

    /* Retrieve the raw contents of the specified OID. The returned
       fat_ptr may reference memory or disk.
     */
    fat_ptr oid_get(FID f, OID o);
    fat_ptr *oid_get_ptr(FID f, OID o);

    fat_ptr oid_get(oid_array *oa, OID o);
    fat_ptr *oid_get_ptr(oid_array *oa, OID o);

    /* Update the contents of the specified OID. The fat_ptr may
       reference memory or disk (or be NULL).
     */
    void oid_put(FID f, OID o, fat_ptr p);
    void oid_put(oid_array *oa, OID o, fat_ptr p);

    void oid_put_new(FID f, OID o, fat_ptr p);
    void oid_put_new(oid_array *oa, OID o, fat_ptr p);

    /* Return the overwritten version (could be an in-flight version!) */
    dbtuple *oid_put_update(FID f, OID o, const varstr* value, xid_context *updater_xc, dbtuple *&new_tuple);
    dbtuple *oid_put_update(oid_array *oa, OID o,
                            const varstr *value, xid_context *updater_xc, dbtuple *&new_tuple);

    dbtuple *oid_get_latest_version(FID f, OID o);
    dbtuple *oid_get_latest_version(oid_array *oa, OID o);

    dbtuple *oid_get_version(FID f, OID o, xid_context *visitor_xc);
    dbtuple *oid_get_version(oid_array *oa, OID o, xid_context *visitor_xc);

    fat_ptr *ensure_tuple(FID f, OID o);
    fat_ptr *ensure_tuple(oid_array *oa, OID o);
    fat_ptr ensure_tuple(fat_ptr *ptr);

    void oid_unlink(FID f, OID o, void *object_payload);
    void oid_unlink(oid_array *oa, OID o, void *object_payload);

    bool file_exists(FID f);
    void recreate_file(FID f);    // for recovery only
    void recreate_allocator(FID f, OID m);  // for recovery only
    oid_array *get_array(FID f);

    virtual ~sm_oid_mgr() { }
    
protected:
    // Forbid direct instantiation
    sm_oid_mgr() { }
};

extern sm_oid_mgr *oidmgr;
extern std::unordered_map<std::string, std::pair<FID, ndb_ordered_index *> > fid_map;
