// -*- mode:c++ -*-
#pragma once

#include "sm-common.h"

#include "sm-heap.h"
#include "sm-log.h"

#include "../tuple.h"

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

    /* Update the contents of the specified OID. The fat_ptr may
       reference memory or disk (or be NULL).
     */
    void oid_put(FID f, OID o, fat_ptr p);

    void oid_put_new(FID f, OID o, fat_ptr p);

    /* Return the overwritten version (could be an in-flight version!) */
    dbtuple *oid_put_update(FID f, OID o, object* new_desc, xid_context *updater_xc);

    dbtuple *oid_get_latest_version(FID f, OID o);
    dbtuple *oid_get_version(FID f, OID o, xid_context *visitor_xc);

    void oid_unlink(FID f, OID o, void *object_payload);

    bool file_exists(FID f);
    void recreate_file(FID f);    // for recovery only
    void recreate_allocator(FID f, OID m);  // for recovery only

    virtual ~sm_oid_mgr() { }
    
protected:
    // Forbid direct instantiation
    sm_oid_mgr() { }
};

extern sm_oid_mgr *oidmgr;
extern std::unordered_map<std::string, FID> fid_map;
