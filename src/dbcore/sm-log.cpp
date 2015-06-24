#include "sm-log-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "../benchmarks/ndb_wrapper.h"
#include "../txn_btree.h"
#include <cstring>

using namespace RCU;

sm_log *logmgr = NULL;
bool sm_log::need_recovery = false;
int sm_log::warm_up = sm_log::WU_NONE;

LSN
sm_log::flush()
{
    return get_impl(this)->_lm.flush();
}

void
sm_log::update_chkpt_mark(LSN cstart, LSN cend)
{
    get_impl(this)->_lm._lm.update_chkpt_mark(cstart, cend);
}

void
sm_log::load_object(char *buf, size_t bufsz, fat_ptr ptr, size_t align_bits)
{
    get_impl(this)->_lm._lm.load_object(buf, bufsz, ptr, align_bits);
}

fat_ptr
sm_log::load_ext_pointer(fat_ptr ptr)
{
    return get_impl(this)->_lm._lm.load_ext_pointer(ptr);
}


sm_log *
sm_log::new_log(char const *dname, size_t segsz,
                sm_log_recover_function *rfn, void *rarg,
                size_t bufsz)
{
    need_recovery = false;
    return new sm_log_impl(dname, segsz, rfn, rarg, bufsz);
}

sm_log_scan_mgr *
sm_log::get_scan_mgr()
{
    return get_impl(this)->_lm._lm.scanner;
}

sm_tx_log *
sm_log::new_tx_log()
{
    auto *self = get_impl(this);
    typedef _impl_of<sm_tx_log>::type Impl;
    return new (Impl::alloc_storage()) Impl(self);
}

fat_ptr
sm_log_impl::lsn2ptr(LSN lsn, bool is_ext) {
    return get_impl(this)->_lm._lm.lsn2ptr(lsn, is_ext);
}

LSN
sm_log_impl::ptr2lsn(fat_ptr ptr) {
    return _lm._lm.ptr2lsn(ptr);
}

LSN
sm_log::cur_lsn()
{
    auto *log = &get_impl(this)->_lm;
    auto offset = log->cur_lsn_offset();
    auto *sid = log->_lm.get_offset_segment(offset);

	if (not sid) {
		/* must have raced a new segment opening */
		/*
		while (1) {
			sid = log->_lm._newest_segment();
			if (sid->start_offset >= offset)
				break;
		}
		*/

retry:
		sid = log->_lm._newest_segment();
		ASSERT(sid);
		if (offset < sid->start_offset)
			offset = sid->start_offset;
		else if (sid->end_offset <= offset) {
			    goto retry; 
		}
	}
    return sid->make_lsn(offset);
}

LSN
sm_log::durable_lsn()
{
    auto *log = &get_impl(this)->_lm;
    auto offset = log->dur_lsn_offset();
    auto *sid = log->_lm.get_offset_segment(offset);
    ASSERT(sid);
    return sid->make_lsn(offset);
}

void
sm_log::wait_for_durable_lsn(LSN dlsn)
{
    auto *self = get_impl(this);
    self->_lm.wait_for_durable(dlsn.offset());
}

/* The main recovery function.
 *
 * Without checkpointing, recovery starts with an empty, new oidmgr, and then
 * scans the log to insert/update **versions**.
 *
 * FIDs/OIDs met during the above scan are blindly inserted to corresponding
 * object arrays, without touching the allocator (thru ensure_size and
 * oid_getput interfaces).
 *
 * The above scan also figures out the <FID, table name> pairs for all tables to
 * rebuild their indexes. The max OID is also gathered for each FID, including
 * the internal files (OBJARRAY_FID etc.) to recover allocator status.
 *
 * After the above scan, an allocator is made for each FID with its hiwater_mark
 * and capacity_mark updated to the FID's max OID+64. This allows the oidmgr to
 * allocate new OIDs > max OID.
 *
 * Note that alloc_oid hasn't been used so far, and the caching structures
 * should all be empty.
 *
 * A second scan then starts (again from the beginning of the log) to rebuild
 * indexes. Note that the previous step on recreate allocators must be done
 * before index rebuild, because each index's tree data structure will use
 * create_file and alloc_oid to store their nodes. So we need to make the OID
 * allocators (esp. the internal files) ready to avoid allocating already-
 * allocated OIDs.
 */
void
sm_log::recover(void *arg, sm_log_scan_mgr *scanner,
                LSN chkpt_begin, LSN chkpt_end, char const *dname)
{
    RCU::rcu_register();
    RCU::rcu_enter();

    // One hiwater_mark/capacity_mark per FID
    himark_map_t himarks;
    std::vector<std::future<std::pair<FID, OID> > > futures;
    auto *scan = scanner->new_log_scan(chkpt_begin, warm_up == WU_EAGER);

    FID max_fid = 0;
    for (auto &fm : fid_map) {
        FID fid = fm.second.first;
        if (fid > max_fid)
            max_fid = fid;

        // Scan the rest of the log
        futures.emplace_back(std::async(std::launch::async, redo_file,
                                        scanner, chkpt_begin, fid));
    }

    // Look for new table creations after the chkpt
    // Spawn one redo thread per new table found
    for (; scan->valid(); scan->next()) {
        if (scan->type() != sm_log_scan_mgr::LOG_FID)
            continue;
        recover_fid(scan);
        FID fid = scan->fid();
        if (max_fid < fid)
            max_fid = fid;
        futures.emplace_back(std::async(std::launch::async, redo_file,
                                        scanner, chkpt_begin, fid));
    }

    // Now recover allocator status
    // Note: must do this before opening indexes, because each index itself
    // uses an OID array (for the tree itself), which is allocated thru
    // alloc_oid. So we need to fix the watermarks here to make sure trees
    // don't get some already allocated FID.
    for (auto & f : futures) {
        auto h = f.get();
        if (h.second)
            oidmgr->recreate_allocator(h.first, h.second);
    }

    // Fix internal files' marks too
    oidmgr->recreate_allocator(sm_oid_mgr_impl::OBJARRAY_FID, max_fid);
    oidmgr->recreate_allocator(sm_oid_mgr_impl::ALLOCATOR_FID, max_fid);
    //oidmgr->recreate_allocator(sm_oid_mgr_impl::METADATA_FID, max_fid);

    // So by now we will have a fully working database, once index is rebuilt.
    // WARNING: DO NOT TAKE CHKPT UNTIL WE REPLAYED ALL INDEXES!
    // Otherwise we migth lose some FIDs/OIDs created before the chkpt.
    //
    // For easier measurement (like "how long does it take to bring the
    // system back to fully memory-resident after recovery), we spawn the
    // warm-up thread after rebuilding indexes as well.
}

std::pair<FID, OID>
sm_log::redo_file(sm_log_scan_mgr *scanner, LSN chkpt_begin, FID fid)
{
    ASSERT(oidmgr->file_exists(fid));
	RCU::rcu_register();
    RCU::rcu_enter();
    OID himark = 0;
    uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
    auto *scan = scanner->new_log_scan(chkpt_begin, warm_up == WU_EAGER);
    for (; scan->valid(); scan->next()) {
        auto f = scan->fid();
        if (f != fid)
            continue;
        auto o = scan->oid();
        if (himark < o)
            himark = o;

        switch (scan->type()) {
        case sm_log_scan_mgr::LOG_UPDATE:
        case sm_log_scan_mgr::LOG_RELOCATE:
            ucount++;
            recover_update(scan);
            size += scan->payload_size();
            break;
        case sm_log_scan_mgr::LOG_DELETE:
            dcount++;
            recover_update(scan, true);
            break;
        case sm_log_scan_mgr::LOG_INSERT_INDEX:
            iicount++;
            // ignore for now, we redo this in open_index
            break;
        case sm_log_scan_mgr::LOG_INSERT:
            icount++;
            recover_insert(scan);
            size += scan->payload_size();
            break;
        case sm_log_scan_mgr::LOG_CHKPT:
            break;
        case sm_log_scan_mgr::LOG_FID:
            // The main recover function should already did this
            ASSERT(oidmgr->file_exists(fid));
            break;
        default:
            DIE("unreachable");
        }
    }
    ASSERT(icount == iicount);
    printf("[Recovery.log] FID %d - inserts/updates/deletes/size: %lu/%lu/%lu/%lu\n",
           fid, icount, ucount, dcount, size);

    delete scan;
    RCU::rcu_exit();
    RCU::rcu_deregister();
    return std::make_pair(fid, himark);
}

// The version-loading mechanism will only dig out the latest
// version as a result.
fat_ptr
sm_log::recover_prepare_version(sm_log_scan_mgr::record_scan *logrec,
                                fat_ptr next)
{
    // Note: payload_size() includes the whole varstr
    // See do_tree_put's log_update call.
    size_t sz = sizeof(object);
    if (warm_up == WU_EAGER) {
        sz += (sizeof(dbtuple) + logrec->payload_size());
        sz = align_up(sz);
    }

    object *obj = NULL;
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
    obj = t.op->get(sz);
    if (not obj)
#endif
        obj = new (MM::allocate(sz)) object(logrec->payload_ptr(), next);

    if (warm_up != WU_EAGER)
        return fat_ptr::make(obj, INVALID_SIZE_CODE, fat_ptr::ASI_LOG_FLAG);

    // Load tuple varstr from logrec
    dbtuple* tuple = (dbtuple *)obj->payload();
    tuple = dbtuple::init((char*)tuple, sz);
    logrec->load_object((char *)tuple->get_value_start(), sz);

    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr),
            tuple->size);

    ASSERT(obj->_next == next);
    obj->tuple()->clsn = get_impl(logrec)->start_lsn.to_log_ptr();
    ASSERT(logrec->payload_lsn().offset() == logrec->payload_ptr().offset());
    ASSERT(obj->tuple()->clsn.asi_type() == fat_ptr::ASI_LOG);
    return fat_ptr::make(obj, INVALID_SIZE_CODE);
}

void
sm_log::recover_insert(sm_log_scan_mgr::record_scan *logrec)
{
    FID f = logrec->fid();
    OID o = logrec->oid();
    fat_ptr ptr = recover_prepare_version(logrec, NULL_PTR);
    ASSERT(oidmgr->file_exists(f));
    oid_array *oa = get_impl(oidmgr)->get_array(f);
    oa->ensure_size(oa->alloc_size(o));
    oidmgr->oid_put_new(f, o, ptr);
    ASSERT(ptr.offset() and oidmgr->oid_get(f, o).offset() == ptr.offset());
    //printf("[Recovery] insert: FID=%d OID=%d\n", f, o);
}

void
sm_log::recover_update(sm_log_scan_mgr::record_scan *logrec, bool is_delete)
{
    FID f = logrec->fid();
    OID o = logrec->oid();
    ASSERT(oidmgr->file_exists(f));
    auto head_ptr = oidmgr->oid_get(f, o);
    fat_ptr ptr = NULL_PTR;
    if (not is_delete)
        ptr = recover_prepare_version(logrec, head_ptr);
    oidmgr->oid_put(f, o, ptr);
    ASSERT(oidmgr->oid_get(f, o).offset() == ptr.offset());
    // this has to go if on-demand loading is enabled
    //ASSERT(((object *)oidmgr->oid_get(f, o).offset())->_next == head_ptr);
    //printf("[Recovery] update: FID=%d OID=%d\n", f, o);
}

void
sm_log::recover_fid(sm_log_scan_mgr::record_scan *logrec)
{
    FID f = logrec->fid();
    auto sz = logrec->payload_size();
    char *buf = (char *)malloc(sz);
    logrec->load_object(buf, sz);
    std::string name(buf);
    fid_map.emplace(name, std::make_pair(f, (ndb_ordered_index *)NULL));
    ASSERT(not oidmgr->file_exists(f));
    oidmgr->recreate_file(f);
    printf("[Recovery: log] FID(%s) = %d\n", buf, f);
    free(buf);
}

std::pair<std::string, uint64_t>
sm_log::rebuild_index(FID fid, ndb_ordered_index *index)
{
    uint64_t count = 0;
    RCU::rcu_register();
    RCU::rcu_enter();
    //LSN chkpt_begin = get_impl(logmgr)->_lm._lm.get_chkpt_start();
    //auto *scan = logmgr->get_scan_mgr()->new_log_scan(chkpt_begin, true);
    // This has to start from the beginning of the log for now, because we
    // don't checkpoint the key-oid pair.
    auto *scan = logmgr->get_scan_mgr()->new_log_scan(LSN{0x1ff}, true);
    for (; scan->valid(); scan->next()) {
        if (scan->type() != sm_log_scan_mgr::LOG_INSERT_INDEX or scan->fid() != fid)
            continue;
        // Below ASSERT has to go as the object might be already deleted
        //ASSERT(oidmgr->oid_get(fid, scan->oid()).offset());
        auto sz = scan->payload_size();
        char *buf = (char *)malloc(sz);
        scan->load_object(buf, sz);

        // Extract the real key length (don't use varstr.data()!)
        size_t len = ((varstr *)buf)->size();
        ASSERT(align_up(len + sizeof(varstr)) == sz);

        // Construct the varkey (skip the varstr struct then it's data)
        varkey key((uint8_t *)((char *)buf + sizeof(varstr)), len);

        //printf("key %s %s\n", (char *)key.data(), buf);
        ALWAYS_ASSERT(index->btr.underlying_btree.insert_if_absent(key, fid, scan->oid(), NULL));
        count++;
        free((void *)buf);
    }
    delete scan;
    RCU::rcu_exit();
    RCU::rcu_deregister();
    return make_pair(index->name, count);
}

void
sm_log::recover_index()
{
    std::vector<std::future<std::pair<std::string, uint64_t> > > futures;
    for (auto &e : fid_map) {
        ndb_ordered_index *index = e.second.second;
        FID fid = e.second.first;
        futures.emplace_back(std::async(std::launch::async, rebuild_index, fid, index));
    }

    for (auto &f : futures) {
        std::pair<std::string, uint64_t> r = f.get();
        printf("[Recovery] %s index inserts: %lu\n", r.first.c_str(), r.second);
    }

    // XXX (tzwang): this doesn't belong here, but recover_index is invoked
    // by the benchmark code after opened indexes. Putting this in the
    // benchmark code looks even uglier...
    if (warm_up == WU_LAZY)
        oidmgr->start_warm_up();
}
