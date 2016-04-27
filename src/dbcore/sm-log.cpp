#include "sm-log-impl.h"
#include "sm-log-offset.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-thread.h"
#include "../benchmarks/ndb_wrapper.h"
#include "../txn_btree.h"
#include <cstring>

using namespace RCU;

sm_log *logmgr = NULL;
bool sm_log::need_recovery = false;

uint64_t
sm_log::persist_log_buffer()
{
    ALWAYS_ASSERT(sysconf::nvram_log_buffer);
    /** dummy. FIXME(tzwang) **/
    return get_impl(this)->_lm.smallest_tls_lsn_offset();
}

void
sm_log::set_tls_lsn_offset(uint64_t offset)
{
    get_impl(this)->_lm.set_tls_lsn_offset(offset);
}

uint64_t
sm_log::get_tls_lsn_offset()
{
    return get_impl(this)->_lm.get_tls_lsn_offset();
}

window_buffer&
sm_log::get_logbuf()
{
    return get_impl(this)->_lm._logbuf;
}

void
sm_log::redo_log(LSN chkpt_start_lsn, LSN chkpt_end_lsn)
{
    get_impl(this)->_lm._lm.redo_log(chkpt_start_lsn, chkpt_end_lsn);
}

segment_id*
sm_log::assign_segment(uint64_t lsn_begin, uint64_t lsn_end)
{
    return get_impl(this)->_lm._lm.assign_segment(lsn_begin, lsn_end).sid;
}

segment_id *
sm_log::flush_log_buffer(window_buffer &logbuf, uint64_t new_dlsn_offset, bool update_dmark)
{
    return get_impl(this)->_lm.flush_log_buffer(logbuf, new_dlsn_offset, update_dmark);
}

void sm_log::enqueue_committed_xct(uint32_t worker_id, uint64_t start_time)
{
    get_impl(this)->_lm.enqueue_committed_xct(worker_id, start_time);
}

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
sm_log::new_log(sm_log_recover_function *rfn, void *rarg)
{
    need_recovery = false;
    if (sysconf::null_log_device) {
      dirent_iterator iter(sysconf::log_dir.c_str());
      for (char const *fname : iter) {
        if (strcmp(fname, ".") and strcmp(fname, ".."))
          os_unlinkat(iter.dup(), fname);
      }
    }
    ALWAYS_ASSERT(sysconf::log_segment_mb);
    ALWAYS_ASSERT(sysconf::log_buffer_mb);
    return new sm_log_impl(rfn, rarg);
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
sm_log::durable_flushed_lsn()
{
    auto *log = &get_impl(this)->_lm;
    auto offset = log->dur_flushed_lsn_offset();
    auto *sid = log->_lm.get_offset_segment(offset);
    ASSERT(sid);
    return sid->make_lsn(offset);
}

void
sm_log::wait_for_durable_flushed_lsn_offset(uint64_t offset)
{
    auto *self = get_impl(this);
    self->_lm.wait_for_durable(offset);
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
                LSN chkpt_begin, LSN chkpt_end)
{
    RCU::rcu_enter();

    // Look for new table creations after the chkpt
    // Use one redo thread per new table found
    // XXX(tzwang): no support for dynamically created tables for now
    // TODO(tzwang): figure out how this interacts with chkpt

    // One hiwater_mark/capacity_mark per FID
    FID max_fid = 0;
    static std::vector<struct redo_runner> redoers;
    if (redoers.size() == 0) {
        auto *scan = scanner->new_log_scan(chkpt_begin, sysconf::eager_warm_up());
        for (; scan->valid(); scan->next()) {
            if (scan->type() != sm_log_scan_mgr::LOG_FID)
                continue;
            FID fid = scan->fid();
            max_fid = std::max(fid, max_fid);
            redoers.emplace_back(scanner, chkpt_begin, fid, recover_fid(scan));
        }
        delete scan;
    }

    if (redoers.size() == 0) {
        for (auto &fm : fid_map) {
            FID fid = fm.second.first;
            ASSERT(fid);
            max_fid = std::max(fid, max_fid);
            redoers.emplace_back(scanner, chkpt_begin, fid, fm.second.second);
        }
    }

    // Fix internal files' marks
    oidmgr->recreate_allocator(sm_oid_mgr_impl::OBJARRAY_FID, max_fid);
    oidmgr->recreate_allocator(sm_oid_mgr_impl::ALLOCATOR_FID, max_fid);
    //oidmgr->recreate_allocator(sm_oid_mgr_impl::METADATA_FID, max_fid);

    process:
    for (auto &r : redoers) {
        // Scan the rest of the log
        r.chkpt_begin = chkpt_begin;
        r.scanner = scanner;
        if (not r.done and not r.is_impersonated() and r.try_impersonate()) {
            r.start();
        }
    }

    for (auto &r : redoers) {
        if (r.is_impersonated()) {
            r.join();
            goto process;
        }
    }

    // WARNING: DO NOT TAKE CHKPT UNTIL WE REPLAYED ALL INDEXES!
    // Otherwise we migth lose some FIDs/OIDs created before the chkpt.
    //
    // For easier measurement (like "how long does it take to bring the
    // system back to fully memory-resident after recovery), we spawn the
    // warm-up thread after rebuilding indexes as well.
    if (sysconf::lazy_warm_up())
        oidmgr->start_warm_up();
}

void
sm_log::redo_runner::my_work(char *)
{
    auto himark = redo_file(scanner, chkpt_begin, fid);

    // Now recover allocator status
    // Note: must do this before opening indexes, because each index itself
    // uses an OID array (for the tree itself), which is allocated thru
    // alloc_oid. So we need to fix the watermarks here to make sure trees
    // don't get some already allocated FID.
    if (himark)
        oidmgr->recreate_allocator(fid, himark);

    rebuild_index(scanner, fid, fid_index);
    done = true;
    __sync_synchronize();
}

FID
sm_log::redo_file(sm_log_scan_mgr *scanner, LSN chkpt_begin, FID fid)
{
    ASSERT(oidmgr->file_exists(fid));
    RCU::rcu_enter();
    OID himark = 0;
    uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
    auto *scan = scanner->new_log_scan(chkpt_begin, sysconf::eager_warm_up());
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
    return himark;
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
    if (sysconf::eager_warm_up()) {
        sz += (sizeof(dbtuple) + logrec->payload_size());
        sz = align_up(sz);
    }

    object *obj = new (MM::allocate(sz, 0)) object(logrec->payload_ptr(), next, 0);

    if (not sysconf::eager_warm_up())
        return fat_ptr::make(obj, INVALID_SIZE_CODE, fat_ptr::ASI_LOG_FLAG);

    // Load tuple varstr from logrec
    dbtuple* tuple = (dbtuple *)obj->payload();
    new (tuple) dbtuple(sz);
    logrec->load_object((char *)tuple->get_value_start(), sz);

    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr),
            tuple->size);

    ASSERT(obj->_next == next);
    obj->_clsn = get_impl(logrec)->start_lsn.to_log_ptr();
    ASSERT(logrec->payload_lsn().offset() == logrec->payload_ptr().offset());
    ASSERT(obj->_clsn.asi_type() == fat_ptr::ASI_LOG);
    return fat_ptr::make(obj, encode_size_aligned(sz));
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

ndb_ordered_index*
sm_log::recover_fid(sm_log_scan_mgr::record_scan *logrec)
{
    FID f = logrec->fid();
    auto sz = logrec->payload_size();
    char *buf = (char *)malloc(sz);
    logrec->load_object(buf, sz);
    std::string name(buf);
    // XXX(tzwang): no support for dynamically created tables for now
    ASSERT(fid_map.find(name) != fid_map.end());
    ASSERT(fid_map[name].second);
    fid_map[name].first = f;  // fill in the fid
    ASSERT(not oidmgr->file_exists(f));
    oidmgr->recreate_file(f);
    fid_map[name].second->set_btr_fid(f);
    printf("[Recovery: log] FID(%s) = %d\n", buf, f);
    free(buf);
    return fid_map[name].second;
}

std::pair<std::string, uint64_t>
sm_log::rebuild_index(sm_log_scan_mgr *scanner, FID fid, ndb_ordered_index *index)
{
    uint64_t count = 0;
    RCU::rcu_enter();
    // This has to start from the beginning of the log for now, because we
    // don't checkpoint the key-oid pair.
    auto *scan = scanner->new_log_scan(LSN{0x1ff}, true);
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
        ALWAYS_ASSERT(index->btr.underlying_btree.insert_if_absent(key, scan->oid(), NULL));
        count++;
        free((void *)buf);
    }
    delete scan;
    RCU::rcu_exit();
    return make_pair(index->name, count);
}
