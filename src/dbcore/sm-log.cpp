#include "sm-log-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "../benchmarks/ndb_wrapper.h"
#include "../txn_btree.h"
#include <cstring>

using namespace RCU;

sm_log *logmgr = NULL;
bool sm_log::need_recovery = false;
int sm_log::fetch_at_recovery = 0;
uint sm_log::nredo_parts = 6;

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
sm_log::recover(void *arg, sm_log_scan_mgr *scanner, LSN chkpt_begin, LSN chkpt_end)
{
    // The hard case: no chkpt, have to do it the hard way
    oidmgr = sm_oid_mgr::create(NULL, NULL);

    if (not sm_log::need_recovery)
        return;

    std::future<himark_map_t> futures[nredo_parts];
    for (uint i = 0; i < nredo_parts; i++)
        futures[i] = std::async(std::launch::async, redo, scanner, chkpt_begin, i);

    // Now recover allocator status
    // Note: must do this before opening indexes, because each index itself
    // uses an OID array (for the tree itself), which is allocated thru
    // alloc_oid. So we need to fix the watermarks here to make sure trees
    // don't get some already allocated FID.
    FID max_fid = 0;
    for (auto & f : futures) {
        auto h = f.get();
        for (auto &m : h) {
            oidmgr->recreate_allocator(m.first, m.second);
            if (m.first > max_fid)
                max_fid = m.first;
        }
    }

    // Fix internal files' marks too
    oidmgr->recreate_allocator(sm_oid_mgr_impl::OBJARRAY_FID, max_fid);
    oidmgr->recreate_allocator(sm_oid_mgr_impl::ALLOCATOR_FID, max_fid);
    //oidmgr->recreate_allocator(sm_oid_mgr_impl::METADATA_FID, max_fid);

    // So by now we will have a fully working database, once index is rebuilt.
    // WARNING: DO NOT TAKE CHKPT UNTIL WE REPLAYED ALL INDEXES!
    // Otherwise we migth lose some FIDs/OIDs created before the chkpt.
}

sm_log::himark_map_t
sm_log::redo(sm_log_scan_mgr *scanner, LSN chkpt_begin, uint mod_part)
{
	RCU::rcu_register();
    RCU::rcu_enter();

    // One hiwater_mark/capacity_mark per FID
    himark_map_t himarks;

    uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
    auto *scan = scanner->new_log_scan(chkpt_begin);
    for (; scan->valid(); scan->next()) {
        size += scan->payload_size();
        auto f = scan->fid();
        if (f % nredo_parts != mod_part)
            continue;
        auto o = scan->oid();
        if (himarks[f] < o)
            himarks[f] = o;

        switch (scan->type()) {
        case sm_log_scan_mgr::LOG_UPDATE:
        case sm_log_scan_mgr::LOG_RELOCATE:
            ucount++;
            recover_update(scan);
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
            break;
        case sm_log_scan_mgr::LOG_CHKPT:
            break;
        case sm_log_scan_mgr::LOG_FID:
            recover_fid(scan);
            break;
        default:
            DIE("unreachable");
        }
    }
    ASSERT(icount == iicount);
    printf("[Recovery t%d] inserts/updates/deletes/size: %lu/%lu/%lu/%lu\n",
           mod_part, icount, ucount, dcount, size);

    delete scan;
    RCU::rcu_exit();
    RCU::rcu_deregister();
    return himarks;
}

// The version-loading mechanism will only dig out the latest
// version as a result.
fat_ptr
sm_log::recover_prepare_version(sm_log_scan_mgr::record_scan *logrec,
                                object *next)
{
    // The tx will need to call sm_log::load_object() to load the object
    // when accessing the version. The caller should write this return
    // value to the corresponding OID array slot.
    if (not sm_log::fetch_at_recovery)
        return logrec->payload_ptr();

    // Note: payload_size() includes the whole varstr
    // See do_tree_put's log_update call.
    const size_t sz = logrec->payload_size();
    ASSERT(sz == decode_size_aligned(logrec->payload_ptr().size_code()));
    size_t alloc_sz = sizeof(dbtuple) + sizeof(object) + align_up(sz);

    object *obj = NULL;
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
    obj = t.op->get(alloc_sz);
    if (not obj)
#endif
        obj = new (MM::allocate(alloc_sz)) object(alloc_sz);

    // Load tuple varstr from logrec
    dbtuple* tuple = (dbtuple *)obj->payload();
    tuple = dbtuple::init((char*)tuple, sz);
    logrec->load_object((char *)tuple->get_value_start(), sz);

    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr),
            tuple->size);

    obj->_next = fat_ptr::make(next, INVALID_SIZE_CODE);
    obj->tuple()->clsn = logrec->payload_lsn().to_log_ptr();
    ASSERT(logrec->payload_lsn().offset() == logrec->payload_ptr().offset());
    ASSERT(obj->tuple()->clsn.asi_type() == fat_ptr::ASI_LOG);
    return fat_ptr::make(obj, INVALID_SIZE_CODE);
}

void
sm_log::recover_insert(sm_log_scan_mgr::record_scan *logrec)
{
    FID f = logrec->fid();
    OID o = logrec->oid();
    fat_ptr ptr = recover_prepare_version(logrec, NULL);
    ASSERT(oidmgr->file_exists(f));
    oid_array *oa = get_impl(oidmgr)->get_array(f);
    oa->ensure_size(oa->alloc_size(o));
    if (fetch_at_recovery) {
        oidmgr->oid_put_new(f, o, ptr);
        ASSERT(ptr.offset() and oidmgr->oid_get(f, o).offset() == ptr.offset());
    }
    else {
        oidmgr->oid_put_header(f, o, ptr);
    }
    //printf("[Recovery] insert: FID=%d OID=%d\n", f, o);
}

void
sm_log::recover_update(sm_log_scan_mgr::record_scan *logrec, bool is_delete)
{
    FID f = logrec->fid();
    OID o = logrec->oid();
    ASSERT(oidmgr->file_exists(f));
    auto head_ptr = oidmgr->oid_get(f, o);
    if (is_delete)
        oidmgr->oid_put(f, o, NULL_PTR);
    else {
        fat_ptr ptr = recover_prepare_version(logrec, (object *)head_ptr.offset());
        if (fetch_at_recovery) {
            oidmgr->oid_put(f, o, ptr);
            ASSERT(ptr.offset() and oidmgr->oid_get(f, o).offset() == ptr.offset());
            // this has to go if on-demand loading is enabled
            //ASSERT(((object *)oidmgr->oid_get(f, o).offset())->_next == head_ptr);
        }
        else {
            oidmgr->oid_put_header(f, o, ptr);
        }
    }
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
    printf("[Recovery] FID(%s) = %d\n", buf, f);
    fid_map.emplace(name, f);
    ASSERT(not oidmgr->file_exists(f));
    oidmgr->recreate_file(f);
    free(buf);
}

void
sm_log::recover_index(FID fid, ndb_ordered_index *index)
{
    ASSERT(fid_map[index->name] == fid);
    uint64_t count = 0;
    LSN chkpt_begin = get_impl(logmgr)->_lm._lm.get_chkpt_start();
    auto *scan = logmgr->get_scan_mgr()->new_log_scan(chkpt_begin);
    DEFER(delete scan);
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
    printf("[Recovery] %s index inserts: %lu\n", index->name.c_str(), count);
}
