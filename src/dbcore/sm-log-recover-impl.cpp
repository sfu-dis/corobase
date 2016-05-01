#include "../benchmarks/ndb_wrapper.h"
#include "../txn_btree.h"
#include "../util.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-oid-alloc-impl.h"

#define SEPARATE_INDEX_REBUILD 0

std::unordered_map<FID, ndb_ordered_index *> reverse_fid_map;

// The version-loading mechanism will only dig out the latest version as a result.
fat_ptr
sm_log_recover_impl::recover_prepare_version(sm_log_scan_mgr::record_scan *logrec, fat_ptr next) {
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
  //obj->_clsn = get_impl(logrec)->start_lsn.to_log_ptr();
  obj->_clsn = logrec->payload_lsn().to_log_ptr();
  ASSERT(logrec->payload_lsn().offset() == logrec->payload_ptr().offset());
  ASSERT(obj->_clsn.asi_type() == fat_ptr::ASI_LOG);
  return fat_ptr::make(obj, encode_size_aligned(sz));
}

void
sm_log_recover_impl::recover_insert(sm_log_scan_mgr::record_scan *logrec) {
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
sm_log_recover_impl::recover_index_insert(sm_log_scan_mgr::record_scan *logrec) {
  ASSERT(SEPARATE_INDEX_REBUILD == 0);
  recover_index_insert(logrec, reverse_fid_map[logrec->fid()]);
}

void
sm_log_recover_impl::recover_index_insert(sm_log_scan_mgr::record_scan *logrec, ndb_ordered_index *index) {
  auto sz = logrec->payload_size();
  static __thread char *buf;
  static __thread uint64_t buf_size;
  if (unlikely(not buf)) {
    buf = (char *)MM::allocate(sz, 0);
    buf_size = align_up(sz);
  } else if (unlikely(buf_size < sz)) {
    MM::deallocate(fat_ptr::make(buf, encode_size_aligned(buf_size)));
    buf = (char *)MM::allocate(sz, 0);
    buf_size = sz;
  }
  logrec->load_object(buf, sz);

  // Extract the real key length (don't use varstr.data()!)
  size_t len = ((varstr *)buf)->size();
  ASSERT(align_up(len + sizeof(varstr)) == sz);

  // Construct the varkey (skip the varstr struct then it's data)
  varkey key((uint8_t *)((char *)buf + sizeof(varstr)), len);

  //printf("key %s %s\n", (char *)key.data(), buf);
  ALWAYS_ASSERT(index->btr.underlying_btree.insert_if_absent(key, logrec->oid(), NULL));
}

void
sm_log_recover_impl::recover_update(sm_log_scan_mgr::record_scan *logrec, bool is_delete) {
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
sm_log_recover_impl::recover_fid(sm_log_scan_mgr::record_scan *logrec) {
  static char name_buf[256];  // No CC for this, don't run multiple recover_fid() threads
  FID f = logrec->fid();
  auto sz = logrec->payload_size();
  ALWAYS_ASSERT(sz <= 256);  // 256 should be enough, revisit later if not
  logrec->load_object(name_buf, sz);
  std::string name(name_buf);
  // XXX(tzwang): no support for dynamically created tables for now
  ASSERT(fid_map.find(name) != fid_map.end());
  ASSERT(fid_map[name].second);
  fid_map[name].first = f;  // fill in the fid
  ASSERT(not oidmgr->file_exists(f));
  oidmgr->recreate_file(f);
  fid_map[name].second->set_btr_fid(f);
  reverse_fid_map.emplace(f, fid_map[name].second);
  printf("[Recovery: log] FID(%s) = %d\n", name_buf, f);
  return fid_map[name].second;
}

void
sm_log_recover_impl::rebuild_index(sm_log_scan_mgr *scanner, FID fid, ndb_ordered_index *index) {
  ALWAYS_ASSERT(SEPARATE_INDEX_REBUILD);
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
    recover_index_insert(scan, index);
    count++;
  }
  delete scan;
  RCU::rcu_exit();
}

/* The main recovery function of parallel_file_replay.
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
 * The SEPARATE_INDEX_REBUILD macro defines whether we insert into indexs while
 * redoing LOG_INSERT. If so, there is no need for another pass of scanning the
 * log to rebuild indexes (rebuild_index()). Otherwise a second scan then starts
 * (again from the beginning of the log) to rebuild indexes.
 */
void
parallel_file_replay::operator()(void *arg, sm_log_scan_mgr *s, LSN cb, LSN ce) {
  scanner = s;
  chkpt_begin = cb;

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
      recover_fid(scan);
    }
    delete scan;
  }

  if (redoers.size() == 0) {
    for (auto &fm : fid_map) {
      FID fid = fm.second.first;
      ASSERT(fid);
      max_fid = std::max(fid, max_fid);
      redoers.emplace_back(this, fid, fm.second.second);
    }
  }

  // Fix internal files' marks
  oidmgr->recreate_allocator(sm_oid_mgr_impl::OBJARRAY_FID, max_fid);
  oidmgr->recreate_allocator(sm_oid_mgr_impl::ALLOCATOR_FID, max_fid);
  //oidmgr->recreate_allocator(sm_oid_mgr_impl::METADATA_FID, max_fid);

  uint32_t done = 0;
process:
  for (auto &r : redoers) {
    // Scan the rest of the log
    if (not r.done and not r.is_impersonated() and r.try_impersonate()) {
      r.start();
    }
  }

  // Loop over existing redoers to scavenge and reuse available threads
  while (done < redoers.size()) {
    for (auto &r : redoers) {
      if (r.is_impersonated() and r.try_join()) {
        if (++done < redoers.size()) {
          goto process;
        }
        else {
          break;
        }
      }
    }
  }

  // Reset redoer states for backup servers to reuse
  for (auto &r : redoers) {
    r.done = false;
  }

  // WARNING: DO NOT TAKE CHKPT UNTIL WE REPLAYED ALL INDEXES!
  // Otherwise we migth lose some FIDs/OIDs created before the chkpt.
  //
  // For easier measurement (like "how long does it take to bring the
  // system back to fully memory-resident after recovery), we spawn the
  // warm-up thread after rebuilding indexes as well.
  if (sysconf::lazy_warm_up()) {
    oidmgr->start_warm_up();
  }

  std::cout << "[Recovery] done\n";
}

void
parallel_file_replay::redo_runner::my_work(char *) {
  auto himark = redo_file();

  // Now recover allocator status
  // Note: indexes currently don't use an OID array for themselves
  // (ie for tree nodes) so it's safe to do this any time as long
  // as it's before starting to process new transactions.
  if (himark)
    oidmgr->recreate_allocator(fid, himark);

#if SEPARATE_INDEX_REBUILD
  rebuild_index(scanner, fid, fid_index);
#endif

  done = true;
  __sync_synchronize();
}

FID
parallel_file_replay::redo_runner::redo_file() {
  ASSERT(oidmgr->file_exists(fid));
  RCU::rcu_enter();
  OID himark = 0;
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  auto *scan = owner->scanner->new_log_scan(owner->chkpt_begin, sysconf::eager_warm_up());
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
      owner->recover_update(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_DELETE:
      dcount++;
      owner->recover_update(scan, true);
      break;
    case sm_log_scan_mgr::LOG_INSERT_INDEX:
      iicount++;
#if SEPARATE_INDEX_REBUILD == 0
      owner->recover_index_insert(scan);
#endif
      break;
    case sm_log_scan_mgr::LOG_INSERT:
      icount++;
      owner->recover_insert(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_CHKPT:
      break;
    case sm_log_scan_mgr::LOG_FID:
      // The main recover function should have already did this
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

void
parallel_oid_replay::operator()(void *arg, sm_log_scan_mgr *s, LSN cb, LSN ce) {
  util::scoped_timer t("parallel_oid_replay");
  scanner = s;
  chkpt_begin = cb;

  RCU::rcu_enter();
  // Look for new table creations after the chkpt
  // Use one redo thread per new table found
  // XXX(tzwang): no support for dynamically created tables for now
  // TODO(tzwang): figure out how this interacts with chkpt

  // One hiwater_mark/capacity_mark per FID
  FID max_fid = 0;
  if (redoers.size() == 0) {
    auto *scan = scanner->new_log_scan(chkpt_begin, sysconf::eager_warm_up());
    for (; scan->valid(); scan->next()) {
      if (scan->type() != sm_log_scan_mgr::LOG_FID)
        continue;
      FID fid = scan->fid();
      max_fid = std::max(fid, max_fid);
      recover_fid(scan);
    }
    delete scan;
  }

  if (redoers.size() == 0) {
    for (uint32_t i = 0; i < nredoers; ++i) {
      redoers.emplace_back(this, i);
    }
  }

  // Fix internal files' marks
  oidmgr->recreate_allocator(sm_oid_mgr_impl::OBJARRAY_FID, max_fid);
  oidmgr->recreate_allocator(sm_oid_mgr_impl::ALLOCATOR_FID, max_fid);
  //oidmgr->recreate_allocator(sm_oid_mgr_impl::METADATA_FID, max_fid);

  uint32_t done = 0;
process:
  for (auto &r : redoers) {
    // Scan the rest of the log
    if (not r.done and not r.is_impersonated() and r.try_impersonate()) {
      r.start();
    }
  }

  // Loop over existing redoers to scavenge and reuse available threads
  while (done < redoers.size()) {
    for (auto &r : redoers) {
      if (r.is_impersonated() and r.try_join()) {
        if (++done < redoers.size()) {
          goto process;
        }
        else {
          break;
        }
      }
    }
  }

  // Reset redoer states for backup servers to reuse
  for (auto &r : redoers) {
    r.done = false;
  }

  // WARNING: DO NOT TAKE CHKPT UNTIL WE REPLAYED ALL INDEXES!
  // Otherwise we migth lose some FIDs/OIDs created before the chkpt.
  //
  // For easier measurement (like "how long does it take to bring the
  // system back to fully memory-resident after recovery), we spawn the
  // warm-up thread after rebuilding indexes as well.
  if (sysconf::lazy_warm_up()) {
    oidmgr->start_warm_up();
  }

  //std::cout << "[Recovery] done\n";
}

void
parallel_oid_replay::redo_runner::redo_partition() {
  util::scoped_timer t("redo_partition");
  RCU::rcu_enter();
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  auto *scan = owner->scanner->new_log_scan(owner->chkpt_begin, sysconf::eager_warm_up());
  static __thread std::unordered_map<FID, OID> max_oid;

  for (; scan->valid(); scan->next()) {
    auto oid = scan->oid();
    if (oid % owner->redoers.size() != oid_partition)
      continue;

    auto fid = scan->fid();
    max_oid[fid] = std::max(max_oid[fid], oid);

    switch (scan->type()) {
    case sm_log_scan_mgr::LOG_UPDATE:
    case sm_log_scan_mgr::LOG_RELOCATE:
      ucount++;
      owner->recover_update(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_DELETE:
      dcount++;
      owner->recover_update(scan, true);
      break;
    case sm_log_scan_mgr::LOG_INSERT_INDEX:
      iicount++;
#if SEPARATE_INDEX_REBUILD == 0
      owner->recover_index_insert(scan);
#endif
      break;
    case sm_log_scan_mgr::LOG_INSERT:
      icount++;
      owner->recover_insert(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_CHKPT:
      break;
    case sm_log_scan_mgr::LOG_FID:
      // The main recover function should have already did this
      ASSERT(oidmgr->file_exists(scan->fid()));
      break;
    default:
      DIE("unreachable");
    }
  }
  ASSERT(icount == iicount);
  printf("[Recovery.log] OID partition %d - inserts/updates/deletes/size: %lu/%lu/%lu/%lu\n",
    oid_partition, icount, ucount, dcount, size);

  for (auto &m : max_oid) {
    oidmgr->recreate_allocator(m.first, m.second);
  }

  delete scan;
  RCU::rcu_exit();
}

void
parallel_oid_replay::redo_runner::my_work(char *) {
  redo_partition();
  done = true;
  __sync_synchronize();
}
