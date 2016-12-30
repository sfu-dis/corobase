#include "../benchmarks/ndb_wrapper.h"
#include "../txn_btree.h"
#include "../util.h"
#include "sm-file.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-oid-alloc-impl.h"
#include "sm-rep.h"

// The version-loading mechanism will only dig out the latest version as a result.
fat_ptr
sm_log_recover_impl::recover_prepare_version(sm_log_scan_mgr::record_scan *logrec, fat_ptr next) {
  // Note: payload_size() includes the whole varstr
  // See do_tree_put's log_update call.
  if (not config::eager_warm_up()) {
    ASSERT(logrec->payload_ptr().asi_type() == fat_ptr::ASI_LOG);
    return logrec->payload_ptr();
  }

  size_t sz = sizeof(object);
  sz += (sizeof(dbtuple) + logrec->payload_size());
  sz = align_up(sz);

  object *obj = new (MM::allocate(sz, 0)) object(logrec->payload_ptr(), next, 0);
  obj->_clsn = logrec->payload_ptr();

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
  obj->_clsn = logrec->payload_lsn().to_log_ptr();
  ASSERT(logrec->payload_lsn().offset() == logrec->payload_ptr().offset());
  ASSERT(obj->_clsn.asi_type() == fat_ptr::ASI_LOG);
  return fat_ptr::make(obj, encode_size_aligned(sz));
}

void
sm_log_recover_impl::recover_insert(sm_log_scan_mgr::record_scan *logrec, bool latest) {
  FID f = logrec->fid();
  OID o = logrec->oid();
  fat_ptr ptr = recover_prepare_version(logrec, NULL_PTR);
  ASSERT(oidmgr->file_exists(f));
  oid_array *oa = get_impl(oidmgr)->get_array(f);
  oa->ensure_size(o);
  // The chkpt recovery process might have picked up this tuple already
  if(latest) {
    oidmgr->oid_put_latest(f, o, ptr, nullptr, logrec->payload_lsn().offset());
  } else {
    oidmgr->oid_put_new_if_absent(f, o, ptr, nullptr);
  }
}

void
sm_log_recover_impl::recover_index_insert(sm_log_scan_mgr::record_scan *logrec) {
  // No need if the chkpt recovery already picked up this tuple
  FID fid = logrec->fid();
  auto* fd = sm_file_mgr::get_file(fid);
  auto* oa = fd->main_array;
  if(oa->get(logrec->oid())->key == nullptr) {
    recover_index_insert(logrec, fd->index);
  }
}

void
sm_log_recover_impl::recover_index_insert(sm_log_scan_mgr::record_scan *logrec, ndb_ordered_index *index) {
  static const uint32_t kBufferSize = 128 * config::MB;
  ASSERT(index);
  auto sz = align_up(logrec->payload_size());
  static __thread char *buf;
  if (unlikely(not buf)) {
    buf = (char *)malloc(kBufferSize);
  }
  char* payload_buf = nullptr;
  ALWAYS_ASSERT(sz < kBufferSize);
  if(logrec->payload_lsn().offset() >= logmgr->durable_flushed_lsn_offset()) {
    // In the log buffer, point directly to it without memcpy
    ASSERT(config::is_backup_srv());
    auto *logrec_impl = get_impl(logrec);
    logrec_impl->scan.has_payloads = true;  // FIXME(tzwang): do this in a better way
    payload_buf = (char*)logrec_impl->scan.payload();
    ALWAYS_ASSERT(payload_buf);
  } else {
    logrec->load_object(buf, sz);
    payload_buf = buf;
  }

  // Extract the real key length (don't use varstr.data()!)
  size_t len = ((varstr *)payload_buf)->size();
  ASSERT(align_up(len + sizeof(varstr)) == sz);

  oid_array *oa = get_impl(oidmgr)->get_array(logrec->fid());
  if(volatile_read(oidmgr->oid_get_entry_ptr(oa, logrec->oid())->key)) {
    return;
  }

  // Construct the varkey (skip the varstr struct then it's data)
  varstr* key = (varstr*)MM::allocate(sizeof(varstr) + len, 0);
  new (key) varstr((char *)key + sizeof(varstr), len);
  key->copy_from((char *)payload_buf + sizeof(varstr), len);

  if(index->btr.underlying_btree.insert_if_absent(*key, logrec->oid(), NULL, 0)) {
    __sync_bool_compare_and_swap(&oidmgr->oid_get_entry_ptr(oa, logrec->oid())->key, nullptr, key);
  }
}

void
sm_log_recover_impl::recover_update(sm_log_scan_mgr::record_scan *logrec,
                                    bool is_delete, bool latest) {
  FID f = logrec->fid();
  OID o = logrec->oid();
  ASSERT(oidmgr->file_exists(f));

  auto* oa = sm_file_mgr::get_file(f)->main_array;
  fat_ptr head_ptr = oa->get(o)->ptr;

  fat_ptr ptr = NULL_PTR;
  if(!is_delete) {
    ptr = recover_prepare_version(logrec, head_ptr);
  }
  if(latest) {
    oidmgr->oid_put(oa, o, ptr);
  } else {
    oidmgr->oid_put_latest(oa, o, ptr, nullptr, logrec->payload_lsn().offset());
  }
}

void
sm_log_recover_impl::recover_update_key(sm_log_scan_mgr::record_scan* logrec) {
  // Used when emulating the case where we didn't have OID arrays - must update tree leaf nodes
  auto* index = sm_file_mgr::get_index(logrec->fid());
  ASSERT(index);
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
  varstr* key = (varstr*)MM::allocate(sizeof(varstr) + len, 0);
  new (key) varstr((char *)key + sizeof(varstr), len);
  key->copy_from((char *)buf + sizeof(varstr), len);

  OID old_oid = 0;
  index->btr.underlying_btree.insert(*key, logrec->oid(), nullptr, &old_oid, nullptr);
  ALWAYS_ASSERT(old_oid == logrec->oid());
}

ndb_ordered_index*
sm_log_recover_impl::recover_fid(sm_log_scan_mgr::record_scan *logrec) {
  char name_buf[256];
  FID f = logrec->fid();
  auto sz = logrec->payload_size();
  ALWAYS_ASSERT(sz <= 256);  // 256 should be enough, revisit later if not
  logrec->load_object(name_buf, sz);
  std::string name(name_buf);
  // XXX(tzwang): no support for dynamically created tables for now
  ASSERT(sm_file_mgr::name_map.find(name) != sm_file_mgr::name_map.end());
  ASSERT(!sm_file_mgr::get_index(name));
  sm_file_mgr::name_map[name]->fid = f;  // fill in the fid
  // The benchmark should have registered the table with the engine
  sm_file_mgr::name_map[name]->index = new ndb_ordered_index(name);
  ASSERT(not oidmgr->file_exists(f));
  oidmgr->recreate_file(f);
  ASSERT(sm_file_mgr::name_map[name]->index);
  sm_file_mgr::name_map[name]->index->set_oid_array(f);
  sm_file_mgr::name_map[name]->main_array = oidmgr->get_array(f);
  if (sm_file_mgr::fid_map.find(f) == sm_file_mgr::fid_map.end()) {
    // chkpt recovery might have did this
    ASSERT(sm_file_mgr::name_map[name]->index);
    sm_file_mgr::fid_map[f] = sm_file_mgr::name_map[name];
  }

  printf("[Recovery: log] FID(%s) = %d\n", name_buf, f);
  return sm_file_mgr::get_index(name);
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
 */
void
parallel_file_replay::operator()(void *arg, sm_log_scan_mgr *s, LSN from, LSN to) {
  scanner = s;
  start_lsn = from;
  end_lsn = to;

  RCU::rcu_enter();
  // Look for new table creations after the chkpt
  // Use one redo thread per new table found
  // XXX(tzwang): no support for dynamically created tables for now
  // TODO(tzwang): figure out how this interacts with chkpt

  // One hiwater_mark/capacity_mark per FID
  FID max_fid = 0;
  std::vector<struct redo_runner> redoers;
  auto *scan = scanner->new_log_scan(start_lsn, config::eager_warm_up());
  for (; scan->valid() and scan->payload_lsn() < end_lsn; scan->next()) {
    if (scan->type() != sm_log_scan_mgr::LOG_FID)
      continue;
    FID fid = scan->fid();
    max_fid = std::max(fid, max_fid);
    recover_fid(scan);
  }
  delete scan;

  for (auto &fm : sm_file_mgr::fid_map) {
    sm_file_descriptor *fd = fm.second;
    ASSERT(fd->fid);
    max_fid = std::max(fd->fid, max_fid);
    redoers.emplace_back(this, fd->fid, fd->index);
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
  if (config::lazy_warm_up()) {
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
  if (himark) {
    oidmgr->recreate_allocator(fid, himark);
  }
  done = true;
  __sync_synchronize();
}

FID
parallel_file_replay::redo_runner::redo_file() {
  ASSERT(oidmgr->file_exists(fid));
  RCU::rcu_enter();
  OID himark = 0;
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  auto *scan = owner->scanner->new_log_scan(owner->start_lsn, config::eager_warm_up());
  for (; scan->valid() and scan->payload_lsn() < owner->end_lsn; scan->next()) {
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
      owner->recover_update(scan, false, false);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_UPDATE_KEY:
      owner->recover_update_key(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_DELETE:
      dcount++;
      owner->recover_update(scan, true, false);
      break;
    case sm_log_scan_mgr::LOG_INSERT_INDEX:
      iicount++;
      owner->recover_index_insert(scan);
      break;
    case sm_log_scan_mgr::LOG_INSERT:
      icount++;
      owner->recover_insert(scan);
      size += scan->payload_size();
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
  DLOG(INFO) << "[Recovery.log] FID " << fid
    << " - inserts/updates/deletes/size: "
    << icount << "/" << ucount << "/" << dcount << "/" << size;

  delete scan;
  RCU::rcu_exit();
  return himark;
}

void
parallel_oid_replay::operator()(void *arg, sm_log_scan_mgr *s, LSN from, LSN to) {
  util::scoped_timer t("parallel_oid_replay");
  scanner = s;
  start_lsn = from;
  end_lsn = to;

  RCU::rcu_enter();
  // Look for new table creations after the chkpt
  // Use one redo thread per new table found
  // XXX(tzwang): no support for dynamically created tables for now
  // TODO(tzwang): figure out how this interacts with chkpt

  // One hiwater_mark/capacity_mark per FID
  FID max_fid = 0;
  if (redoers.size() == 0) {
    auto *scan = scanner->new_log_scan(start_lsn, config::eager_warm_up());
    for (; scan->valid() and scan->payload_lsn() < end_lsn; scan->next()) {
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
  if (config::lazy_warm_up()) {
    oidmgr->start_warm_up();
  }

  //std::cout << "[Recovery] done\n";
}

void
parallel_oid_replay::redo_runner::redo_partition() {
  //util::scoped_timer t("redo_partition");
  RCU::rcu_enter();
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  ALWAYS_ASSERT(owner->start_lsn.segment() >= 1);
  auto *scan = owner->scanner->new_log_scan(owner->start_lsn, config::eager_warm_up());
  static __thread std::unordered_map<FID, OID> max_oid;

  for (; scan->valid() and scan->payload_lsn() < owner->end_lsn; scan->next()) {
    auto oid = scan->oid();
    if (oid % owner->redoers.size() != oid_partition)
      continue;

    auto fid = scan->fid();
    if(!config::is_backup_srv()) {
      max_oid[fid] = std::max(max_oid[fid], oid);
    }

    switch (scan->type()) {
    case sm_log_scan_mgr::LOG_UPDATE_KEY:
      owner->recover_update_key(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_UPDATE:
    case sm_log_scan_mgr::LOG_RELOCATE:
      ucount++;
      owner->recover_update(scan, false, false);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_DELETE:
      dcount++;
      owner->recover_update(scan, true, false);
      break;
    case sm_log_scan_mgr::LOG_INSERT_INDEX:
      iicount++;
      owner->recover_index_insert(scan);
      break;
    case sm_log_scan_mgr::LOG_INSERT:
      icount++;
      owner->recover_insert(scan);
      size += scan->payload_size();
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
  DLOG(INFO) << "[Recovery.log] OID partition " << oid_partition
    << " - inserts/updates/deletes/size: "
    << icount << "/" << ucount << "/" << dcount << "/" << size;

  if(!config::is_backup_srv()) {
    for(auto &m : max_oid) {
      oidmgr->recreate_allocator(m.first, m.second);
    }
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

void
parallel_offset_replay::operator()(void *arg, sm_log_scan_mgr *s, LSN from, LSN to) {
  util::scoped_timer t("parallel_offset_replay");
  scanner = s;

  RCU::rcu_enter();
  // Look for new table creations after the chkpt
  // Use one redo thread per new table found
  // XXX(tzwang): no support for dynamically created tables for now
  // XXX(tzwang): 20161212: So far this is for log shipping only, so no support
  // for recovering table FIDs.

  if(redoers.size() == 0) {
    for (uint32_t i = 0; i < nredoers; ++i) {
      redoers.emplace_back(this, INVALID_LSN, INVALID_LSN);
    }
  }

  uint32_t done = 0;
  uint64_t logbuf_part = -1;
  LSN partition_start = from;

  // Figure out which index to start with
  uint64_t min_offset = ~uint64_t{0};
  for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
    uint64_t off = rep::logbuf_partition_bounds[i];
    if(off < min_offset && off > from.offset()) {
      min_offset = off;
      logbuf_part = i;
    }
  }
  if(logbuf_part == -1) {
    // Too small, a single thread is enough
    auto& r = redoers[0];
    bool v = r.try_impersonate();
    ALWAYS_ASSERT(v);
    r.start_lsn = from;
    r.end_lsn = to;
    __sync_synchronize();
    r.start();
    r.join();
    r.done = false;
  } else {
    uint32_t examined_parts = 0;

    bool all_dispatched = false;
    for (auto &r : redoers) {
      // Assign a log buffer partition
      if(examined_parts++ >= config::logbuf_partitions) {
        break;
      }
      uint32_t idx = logbuf_part % config::logbuf_partitions;
      ++logbuf_part;
      LSN partition_end = LSN{ rep::logbuf_partition_bounds[idx] };
      if(!partition_end.offset() || partition_end.offset() > to.offset()) {
        partition_end = to;
        all_dispatched = true;
      }

      if(partition_end.offset() && partition_end.offset() > partition_start.offset()) {
        r.start_lsn = partition_start;
        r.end_lsn = partition_end;

        DLOG(INFO) << "start=" << std::hex << partition_start.offset() 
          << " end=" << partition_end.offset() << std::dec;
        partition_start = partition_end;  // for next thread
        __sync_synchronize();
        ALWAYS_ASSERT(!r.is_impersonated());
        auto v = r.try_impersonate();
        ALWAYS_ASSERT(v);
        r.start();
        if(all_dispatched) {
          break;
        }
      }
    }

    for (auto &r : redoers) {
      if(r.is_impersonated()) {
        r.join();
        ++done;
        r.done = false;
      }
    }
  }
}

void
parallel_offset_replay::redo_runner::redo_logbuf_partition() {
  ALWAYS_ASSERT(config::is_backup_srv());

  //util::scoped_timer t("redo_partition");
  RCU::rcu_enter();
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  auto *scan = owner->scanner->new_log_scan(start_lsn, config::eager_warm_up());

  for (; scan->valid() and scan->payload_lsn() < end_lsn; scan->next()) {
    ALWAYS_ASSERT(scan->payload_lsn() >= start_lsn);
    ALWAYS_ASSERT(scan->payload_lsn().segment() >= 1);
    auto oid = scan->oid();
    auto fid = scan->fid();

    switch (scan->type()) {
    case sm_log_scan_mgr::LOG_UPDATE_KEY:
      owner->recover_update_key(scan);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_UPDATE:
    case sm_log_scan_mgr::LOG_RELOCATE:
      ucount++;
      owner->recover_update(scan, false, true);
      size += scan->payload_size();
      break;
    case sm_log_scan_mgr::LOG_DELETE:
      dcount++;
      owner->recover_update(scan, true, true);
      break;
    case sm_log_scan_mgr::LOG_INSERT_INDEX:
      iicount++;
      owner->recover_index_insert(scan);
      break;
    case sm_log_scan_mgr::LOG_INSERT:
      icount++;
      owner->recover_insert(scan, true);
      size += scan->payload_size();
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
  DLOG(INFO) << "[Recovery.log] 0x" << start_lsn.offset() << "-" << end_lsn.offset()
    << " inserts/updates/deletes/size: " << std::dec << icount << "/"
    << ucount << "/" << dcount << "/" << size;

  // Normally we'd also recreate_allocator here; for log shipping
  // redo this takes ~10% of total cycles (need to take a lock etc),
  // and backups don't take writes until take-over, so we do it when
  // taking over as new primary only.
  // TODO(tzwang): record the maximum OID to use during take-over.
  delete scan;
  RCU::rcu_exit();
}

void
parallel_offset_replay::redo_runner::my_work(char *) {
  redo_logbuf_partition();
  done = true;
  __sync_synchronize();
}
