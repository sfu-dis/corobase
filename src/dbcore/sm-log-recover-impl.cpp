#include "../benchmarks/ndb_wrapper.h"
#include "../util.h"
#include "sm-index.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-oid-alloc-impl.h"
#include "sm-rep.h"

// Returns something that we will install on the OID entry. 
fat_ptr
sm_log_recover_impl::prepare_version(sm_log_scan_mgr::record_scan *logrec, fat_ptr next) {
  // Regardless of the replay/warm-up policy (ie whether to load tuples from storage
  // to memory), here we need a wrapper that points to the ``real'' localtion and
  // the next version.
  //
  // Note: payload_size() includes the whole varstr. See do_tree_put's log_update call.
  size_t sz = sizeof(Object);

  // Pre-allocate space for the payload
  sz += (sizeof(dbtuple) + logrec->payload_size());
  sz = align_up(sz);

  Object* obj = new (MM::allocate(sz, 0)) Object(logrec->payload_ptr(), next,
                                                 0, config::eager_warm_up());
  obj->SetClsn(logrec->payload_ptr());
  ASSERT(obj->GetClsn().asi_type() == fat_ptr::ASI_LOG);

  if(config::eager_warm_up()) {
    obj->Pin();
  }
  return fat_ptr::make(obj, encode_size_aligned(sz), 0);
}

void
sm_log_recover_impl::recover_insert(sm_log_scan_mgr::record_scan *logrec, bool latest) {
  FID f = logrec->fid();
  OID o = logrec->oid();
  fat_ptr ptr = prepare_version(logrec, NULL_PTR);
  ASSERT(oidmgr->file_exists(f));
  oid_array *oa = get_impl(oidmgr)->get_array(f);
  oa->ensure_size(o);
  // The chkpt recovery process might have picked up this tuple already
  if(latest) {
    if(!oidmgr->oid_put_latest(f, o, ptr, nullptr, logrec->payload_lsn().offset())) {
      MM::deallocate(ptr);
    }
  } else {
    oidmgr->oid_put_new_if_absent(f, o, ptr);
    ASSERT(oidmgr->oid_get(oa, o) == ptr);
  }
}

void
sm_log_recover_impl::recover_index_insert(sm_log_scan_mgr::record_scan *logrec) {
  // No need if the chkpt recovery already picked up this tuple
  FID fid = logrec->fid();
  IndexDescriptor* id = IndexDescriptor::Get(fid);
  if(config::is_backup_srv() ||
    oidmgr->oid_get(id->GetKeyArray(), logrec->oid()).offset() == 0) {
    recover_index_insert(logrec, id->GetIndex());
  }
}

void
sm_log_recover_impl::recover_index_insert(sm_log_scan_mgr::record_scan *logrec,
                                          OrderedIndex* index) {
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

  oid_array *ka = nullptr;
  if(!config::is_backup_srv() && volatile_read(*ka->get(logrec->oid())) != NULL_PTR) {
    ka = get_impl(oidmgr)->get_array(logrec->fid());
    return;
  }

  varstr payload_key((char*)payload_buf + sizeof(varstr), len);
  if(index->tree_.underlying_btree.insert_if_absent(payload_key, logrec->oid(), NULL)) {
    // Don't add the key on backup - on backup chkpt will traverse OID arrays
    if(!config::is_backup_srv()) {
      // Construct the varkey to be inserted in the oid array
      // (skip the varstr struct then it's data)
      varstr* key = (varstr*)MM::allocate(sizeof(varstr) + len, 0);
      new (key) varstr((char *)key + sizeof(varstr), len);
      key->copy_from((char *)payload_buf + sizeof(varstr), len);
      volatile_write(*ka->get(logrec->oid()), fat_ptr::make((void*)key, INVALID_SIZE_CODE));
    }
  }
}

void
sm_log_recover_impl::recover_update(sm_log_scan_mgr::record_scan *logrec,
                                    bool is_delete, bool latest) {
  FID f = logrec->fid();
  OID o = logrec->oid();
  ASSERT(oidmgr->file_exists(f));

  auto* oa = IndexDescriptor::Get(f)->GetTupleArray();
  fat_ptr head_ptr = *oa->get(o);

  fat_ptr ptr = NULL_PTR;
  if(!is_delete) {
    ptr = prepare_version(logrec, head_ptr);
  }
  if(latest) {
    if(!oidmgr->oid_put_latest(oa, o, ptr, nullptr, logrec->payload_lsn().offset())) {
      MM::deallocate(ptr);
    }
  } else {
    oidmgr->oid_put(oa, o, ptr);
  }
}

void
sm_log_recover_impl::recover_update_key(sm_log_scan_mgr::record_scan* logrec) {
  return;
  // Disabled for now, fix later
#if 0
  // Used when emulating the case where we didn't have OID arrays - must update tree leaf nodes
  auto* index = sm_index_mgr::get_index(logrec->fid());
  ASSERT(index);
  static const uint32_t kBufferSize = 128 * config::MB;
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

  varstr key((char*)payload_buf + sizeof(varstr), len);
  OID old_oid = 0;
  index->btr.underlying_btree.insert(key, logrec->oid(), nullptr, &old_oid, nullptr);
  ASSERT(old_oid == logrec->oid());
#endif
}

OrderedIndex* sm_log_recover_impl::recover_fid(sm_log_scan_mgr::record_scan *logrec) {
  // XXX(tzwang): no support for dynamically created tables for now
  char buf[256];
  auto sz = logrec->payload_size();
  ALWAYS_ASSERT(sz <= 256);  // 256 should be enough, revisit later if not
  logrec->load_object(buf, sz);
  FID key_fid = *(FID*)buf;
  std::string name(buf + sizeof(FID));

  // The benchmark should have registered the table with the engine
  ALWAYS_ASSERT(IndexDescriptor::NameExists(name));
  FID tuple_fid = logrec->fid();
  IndexDescriptor::Get(name)->Recover(tuple_fid, key_fid);
  LOG(INFO) << "[Recovery] " << name << "(" << tuple_fid << ", " << key_fid << ")";
  return IndexDescriptor::GetIndex(name);
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
  ASSERT(icount <= iicount);  // No insert log record for 2nd index
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
  DLOG(INFO) << "About to roll " << std::hex << from.offset() << " " << to.offset();

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

  uint64_t logbuf_part = -1;
  LSN partition_start = from;

  // Figure out which index to start with
  uint64_t min_offset = ~uint64_t{0};
  for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
    LSN bound = LSN{ rep::logbuf_partition_bounds[i] };
    if(bound.offset() < min_offset && bound.offset() > from.offset()) {
      min_offset = bound.offset();
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
    bool all_dispatched = false;
    while(!all_dispatched) {
      // Keep dispatching until we replayed all the needed range. One
      // partition per thread.
      for (auto &r : redoers) {
        // Assign a log buffer partition
        uint32_t idx = logbuf_part % config::logbuf_partitions;
        ++logbuf_part;
        LSN partition_end = LSN{ rep::logbuf_partition_bounds[idx] };

        r.start_lsn = partition_start;
        if(partition_end < partition_start) {
          partition_end = to;
          all_dispatched = true;
        }
        r.end_lsn = partition_end;

        DLOG(INFO) << "Dispatch for " << std::hex << partition_start.offset() 
                   << " - " << partition_end.offset() << std::dec;
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

      for (auto &r : redoers) {
        if(r.is_impersonated()) {
          r.join();
          r.done = false;
          if(!all_dispatched) {
            break;
          }
        }
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
    LSN payload_lsn = scan->payload_lsn();
    //ALWAYS_ASSERT(payload_lsn >= start_lsn);
    ALWAYS_ASSERT(payload_lsn.segment() >= 1);
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
  ASSERT(icount <= iicount);
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
