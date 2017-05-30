#include <x86intrin.h>
#include "../benchmarks/ndb_wrapper.h"
#include "../util.h"
#include "sm-index.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-oid-alloc-impl.h"
#include "sm-rep.h"

void
parallel_offset_replay::operator()(void *arg, sm_log_scan_mgr *s, LSN from, LSN to) {
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
      redo_runner* r = new redo_runner(this, INVALID_LSN, INVALID_LSN);
      redoers.push_back(r);
      bool success = r->try_impersonate(false);
      ALWAYS_ASSERT(success);
    }
  }

  uint64_t logbuf_part = -1;
  LSN partition_start = from;

  if(nredoers > 1) {
    // Figure out which index to start with
    uint64_t min_offset = ~uint64_t{0};
    for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
      LSN bound = LSN{ rep::logbuf_partition_bounds[i] };
      if(bound.offset() < min_offset && bound.offset() > from.offset()) {
        min_offset = bound.offset();
        logbuf_part = i;
      }
    }
  }

#ifndef NDEBUG
  uint32_t ndispatches = 0;
#endif
  bool all_dispatched = false;
  uint32_t idx = 0;
  while(!all_dispatched) {
    // Get a thread
    auto* r = redoers[idx];
    while(true) {
      if(r->try_wait()) {
        break;
      }
      idx = (idx + 1) % nredoers;
      r = redoers[idx];
    }
    if(logbuf_part == -1 || nredoers == 1) {
      // logbuf_part will be -1 if there is only one partition
      r->start_lsn = from;
      r->end_lsn = to;
      all_dispatched = true;
    } else {
      ASSERT(logbuf_part != -1);
      uint32_t part_id = logbuf_part % config::logbuf_partitions;
      ++logbuf_part;
      LSN partition_end = LSN{ rep::logbuf_partition_bounds[part_id] };

      if(partition_end < partition_start || partition_end >= to) {
        partition_end = to;
        all_dispatched = true;
      }
      r->start_lsn = partition_start;
      r->end_lsn = partition_end;

      DLOG(INFO) << "Dispatch " << r->me << " " << std::hex << partition_start.offset() 
                 << " - " << partition_end.offset() << std::dec;
      partition_start = partition_end;  // for next thread
    }
#ifndef NDEBUG
    ++ndispatches;
#endif
    r->start();
  }

  for (auto &r : redoers) {
    if(r->is_impersonated()) {
      r->wait();
    }
  }
#ifndef NDEBUG
  DLOG(INFO) << "Dispatched " << ndispatches << " threads";
#endif
}

void
parallel_offset_replay::redo_runner::redo_logbuf_partition() {
  ALWAYS_ASSERT(config::is_backup_srv());

  if(config::nvram_log_buffer &&
     config::persist_nvram_on_replay &&
     config::nvram_delay_type != config::kDelayNone) {
    auto *sid = logmgr->get_segment(start_lsn.segment());
    uint64_t start_byte = sid->buf_offset(start_lsn.offset());
    uint64_t size = end_lsn.offset() - start_lsn.offset();
    auto *buf = sm_log::logbuf->read_buf(start_byte, size);

    if(config::nvram_delay_type == config::kDelayClflush) {
      uint32_t clines = size / CACHELINE_SIZE;
      for(uint32_t i = 0; i < clines; ++i) {
        _mm_clflush(&buf[i * CACHELINE_SIZE]);
      }
    } else if (config::nvram_delay_type == config::kDelayClwbEmu) {
      uint64_t total_cycles = size * config::cycles_per_byte;
      unsigned int unused = 0;
      uint64_t cycle_end = __rdtscp(&unused) + total_cycles;
      while(__rdtscp(&unused) < cycle_end) {}
    }
    __atomic_add_fetch(&rep::persisted_nvram_size, size, __ATOMIC_SEQ_CST);
  }

  //util::scoped_timer t("redo_partition");
  RCU::rcu_enter();
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  auto *scan = owner->scanner->new_log_scan(start_lsn, config::eager_warm_up(), true);

  while(true) {
    if(!scan->valid()) {
#ifndef NDEBUG
      // Note: it's possible that we attempt to redo a deadzone on
      // backups as the log partition bounds dont' consider segment
      // boundaries.
      auto* sid = logmgr->get_segment(start_lsn.segment());
      ASSERT(size > 0 || (sid->contains(start_lsn) && sid->end_offset == end_lsn.offset()));
#endif
      break;
    }
    if(scan->payload_lsn() >= end_lsn) {
      break;
    }
    LSN payload_lsn = scan->payload_lsn();
    ALWAYS_ASSERT(payload_lsn >= start_lsn);
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
    case sm_log_scan_mgr::LOG_ENHANCED_DELETE:
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
    scan->next();
  }
  DLOG(INFO) << "[Recovery.log] 0x" << std::hex << start_lsn.offset() << "-"
    << end_lsn.offset() << " inserts/updates/deletes/size: " << std::dec
    << icount << "/" << ucount << "/" << dcount << "/" << size;

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
  __sync_synchronize();
}
