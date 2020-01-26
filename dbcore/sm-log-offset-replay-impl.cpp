#include <x86intrin.h>
#include "../ermia.h"
#include "rcu.h"
#include "sm-table.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-oid-alloc-impl.h"
#include "sm-rep.h"
#include "sm-rep-rdma.h"

namespace ermia {

LSN parallel_offset_replay::operator()(void *arg, sm_log_scan_mgr *s,
                                       LSN from, LSN to) {
  MARK_REFERENCED(arg);
  MARK_REFERENCED(from);
  scanner = s;
  RCU::rcu_enter();
  for (uint32_t i = 0; i < nredoers; ++i) {
    redo_runner *r = new redo_runner(this, INVALID_LSN, INVALID_LSN);
    redoers.push_back(r);
    bool success = r->TryImpersonate(false);
    ALWAYS_ASSERT(success);
    r->Start();
  }
  RCU::rcu_exit();
  return to;
}

void parallel_offset_replay::redo_runner::persist_logbuf_partition() {
  ALWAYS_ASSERT(config::is_backup_srv());
  ALWAYS_ASSERT(config::nvram_log_buffer);
  ALWAYS_ASSERT(config::persist_nvram_on_replay);

  auto *sid = logmgr->get_segment(start_lsn.segment());
  uint64_t start_byte = sid->buf_offset(start_lsn.offset());
  uint64_t size = end_lsn.offset() - start_lsn.offset();
  auto *buf = sm_log::logbuf->read_buf(start_byte, size);

  if (config::nvram_delay_type == config::kDelayClflush) {
    uint32_t clines = size / CACHELINE_SIZE;
    for (uint32_t i = 0; i < clines; ++i) {
      _mm_clflush(&buf[i * CACHELINE_SIZE]);
    }
  } else if (config::nvram_delay_type == config::kDelayClwbEmu) {
    uint64_t total_cycles = size * config::cycles_per_byte;
    unsigned int unused = 0;
    uint64_t cycle_end = __rdtscp(&unused) + total_cycles;
    while (__rdtscp(&unused) < cycle_end) {
    }
  }
  __atomic_add_fetch(&rep::persisted_nvram_size, size, __ATOMIC_SEQ_CST);
}

void parallel_offset_replay::redo_runner::redo_logbuf_partition() {
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  // FIXME(tzwang): must read from storage for background async replay
  auto *scan =
      owner->scanner->new_log_scan(start_lsn, config::eager_warm_up(),
         config::replay_policy != config::kReplayBackground);

  util::timer t;
  while (!config::IsShutdown()) {
    if (!scan->valid()) {
#ifndef NDEBUG
      // Note: it's possible that we attempt to redo a deadzone on backups as
      // the log partition bounds don't consider segment boundaries.
      auto *sid = logmgr->get_segment(start_lsn.segment());
      ASSERT(size > 0 ||
             (sid->contains(start_lsn) && sid->end_offset == end_lsn.offset()));
#endif
      break;
    }
    if (scan->payload_lsn().offset() >= end_lsn.offset()) {
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
        break;
      case sm_log_scan_mgr::LOG_UPDATE:
      case sm_log_scan_mgr::LOG_RELOCATE:
        ucount++;
        owner->recover_update(scan, false, true);
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
        break;
      case sm_log_scan_mgr::LOG_FID:
        // The main recover function should have already did this
        ASSERT(oidmgr->file_exists(scan->fid()));
        break;
      default:
        DIE("unreachable");
    }
    size += scan->payload_size();
    scan->next();
  }
  redo_latency_us += t.lap();
  redo_size += size;
  ++redo_batches;
  DLOG(INFO) << "[Recovery.log] 0x" << std::hex << start_lsn.offset() << "-"
             << end_lsn.offset()
             << " inserts/updates/deletes/size: " << std::dec << icount << "/"
             << ucount << "/" << dcount << "/" << size << " "
             << redo_latency_us << "us so far";

  // Normally we'd also recreate_allocator here; for log shipping
  // redo this takes ~10% of total cycles (need to take a lock etc),
  // and backups don't take writes until take-over, so we do it when
  // taking over as new primary only.
  // TODO(tzwang): record the maximum OID to use during take-over.
  delete scan;
}

struct redo_range {
  LSN start;
  LSN end;
};

void parallel_offset_replay::redo_runner::MyWork(char *) {
  // Distributing persistence work over replay threads: when enabled, this
  // allows each replay thread to persist its own replay partition so we get
  // lower NVRAM persistence latency. But a pure persist-replay procedure would
  // delay the primary if each thread has more than one partition to replay. So
  // what we do is first persist the partitions and collect the corresponding
  // LSN offsets for redo later.
  redo_range *ranges = nullptr;
  bool persist_first = false;
  if (config::nvram_log_buffer && config::persist_nvram_on_replay) {
    persist_first = true;
    ranges = new redo_range[config::log_redo_partitions];
  }

  RCU::rcu_register();
  DEFER(RCU::rcu_deregister());
  RCU::rcu_enter();
  DEFER(RCU::rcu_exit());

  while (true) {
    for (uint32_t i = 0; i < 2; ++i) {
      uint32_t num_ranges = 0;
      rep::ReplayPipelineStage& stage = rep::pipeline_stages[i];
      LSN stage_end = INVALID_LSN;
      do {
        stage_end = volatile_read(stage.end_lsn);
      } while (stage_end.offset() <= volatile_read(rep::replayed_lsn_offset));

      LSN stage_start = volatile_read(stage.start_lsn);
      ASSERT(stage_start.segment() == stage_end.segment());

      DLOG(INFO) << "Start to roll " << std::hex << stage_start.offset()
        << "-" << stage_end.offset() << std::dec << " " << i;

      // Find myself a partition
      uint64_t min_offset = ~uint64_t{0};
      uint64_t idx = -1;
      for (uint32_t j = 0; j < config::log_redo_partitions; ++j) {
        LSN bound = LSN{stage.log_redo_partition_bounds[j]};
        if (bound.offset() < min_offset && bound.offset() > stage_start.offset()) {
          min_offset = bound.offset();
          idx = j;
        }
      }
      bool is_last_thread = false;
      if (idx != -1) {
        start_lsn = stage_start;
        bool done = false;
        while (!done) {
          end_lsn = LSN{stage.log_redo_partition_bounds[idx]};
          if (end_lsn.offset() < stage_start.offset() || end_lsn.offset() > stage.end_lsn.offset()) {
              end_lsn = stage_end;
              done = true;
          }
          if (start_lsn == end_lsn) {
            break;
          }
          // Get a partition - one potential problem is some threads act always
          // faster and get the work. So it's important to have each thread do
          // some amount of non-trivial work after claiming a partition (e.g.,
          // persist it or replay it).
          if (stage.consumed[idx].exchange(true, std::memory_order_seq_cst) == false) {
            DLOG(INFO) << "[Backup] found log partition " << std::hex << start_lsn.offset()
                       << "." << start_lsn.segment() << "-" << end_lsn.offset() << "."
                       << end_lsn.segment() << std::dec;
            if (persist_first) {
              ranges[num_ranges].start = start_lsn;
              ranges[num_ranges].end = end_lsn;
              num_ranges++;
              persist_logbuf_partition();
            } else {
              redo_logbuf_partition();
            }
          }
          start_lsn = end_lsn;
          idx = (idx + 1) % config::log_redo_partitions;
        }
        LOG_IF(FATAL, end_lsn.offset() != stage_end.offset());
        if (persist_first) {
          // At this point all my partitions are persisted and I won't block the
          // primary. Now replay them.
          for (uint32_t i = 0; i < num_ranges; ++i) {
            start_lsn = ranges[i].start;
            end_lsn = ranges[i].end;
            redo_logbuf_partition();
          }
        }
        if (--stage.num_replaying_threads == 0) {
          volatile_write(rep::replayed_lsn_offset, stage.end_lsn.offset());
          DLOG(INFO) << "replayed_lsn_offset=" << std::hex << rep::replayed_lsn_offset << std::dec;
          is_last_thread = true;
        }
      } else {
        // Just one partition
        if (--stage.num_replaying_threads == 0) {
          end_lsn = stage_end;
          start_lsn = stage_start;
          if (persist_first) {
            persist_logbuf_partition();
          }
          redo_logbuf_partition();
          DLOG(INFO) << "[Backup] Rolled forward log " << std::hex << start_lsn.offset()
                     << "." << start_lsn.segment() << "-" << end_lsn.offset() << "."
                     << end_lsn.segment() << std::dec;
          volatile_write(rep::replayed_lsn_offset, stage.end_lsn.offset());
          is_last_thread = true;
        }
      }
      // Make sure everyone is finished before we look at the next stage
      while (volatile_read(rep::replayed_lsn_offset) != stage_end.offset()) {}
    }
  }
}
}  // namespace ermia
