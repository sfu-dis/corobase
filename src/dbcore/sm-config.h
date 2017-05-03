#pragma once
#include <x86intrin.h>
#include <iostream>
#include <string>
#include <numa.h>
#include "sm-defs.h"
#include "../macros.h"

class sm_log_recover_impl;

struct config {
    // Common settings
    static bool verbose;
    static uint32_t benchmark_scale_factor;
    static uint32_t worker_threads;
    static int numa_nodes;
    static const uint32_t MAX_THREADS = 256;
    static const uint64_t MB = 1024 * 1024;
    static const uint64_t GB = MB * 1024;
    static std::string tmpfs_dir;
    static int htt_is_on;
    static uint32_t max_threads_per_node;
    static uint32_t state;
    static bool enable_chkpt;
    static uint64_t chkpt_interval;
    static uint64_t log_buffer_mb;
    static uint64_t log_segment_mb;
    static std::string log_dir;
    static bool nvram_log_buffer;
    static sm_log_recover_impl *recover_functor;
    static uint64_t node_memory_gb;
    static bool phantom_prot;

    // Primary-specific settings
    static bool parallel_loading;
    static bool retry_aborted_transactions;
    static int backoff_aborted_transactions;
    static int enable_gc;
    static uint32_t logbuf_partitions;
    static bool null_log_device;
    static bool group_commit;
    static int group_commit_timeout;
    static int group_commit_queue_length;  // how much to reserve

    // Backup-specific settings
    static uint32_t benchmark_seconds;
    static bool quick_bench_start;
    static bool wait_for_primary;
    static int replay_policy;

    // How does the backup replay log records?
    // Sync - replay in the critical path; ack 'persisted' only after replaying
    //        **and** persisted log records
    // Pipelined - replay out of the critical path; ack 'persisted' immediately
    //             after log records are durable. Start replay in another thread
    //             once received log records (usually overlapped with log flush).
    //             Log buffer cannot be reused until the replay is finished, but
    //             this doesn't block the primary as long as replay finishes fast
    //             enough, before the next batch arrives and log flush finishes.
    // Background - don't care about the shipping, just keep replaying from persisted
    //              log records continuously; no freshness guarantee.
    // None - don't replay at all.
    enum BackupReplayPolicy { kReplaySync, kReplayPipelined, kReplayBackground, kReplayNone};

    enum SystemState { kStateLoading, kStateForwardProcessing, kStateShutdown };
    static inline bool IsLoading() { return volatile_read(state) == kStateLoading; }
    static inline bool IsForwardProcessing() {
      return volatile_read(state) == kStateForwardProcessing;
    }
    static inline bool IsShutdown() { return volatile_read(state) == kStateShutdown; }

    // Warm-up policy when recovering from a chkpt or the log.
    // Set by --recovery-warm-up=[lazy/eager/whatever].
    //
    // lazy: spawn a thread to access every OID entry after recovery; log/chkpt
    //       recovery will only oid_put objects that contain the records' log location.
    //       Tx's might encounter some storage-resident versions, if the tx tried to
    //       access them before the warm-up thread fetched those versions.
    //
    // eager: dig out versions from the log when scanning the chkpt and log; all OID
    //        entries will point to some memory location after recovery finishes.
    //        Txs will only see memory-residents, no need to dig them out during execution.
    //
    // --recovery-warm-up ommitted or = anything else: don't do warm-up at all; it
    //        is the tx's burden to dig out versions when accessing them.
    enum WU_POLICY { WARM_UP_NONE, WARM_UP_LAZY, WARM_UP_EAGER };
    static int recovery_warm_up_policy;  // no/lazy/eager warm-up at recovery

    /* CC-related options */
    static int enable_ssi_read_only_opt;
    static uint64_t ssn_read_opt_threshold;
    static const uint64_t SSN_READ_OPT_DISABLED = 0xffffffffffffffff;

    // XXX(tzwang): enabling safesnap for tpcc basically halves the performance.
    // perf says 30%+ of cycles are on oid_get_version, which makes me suspect
    // it's because enabling safesnap makes the reader has to go deeper in the
    // version chains to find the desired version. So perhaps don't enable this
    // for update-intensive workloads, like tpcc. TPC-E to test and verify.
    static int enable_safesnap;

    /* Log shipping related options */
    static int wait_for_backups;
    static int num_backups;
    static int num_active_backups;
    static std::string primary_srv;
    static std::string primary_port;
    static int log_ship_warm_up_policy;
    static bool log_ship_by_rdma;
    static bool log_key_for_update;

    static uint64_t write_bytes_per_cycle;

    inline static bool is_backup_srv() {
        return primary_srv.size();
    }

    inline static bool eager_warm_up() {
      return recovery_warm_up_policy == WARM_UP_EAGER |
             log_ship_warm_up_policy == WARM_UP_EAGER;
    }

    inline static bool lazy_warm_up() {
      return recovery_warm_up_policy == WARM_UP_LAZY |
             log_ship_warm_up_policy == WARM_UP_LAZY;
    }

    static void init();
    static void sanity_check();
    static inline bool ssn_read_opt_enabled() {
        return ssn_read_opt_threshold < SSN_READ_OPT_DISABLED;
    }

    static inline void calibrate_spin_cycles() {
      char test_arr[CACHELINE_SIZE];
      unsigned int unused = 0;
      uint64_t start  = __rdtscp(&unused);
      _mm_clflush(test_arr);
      uint64_t end = __rdtscp(&unused);
      write_bytes_per_cycle = (double)CACHELINE_SIZE / (end - start);
      LOG(INFO) << write_bytes_per_cycle << " bytes per cycle";
    }

    static inline uint32_t num_backup_replay_threads() { 
      ALWAYS_ASSERT(is_backup_srv());
      // FIXME(tzwang): there will be only half of logbuf_partitions that can get
      // replayed concurrently. For simplicity we assign each partition a thread;
      // revisit if we need more flexibility, e.g.,let a thread take more than one
      // partition.
      // The backup now should have received from the primary about how many log
      // buffer partitions there will be now.
      ALWAYS_ASSERT(logbuf_partitions);
      uint32_t n = logbuf_partitions == 1 ? 1 : logbuf_partitions / 2;
      if(n > worker_threads) {
        LOG(FATAL) << "Not enough worker threads. Need " << n << ", "
                   << worker_threads << " provided";
      }
      return n;
    }

    static inline uint32_t num_worker_threads() {
      if(is_backup_srv()) {
        if(worker_threads > num_backup_replay_threads()) {
          return worker_threads - num_backup_replay_threads();
        } else {
          return 0;
        }
      } else {
        return worker_threads;
      }
    }
};
