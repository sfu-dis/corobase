#pragma once
#include <atomic>
#include <cpuid.h>
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
  static uint32_t threads;
  static uint32_t worker_threads;
  static int numa_nodes;
  static const uint32_t MAX_THREADS = 256;
  static const uint64_t MB = 1024 * 1024;
  static const uint64_t GB = MB * 1024;
  static std::string tmpfs_dir;
  static bool htt_is_on;
  static uint32_t max_threads_per_node;
  static uint32_t state;
  static bool enable_chkpt;
  static uint64_t chkpt_interval;
  static uint64_t log_buffer_mb;
  static uint64_t log_segment_mb;
  static std::string log_dir;
  static uint32_t read_view_stat_interval_ms;
  static std::string read_view_stat_file;

  // NVRAM settings - for backup servers only, the primary doesn't care.
  static bool nvram_log_buffer;
  static uint32_t nvram_delay_type;
  static sm_log_recover_impl *recover_functor;
  static uint64_t node_memory_gb;
  static bool phantom_prot;

  // Primary-specific settings
  static bool parallel_loading;
  static bool retry_aborted_transactions;
  static int backoff_aborted_transactions;
  static int enable_gc;
  static uint32_t log_redo_partitions;
  static bool null_log_device;
  static bool fake_log_write;
  static bool group_commit;
  static uint32_t group_commit_timeout;
  static uint32_t group_commit_queue_length;  // how much to reserve
  static uint64_t group_commit_size_mb;
  static uint64_t group_commit_bytes;

  // Backup-specific settings
  static uint32_t benchmark_seconds;
  static bool quick_bench_start;
  static bool wait_for_primary;
  static int replay_policy;
  static uint32_t replay_threads;
  static bool persist_nvram_on_replay;
  static int persist_policy;

  // Create an object for each version and install directly on the main
  // indirection arrays only; for experimental purpose only to see the
  // difference between pipelined/sync replay which use the pdest array.
  static bool full_replay;

  // How does the backup replay log records?
  // Sync - replay in the critical path; ack 'persisted' only after replaying
  //        **and** persisted log records
  // Pipelined - replay out of the critical path; ack 'persisted' immediately
  //             after log records are durable. Start replay in another thread
  //             once received log records (usually overlapped with log flush).
  //             Log buffer cannot be reused until the replay is finished, but
  //             this doesn't block the primary as long as replay finishes fast
  //             enough, before the next batch arrives and log flush finishes.
  // Background - don't care about the shipping, just keep replaying from
  // persisted
  //              log records continuously; no freshness guarantee.
  // None - don't replay at all.
  enum BackupReplayPolicy {
    kReplaySync,
    kReplayPipelined,
    kReplayBackground,
    kReplayNone
  };

  // When should the backup ensure shipped log records from the primary are
  // persisted?
  // Sync - as in 'synchronous log shipping', primary waits for ack before
  //        return;
  // Async - as in 'asynchronous log shipping', primary doesn't ship on the
  //         critical path as part of the commit/log flush process. Instead,
  //         a background thread does the log shipping.
  // Pipelined - same as Sync but wait for ack before starting to ship the next
  //             batch, i.e., backup's persist operation of batch i is
  //             overlapped with forward processing of batch i+1 on the
  //             primary.
  enum BackupPersistPolicy {
    kPersistSync,
    kPersistAsync,
    kPersistPipelined
  };

  enum NvramDelayType { kDelayNone, kDelayClflush, kDelayClwbEmu };

  enum SystemState { kStateLoading, kStateForwardProcessing, kStateShutdown };
  static inline bool IsLoading() {
    return volatile_read(state) == kStateLoading;
  }
  static inline bool IsForwardProcessing() {
    return volatile_read(state) == kStateForwardProcessing;
  }
  static inline bool IsShutdown() {
    return volatile_read(state) == kStateShutdown;
  }

  // Warm-up policy when recovering from a chkpt or the log.
  // Set by --recovery-warm-up=[lazy/eager/whatever].
  //
  // lazy: spawn a thread to access every OID entry after recovery; log/chkpt
  //       recovery will only oid_put objects that contain the records' log
  //       location.
  //       Tx's might encounter some storage-resident versions, if the tx tried
  //       to
  //       access them before the warm-up thread fetched those versions.
  //
  // eager: dig out versions from the log when scanning the chkpt and log; all
  // OID
  //        entries will point to some memory location after recovery finishes.
  //        Txs will only see memory-residents, no need to dig them out during
  //        execution.
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
  static std::atomic<uint32_t> num_active_backups;
  static std::string primary_srv;
  static std::string primary_port;
  static int log_ship_warm_up_policy;
  static bool log_ship_by_rdma;
  static bool log_key_for_update;

  static double cycles_per_byte;

  inline static bool is_backup_srv() { return primary_srv.size(); }

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

  static inline void CalibrateNvramDelay() {
    /* 20170511(tzwang): tried two ways of estimating how many cycles
     * we need to write one byte to memory. Using clflush gives around
     * 4-5 cycles/byte, non-temporal store gives ~1-1.3. This was on the
     * c6220 node.
     */
    /*
    static const uint32_t kCachelines = 1024;
    char test_arr[kCachelines * CACHELINE_SIZE] CACHE_ALIGNED;
    unsigned int unused = 0;
    unsigned int eax, ebx, ecx, edx;

    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    uint64_t start  = __rdtscp(&unused);
    for(uint32_t i = 0; i < kCachelines; ++i) {
      _mm_clflush(&test_arr[i * CACHELINE_SIZE]);
    }
    uint64_t end = __rdtscp(&unused);
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    cycles_per_byte = (end - start) / (double)(kCachelines * CACHELINE_SIZE);
    LOG(INFO) << cycles_per_byte << " cycles per bytes";
    */
    static const uint32_t kElements = 1024 * 1024 * 64;  // 64MB
    int *test_arr = (int *)malloc(sizeof(int) * kElements);

    unsigned int unused = 0;
    unsigned int eax, ebx, ecx, edx;

    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    uint64_t start = __rdtscp(&unused);
    for (uint32_t i = 0; i < kElements; ++i) {
      // Non-temporal store to skip the cache
      _mm_stream_si32(&test_arr[i], 0xdeadbeef);
    }
    _mm_sfence();  // need fence for non-temporal store
    uint64_t end = __rdtscp(&unused);
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    cycles_per_byte = (end - start) / (double)(kElements * sizeof(int));
    LOG(INFO) << cycles_per_byte << " cycles per bytes";
    free(test_arr);
  }

  static inline void NvramClflush(const char *data, uint64_t size) {
    ASSERT(config::nvram_delay_type == config::kDelayClflush);
    uint32_t clines = size / CACHELINE_SIZE;
    for (uint32_t i = 0; i < clines; ++i) {
      _mm_clflush(&data[i * CACHELINE_SIZE]);
    }
  }

  static inline void NvramClwbEmu(uint64_t size) {
    ASSERT(config::nvram_delay_type == config::kDelayClwbEmu);
    uint64_t total_cycles = size * config::cycles_per_byte;
    unsigned int unused = 0;
    uint64_t cycle_end = __rdtscp(&unused) + total_cycles;
    while (__rdtscp(&unused) < cycle_end) {
    }
  }
};
