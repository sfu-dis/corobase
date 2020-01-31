#pragma once
#include <atomic>
#include <cpuid.h>
#include <x86intrin.h>
#include <iostream>
#include <string>
#include <numa.h>
#include "sm-defs.h"

namespace ermia {

struct sm_log_recover_impl;

namespace config {

static const uint32_t MAX_THREADS = 256;
static const uint64_t MB = 1024 * 1024;
static const uint64_t GB = MB * 1024;

// Common settings
extern bool tls_alloc;
extern bool threadpool;
extern bool verbose;
extern std::string benchmark;
extern uint32_t benchmark_scale_factor;
extern uint32_t threads;
extern uint32_t worker_threads;
extern int numa_nodes;
extern bool numa_spread;
extern std::string tmpfs_dir;
extern bool htt_is_on;
extern bool physical_workers_only;
extern uint32_t state;
extern bool enable_chkpt;
extern uint64_t chkpt_interval;
extern uint64_t log_buffer_mb;
extern uint64_t log_segment_mb;
extern std::string log_dir;
extern uint32_t read_view_stat_interval_ms;
extern std::string read_view_stat_file;
extern bool command_log;
extern uint32_t command_log_buffer_mb;
extern bool print_cpu_util;
extern uint32_t arena_size_mb;
extern bool enable_perf;
extern std::string perf_record_event;

// NVRAM settings - for backup servers only, the primary doesn't care.
extern bool nvram_log_buffer;
extern uint32_t nvram_delay_type;
extern sm_log_recover_impl *recover_functor;
extern uint64_t node_memory_gb;
extern bool phantom_prot;

// Primary-specific settings
extern bool parallel_loading;
extern bool retry_aborted_transactions;
extern int backoff_aborted_transactions;
extern int enable_gc;
extern uint32_t log_redo_partitions;
extern bool null_log_device;
extern bool truncate_at_bench_start;
extern bool group_commit;
extern uint32_t group_commit_timeout;
extern uint32_t group_commit_queue_length;  // how much to reserve
extern uint64_t group_commit_size_kb;
extern uint64_t group_commit_bytes;

// Backup-specific settings
extern uint32_t benchmark_seconds;
extern bool quick_bench_start;
extern bool wait_for_primary;
extern int replay_policy;
extern uint32_t replay_threads;
extern bool persist_nvram_on_replay;
extern int persist_policy;

// DIA-specific settings
extern bool index_probe_only;
extern std::string dia_req_handler;
extern bool dia_req_coalesce;
extern uint32_t dia_batch_size;
extern uint32_t dia_logical_index_threads;
extern uint32_t dia_physical_index_threads;

extern bool coro_tx;
extern uint32_t coro_batch_size;

// Create an object for each version and install directly on the main
// indirection arrays only; for experimental purpose only to see the
// difference between pipelined/sync replay which use the pdest array.
extern bool full_replay;

extern bool log_ship_offset_replay;

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
inline bool IsLoading() {
  return volatile_read(state) == kStateLoading;
}
inline bool IsForwardProcessing() {
  return volatile_read(state) == kStateForwardProcessing;
}
inline bool IsShutdown() {
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
extern int recovery_warm_up_policy;  // no/lazy/eager warm-up at recovery

/* CC-related options */
extern int enable_ssi_read_only_opt;
extern uint64_t ssn_read_opt_threshold;
static const uint64_t SSN_READ_OPT_DISABLED = 0xffffffffffffffff;

// XXX(tzwang): enabling safesnap for tpcc basically halves the performance.
// perf says 30%+ of cycles are on oid_get_version, which makes me suspect
// it's because enabling safesnap makes the reader has to go deeper in the
// version chains to find the desired version. So perhaps don't enable this
// for update-intensive workloads, like tpcc. TPC-E to test and verify.
extern int enable_safesnap;

/* Log shipping related options */
extern int wait_for_backups;
extern int num_backups;
extern std::atomic<uint32_t> num_active_backups;
extern std::string primary_srv;
extern std::string primary_port;
extern int log_ship_warm_up_policy;
extern bool log_ship_by_rdma;
extern bool log_key_for_update;

extern bool amac_version_chain;

extern double cycles_per_byte;

inline bool is_backup_srv() { return primary_srv.size(); }

inline bool eager_warm_up() {
  return recovery_warm_up_policy == WARM_UP_EAGER ||
         log_ship_warm_up_policy == WARM_UP_EAGER;
}

inline bool lazy_warm_up() {
  return recovery_warm_up_policy == WARM_UP_LAZY ||
         log_ship_warm_up_policy == WARM_UP_LAZY;
}

void init();
void sanity_check();
inline bool ssn_read_opt_enabled() {
  return ssn_read_opt_threshold < SSN_READ_OPT_DISABLED;
}

inline void CalibrateNvramDelay() {
  /* 20170511(tzwang): tried two ways of estimating how many cycles
   * we need to write one byte to memory. Using clflush gives around
   * 4-5 cycles/byte, non-temporal store gives ~1-1.3. This was on the
   * c6220 node.
   */
  /*
  const uint32_t kCachelines = 1024;
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
  const uint32_t kElements = 1024 * 1024 * 64;  // 64MB
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

inline void NvramClflush(const char *data, uint64_t size) {
  ASSERT(config::nvram_delay_type == config::kDelayClflush);
  uint32_t clines = size / CACHELINE_SIZE;
  for (uint32_t i = 0; i < clines; ++i) {
    _mm_clflush(&data[i * CACHELINE_SIZE]);
  }
}

inline void NvramClwbEmu(uint64_t size) {
  ASSERT(config::nvram_delay_type == config::kDelayClwbEmu);
  uint64_t total_cycles = size * config::cycles_per_byte;
  unsigned int unused = 0;
  uint64_t cycle_end = __rdtscp(&unused) + total_cycles;
  while (__rdtscp(&unused) < cycle_end) {
  }
}
}  // namespace config
}  // namespace ermia
