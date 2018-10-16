#include <unistd.h>
#include <numa.h>
#include "../macros.h"
#include "sm-config.h"
#include "sm-log-recover-impl.h"
#include "sm-thread.h"
#include <iostream>

namespace ermia {
namespace config {

bool verbose = true;
uint32_t worker_threads = 0;
uint32_t benchmark_seconds = 30;
uint32_t benchmark_scale_factor = 1;
bool parallel_loading = false;
bool retry_aborted_transactions = false;
bool quick_bench_start = false;
bool wait_for_primary = true;
int backoff_aborted_transactions = 0;
int numa_nodes = 0;
int enable_gc = 0;
std::string tmpfs_dir("/dev/shm");
int enable_safesnap = 0;
int enable_ssi_read_only_opt = 0;
uint64_t ssn_read_opt_threshold = SSN_READ_OPT_DISABLED;
int wait_for_backups = 0;
int num_backups = 0;
std::atomic<uint32_t> num_active_backups(0);
uint64_t log_buffer_mb = 512;
uint64_t log_segment_mb = 8192;
uint32_t log_redo_partitions = 0;
std::string log_dir("");
bool null_log_device = false;
bool truncate_at_bench_start = false;
std::string primary_srv("");
std::string primary_port("10000");
bool htt_is_on = true;
bool print_cpu_util = false;
uint64_t node_memory_gb = 12;
bool log_ship_offset_replay = false;
int recovery_warm_up_policy = WARM_UP_NONE;
int log_ship_warm_up_policy = WARM_UP_NONE;
bool nvram_log_buffer = false;
uint32_t nvram_delay_type = kDelayNone;
bool group_commit = false;
uint32_t group_commit_queue_length = 25000;
uint32_t group_commit_timeout = 5;
uint64_t group_commit_size_kb = 4096;
uint64_t group_commit_bytes = 4096 * 1024;
sm_log_recover_impl *recover_functor = nullptr;
bool log_ship_by_rdma = false;
bool log_key_for_update = false;
bool enable_chkpt = 0;
uint64_t chkpt_interval = 50;
bool phantom_prot = 0;
double cycles_per_byte = 0;
uint32_t state = kStateLoading;
int replay_policy = kReplayPipelined;
bool full_replay = false;
uint32_t replay_threads = 0;
uint32_t threads = 0;
bool persist_nvram_on_replay = true;
int persist_policy = kPersistSync;
uint32_t read_view_stat_interval_ms;
std::string read_view_stat_file;
bool command_log = false;
uint32_t command_log_buffer_mb = 16;

void init() {
  ALWAYS_ASSERT(threads);
  thread::Initialize();
  uint32_t max = thread::PerNodeThreadPool::max_threads_per_node;
  numa_nodes = (threads + max - 1) / max;
  ALWAYS_ASSERT(numa_nodes);
  if (num_backups) {
    enable_chkpt = true;
  }
}

void sanity_check() {
  ALWAYS_ASSERT(recover_functor || is_backup_srv());
  ALWAYS_ASSERT(numa_nodes);
  ALWAYS_ASSERT(not group_commit or group_commit_queue_length);
  if (is_backup_srv()) {
    // Must have replay threads if replay is wanted
    ALWAYS_ASSERT(replay_policy == kReplayNone || replay_threads > 0);
    if (log_ship_by_rdma) {
      // No RDMA based cmdlog for now
      ALWAYS_ASSERT(!command_log);
    }
  }
}

}  // namespace config
}  // namespace ermia
