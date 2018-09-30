#include <unistd.h>
#include <numa.h>
#include "../macros.h"
#include "sm-config.h"
#include "sm-log-recover-impl.h"
#include "sm-thread.h"
#include <iostream>

namespace ermia {

bool config::verbose = true;
uint32_t config::worker_threads = 0;
uint32_t config::benchmark_seconds = 30;
uint32_t config::benchmark_scale_factor = 1;
bool config::parallel_loading = false;
bool config::retry_aborted_transactions = false;
bool config::quick_bench_start = false;
bool config::wait_for_primary = true;
int config::backoff_aborted_transactions = 0;
int config::numa_nodes = 0;
int config::enable_gc = 0;
std::string config::tmpfs_dir("/dev/shm");
int config::enable_safesnap = 0;
int config::enable_ssi_read_only_opt = 0;
uint64_t config::ssn_read_opt_threshold = config::SSN_READ_OPT_DISABLED;
int config::wait_for_backups = 0;
int config::num_backups = 0;
std::atomic<uint32_t> config::num_active_backups(0);
uint64_t config::log_buffer_mb = 512;
uint64_t config::log_segment_mb = 8192;
uint32_t config::log_redo_partitions = 0;
std::string config::log_dir("");
bool config::null_log_device = false;
bool config::truncate_at_bench_start = false;
std::string config::primary_srv("");
std::string config::primary_port("10000");
bool config::htt_is_on = true;
bool config::print_cpu_util = false;
uint64_t config::node_memory_gb = 12;
bool config::log_ship_offset_replay = false;
int config::recovery_warm_up_policy = config::WARM_UP_NONE;
int config::log_ship_warm_up_policy = config::WARM_UP_NONE;
bool config::nvram_log_buffer = false;
uint32_t config::nvram_delay_type = config::kDelayNone;
bool config::group_commit = false;
uint32_t config::group_commit_queue_length = 25000;
uint32_t config::group_commit_timeout = 5;
uint64_t config::group_commit_size_kb = 4096;
uint64_t config::group_commit_bytes = 4096 * 1024;
sm_log_recover_impl *config::recover_functor = nullptr;
bool config::log_ship_by_rdma = false;
bool config::log_key_for_update = false;
bool config::enable_chkpt = 0;
uint64_t config::chkpt_interval = 50;
bool config::phantom_prot = 0;
double config::cycles_per_byte = 0;
uint32_t config::state = config::kStateLoading;
int config::replay_policy = config::kReplayPipelined;
bool config::full_replay = false;
uint32_t config::replay_threads = 0;
uint32_t config::threads = 0;
bool config::persist_nvram_on_replay = true;
int config::persist_policy = config::kPersistSync;
uint32_t config::read_view_stat_interval_ms;
std::string config::read_view_stat_file;
bool config::command_log = false;
uint32_t config::command_log_buffer_mb = 16;

void config::init() {
  ALWAYS_ASSERT(threads);
  thread::init();
  uint32_t max = thread::node_thread_pool::max_threads_per_node;
  numa_nodes = (threads + max - 1) / max;
  if (num_backups) {
    enable_chkpt = true;
  }
}

void config::sanity_check() {
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

}  // namespace ermia
