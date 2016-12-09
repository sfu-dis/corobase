#include <unistd.h>
#include <numa.h>
#include "../macros.h"
#include "sm-config.h"
#include "sm-log-recover-impl.h"
#include "sm-thread.h"
#include <iostream>

bool config::verbose = true;
uint32_t config::worker_threads = 0;
uint32_t config::benchmark_seconds = 30;
uint32_t config::benchmark_scale_factor = 1;
bool config::parallel_loading = false;
bool config::retry_aborted_transactions = 0;
int config::backoff_aborted_transactions = 0;
int config::numa_nodes = 0;
int config::enable_gc = 0;
std::string config::tmpfs_dir("/dev/shm");
int config::enable_safesnap = 0;
int config::enable_ssi_read_only_opt = 0;
uint64_t config::ssn_read_opt_threshold = config::SSN_READ_OPT_DISABLED;
int config::wait_for_backups = 0;
int config::num_backups = 0;
int config::num_active_backups = 0;
int config::log_buffer_mb = 512;
int config::log_segment_mb = 8192;
std::string config::log_dir("");
bool config::null_log_device = false;
std::string config::primary_srv("");
std::string config::primary_port("10000");
int config::htt_is_on= 1;
uint64_t config::node_memory_gb = 12;
int config::recovery_warm_up_policy = config::WARM_UP_NONE;
int config::log_ship_warm_up_policy = config::WARM_UP_NONE;
bool config::log_ship_full_redo = false;
bool config::nvram_log_buffer = false;
bool config::group_commit = false;
int config::group_commit_queue_length = 25000;
int config::group_commit_timeout = 5;
sm_log_recover_impl *config::recover_functor = nullptr;
bool config::log_ship_by_rdma = false;
bool config::log_ship_sync_redo = false;
bool config::log_key_for_update = false;
int config::enable_chkpt = 0;
uint64_t config::chkpt_interval = 5;
int config::phantom_prot = 0;

uint32_t config::max_threads_per_node = 0;
bool config::loading = true;

uint64_t config::write_bytes_per_cycle = 0;

void
config::init() {
    ALWAYS_ASSERT(worker_threads);
    // We pin threads compactly, ie., socket by socket
    // Figure out how many socket we will occupy here; this determines how
    // much memory we allocate for the centralized pool per socket too.
    const long ncpus = ::sysconf(_SC_NPROCESSORS_ONLN);
    ALWAYS_ASSERT(ncpus);
    max_threads_per_node = htt_is_on ?
        ncpus / 2 / (numa_max_node() + 1): ncpus / (numa_max_node() + 1);
    numa_nodes = (worker_threads + max_threads_per_node - 1) /  max_threads_per_node;

    thread::init();
}

void config::sanity_check() {
    ALWAYS_ASSERT(recover_functor);
    ALWAYS_ASSERT(numa_nodes);
    ALWAYS_ASSERT(not group_commit or group_commit_queue_length);
}
