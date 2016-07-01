#include <unistd.h>
#include <numa.h>
#include "../macros.h"
#include "sm-config.h"
#include "sm-log-recover-impl.h"
#include "sm-thread.h"
#include <iostream>

uint32_t sysconf::_active_threads = 0;
uint32_t sysconf::worker_threads = 0;
int sysconf::numa_nodes = 0;
int sysconf::enable_gc = 0;
std::string sysconf::tmpfs_dir("/tmpfs");
int sysconf::enable_safesnap = 0;
int sysconf::enable_ssi_read_only_opt = 0;
uint64_t sysconf::ssn_read_opt_threshold = sysconf::SSN_READ_OPT_DISABLED;
int sysconf::wait_for_backups = 0;
int sysconf::num_backups = 0;
int sysconf::num_active_backups = 0;
int sysconf::log_buffer_mb = 512;
int sysconf::log_segment_mb = 8192;
std::string sysconf::log_dir("");
int sysconf::null_log_device = 0;
std::string sysconf::primary_srv("");
std::string sysconf::primary_port("10000");
int sysconf::htt_is_on= 1;
uint64_t sysconf::node_memory_gb = 12;
int sysconf::recovery_warm_up_policy = sysconf::WARM_UP_NONE;
int sysconf::log_ship_warm_up_policy = sysconf::WARM_UP_NONE;
int sysconf::nvram_log_buffer = 0;
int sysconf::group_commit = 0;
int sysconf::group_commit_queue_length = 5000;
sm_log_recover_impl *sysconf::recover_functor = nullptr;
int sysconf::log_ship_by_rdma = 0;
int sysconf::log_ship_sync_redo = 0;

uint32_t sysconf::max_threads_per_node = 0;
bool sysconf::loading = true;

void
sysconf::init() {
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

void sysconf::sanity_check() {
    ALWAYS_ASSERT(recover_functor);
    ALWAYS_ASSERT(numa_nodes);
    ALWAYS_ASSERT(not group_commit or group_commit_queue_length);
}
