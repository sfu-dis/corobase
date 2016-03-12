#include "../macros.h"
#include "sm-config.h"
#include <numa.h>

uint32_t sysconf::_active_threads = 0;
uint32_t sysconf::worker_threads = 0;
int sysconf::numa_nodes = 0;
uint64_t sysconf::prefault_gig = 64;
int sysconf::enable_gc = 0;
std::string sysconf::tmpfs_dir("/tmpfs");
int sysconf::enable_safesnap = 0;
int sysconf::enable_ssi_read_only_opt = 0;
uint64_t sysconf::ssn_read_opt_threshold = sysconf::SSN_READ_OPT_DISABLED;
int sysconf::log_buffer_mb = 512;
int sysconf::log_segment_mb = 8192;
std::string sysconf::log_dir("");
int sysconf::null_log_device = 0;

void
sysconf::init() {
    numa_nodes = numa_max_node() + 1;
}

void sysconf::sanity_check() {
    ALWAYS_ASSERT(numa_nodes);
}
