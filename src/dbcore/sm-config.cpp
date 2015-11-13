#include "../macros.h"
#include "sm-config.h"
#include <numa.h>

uint32_t sysconf::_active_threads = 0;
uint32_t sysconf::worker_threads = 0;
int sysconf::numa_nodes = 0;
uint32_t sysconf::threads;
uint64_t sysconf::prefault_gig = 64;
int sysconf::enable_gc = 0;
std::string sysconf::tmpfs_dir("/tmpfs");
int sysconf::enable_safesnap = 0;
int sysconf::enable_ssi_read_only_opt = 0;

void
sysconf::init() {
    numa_nodes = numa_max_node() + 1;
}

void sysconf::sanity_check() {
    ALWAYS_ASSERT(numa_nodes);
}
