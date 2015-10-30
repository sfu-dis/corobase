#include "../macros.h"
#include "sm-config.h"
#include <numa.h>

uint32_t sysconf::_active_threads = 0;
uint32_t sysconf::worker_threads = 0;
int sysconf::numa_nodes = 0;
uint32_t sysconf::threads;

void
sysconf::init() {
    numa_nodes = numa_max_node() + 1;
}

void sysconf::sanity_check() {
    ALWAYS_ASSERT(numa_nodes);
}
