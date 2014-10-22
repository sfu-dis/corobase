#pragma once
#include <sched.h>
#include <numa.h>
#include <atomic>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include "sm-defs.h"
#include "../macros.h"

namespace RA {
    void init();
    void register_thread();
    void *allocate(uint64_t size);
};

static struct ra_wrapper {
    ra_wrapper() { RA::init(); }
} ra_wrapper_static_init;

