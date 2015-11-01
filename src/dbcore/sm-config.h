#pragma once
#include "sm-defs.h"
class sysconf {
public:
    static uint32_t _active_threads;
    static uint32_t worker_threads;
    static uint32_t threads;
    static int numa_nodes;
    static const uint32_t MAX_THREADS = 1024;
    static uint64_t prefault_gig;

    static void sanity_check();

    inline static uint32_t my_thread_id() {
        static __thread uint32_t __id = 0;
        if (__id == 0) {
            __id = __sync_fetch_and_add(&_active_threads, 1);
        }
        ASSERT(__id < threads);
        return __id;
    }
    
    static void init();
};
