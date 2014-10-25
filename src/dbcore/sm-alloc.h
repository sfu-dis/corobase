#pragma once
#include <sched.h>
#include <numa.h>
#include <atomic>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include "sm-defs.h"
#include "epoch.h"
#include "../macros.h"

namespace RA {
    extern bool system_loading;
    void init();
    void register_thread();
    void *allocate(uint64_t size);
    void *allocate_cold(uint64_t size);

    struct thread_data {
        bool initialized;
    };

    typedef epoch_mgr::epoch_num epoch_num;

    epoch_mgr::tls_storage *get_tls(void*);
    void global_init(void*);
    void* thread_registered(void*);
    void thread_deregistered(void *cookie, void *thread_cookie);
    void* epoch_ended(void *cookie, epoch_num e);
    void* epoch_ended_thread(void *cookie, void *epoch_cookie, void *thread_cookie);
    void epoch_reclaimed(void *cookie, void *epoch_cookie);

    void epoch_enter(void);
    void epoch_exit(void);
    void epoch_thread_quiesce(void);
};

static struct ra_wrapper {
    ra_wrapper() { RA::init(); }
} ra_wrapper_static_init;

class scoped_ra_region {
public:
    scoped_ra_region(void)
    {
        RA::epoch_enter();
    }

    ~scoped_ra_region(void)
    {
        RA::epoch_exit();
    }
};
