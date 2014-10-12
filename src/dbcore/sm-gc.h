#pragma once
#include <type_traits>
#include <thread>
#include <numa.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include "../macros.h"
#include "../core.h"
#include "../util.h"
#include "../rcu-wrapper.h"
#include "epoch.h"
#include "sm-log.h"

/*
 * The garbage collector for ERMIA to remove dead tuple versions.
 */
class GC {
    typedef epoch_mgr::epoch_num epoch_num;

    // if _allocated_memory reaches this many, start GC
    // FIXME: tzwang: problem: if this watermark is too low, then some tx or
    // thread might stuck in some epoch (or we could say some epoch is stuck)
    // because the tx needs more memory, i.e., this tx might need to cross
    // epoch boundaries.
    // TODO. should be based on physical memory size or GC performance
    static const size_t WATERMARK = 128*1024*1024;

    static percore<size_t, false, false> allocated_memory;
    static sm_log *logger;
    LSN lsn;

public:
    struct thread_data {
        bool initialized;
    };
    static __thread struct thread_data tls;

    GC(sm_log *l);
    void cleaner_daemon();
    void report_malloc(size_t nbytes);
    void epoch_enter();
    void epoch_exit();
    void epoch_quiesce();

    static void global_init(void*);
    static epoch_mgr::tls_storage *get_tls(void*);
    static void* thread_registered(void*);
    static void thread_deregistered(void*, void*);
    static void* epoch_ended(void *cookie, epoch_num e);
    static void* epoch_ended_thread(void *, void *epoch_cookie, void *);
    static void epoch_reclaimed(void *cookie, void *epoch_cookie);

private:
    epoch_mgr epochs {{nullptr, &global_init, &get_tls,
                         &thread_registered, &thread_deregistered,
                         &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

    scoped_rcu_region _guard;
};  // end of namespace

