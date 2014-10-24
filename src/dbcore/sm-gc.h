#pragma once
#include <type_traits>
#include <thread>
#include <condition_variable>
#include <mutex>
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
#include "../masstree_btree.h"

class region_allocator;

/*
 * The garbage collector for ERMIA to remove dead tuple versions.
 */
class GC {
    friend class region_allocator;
    typedef epoch_mgr::epoch_num epoch_num;

    // if _allocated_memory reaches this many, start GC
    // TODO. should be based on physical memory size or GC performance
    static const size_t WATERMARK = 64*1024*1024;

    static percore<size_t, false, false> allocated_memory;
    static sm_log *logger;

    // use this (unprotected) global variable to store the lsn, it should be
    // fine as the cleaner only reads it and the only writer is epoch-mgr.
    static LSN reclaim_lsn;

    static std::condition_variable cleaner_cv;
    static std::mutex cleaner_mutex;

public:
    struct thread_data {
        bool initialized;
    };
    static __thread struct thread_data tls;

    GC(sm_log *l);
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
    static void cleaner_daemon();
	static void register_table(concurrent_btree* table);

private:
	static std::vector<concurrent_btree*> tables;
    epoch_mgr epochs {{nullptr, &global_init, &get_tls,
                         &thread_registered, &thread_deregistered,
                         &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

//    scoped_rcu_region _guard;
};  // end of namespace

