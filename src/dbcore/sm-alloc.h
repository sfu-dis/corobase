#pragma once
#include <sched.h>
#include <numa.h>
#include <atomic>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <vector>
#include <future>
#include <new>
#include "sm-defs.h"
#include "epoch.h"
#include "../macros.h"
#include "../object.h"

#ifdef ENABLE_GC
typedef epoch_mgr::epoch_num epoch_num;

// oids that got updated, ie need to cleanup the overwritten versions
struct recycle_oid {
    uintptr_t btr;
    oid_type oid;
    recycle_oid *next;
    recycle_oid(uintptr_t b, oid_type o) : btr(b), oid(o), next(NULL) {}
};
#endif

namespace MM {
    void *allocate(uint64_t size);

#ifdef ENABLE_GC
    struct thread_data {
        bool initialized;
		uint64_t nbytes;
		uint64_t counts;
    };

    epoch_mgr::tls_storage *get_tls(void*);
    void global_init(void*);
    void* thread_registered(void*);
    void thread_deregistered(void *cookie, void *thread_cookie);
    void* epoch_ended(void *cookie, epoch_num e);
    void* epoch_ended_thread(void *cookie, void *epoch_cookie, void *thread_cookie);
    void epoch_reclaimed(void *cookie, void *epoch_cookie);

    void register_thread();
    void deregister_thread();
    void epoch_enter(void);
    void epoch_exit(void);
    void recycle(uintptr_t table, oid_type oid);
    void recycle(recycle_oid *list_head, recycle_oid *list_tail);
    extern LSN trim_lsn;
#endif
};

