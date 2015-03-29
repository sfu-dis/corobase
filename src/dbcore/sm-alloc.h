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
// A pool of objects deallocated by GC, save some calls to
// tcmalloc in allocate().
// Note: object_pool is supoosed to be tx/thread-local.
//
// order 0: (0, 32B)
// order 1: [32, 64)
// order 2: [64, 128)
// order 3: [128, +inf)
//
// Transactions can reuse these objects via put/get functions.
enum { MAX_SIZE_ORDER=4, BASE_OBJECT_SIZE=32 };
class object_pool {
    struct reuse_object {
        uint64_t cstamp;
        //epoch_num epoch;    // object created during this epoch
        object *obj;
        reuse_object *next;
        reuse_object(uint64_t c, object *p) : cstamp(c), obj(p), next(NULL) {}
    };

    // put at tail, get at head
    reuse_object *head[MAX_SIZE_ORDER];
    reuse_object *tail[MAX_SIZE_ORDER];

    int get_order(size_t size) {
        int o = size / BASE_OBJECT_SIZE;
        if (o > MAX_SIZE_ORDER-1)
            o = MAX_SIZE_ORDER-1;
        return o;
    }

public:
    object_pool() {
        memset(head, '\0', sizeof(reuse_object *) * MAX_SIZE_ORDER);
        memset(tail, '\0', sizeof(reuse_object *) * MAX_SIZE_ORDER);
    }
    object *get(size_t size);
    void put(epoch_num e, object *p);
};

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
    object_pool *get_object_pool();

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
    epoch_num epoch_enter(void);
    void epoch_exit(LSN s, epoch_num e);
    void recycle(uintptr_t table, oid_type oid);
    void recycle(recycle_oid *list_head, recycle_oid *list_tail);
#endif
};

