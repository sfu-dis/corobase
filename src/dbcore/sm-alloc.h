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
// only care about min 32, 64, 128, and 256 bytes of objects,
// too small ones (<32 bytes) will be kept in the 1st list and
// be free()'ed by tx's dtor. Other sizes will be used by txs
// through put() and get().
// Note: object_pool is supoosed to be tx/thread-local.
enum { MAX_SIZE_ORDER=4, BASE_OBJECT_SIZE=32 };
class object_pool {
    struct reuse_object {
        epoch_num epoch;
        object *obj;
        reuse_object *next;
        reuse_object(epoch_num e, object *p) : epoch(e), obj(p), next(NULL) {}
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
    object *get(epoch_num e, size_t size);
    void put(epoch_num e, object *p);
    void scavenge_order0(epoch_num e);
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
    void epoch_exit(void);
    void recycle(uintptr_t table, oid_type oid);
    void recycle(recycle_oid *list_head, recycle_oid *list_tail);
    extern LSN trim_lsn;
#endif
};

