#pragma once
#include <mutex>
#include "sm-config.h"
#include "sm-defs.h"
#include "epoch.h"
#include "../macros.h"
#include "../object.h"

#include <sparsehash/dense_hash_map>
using google::dense_hash_map;

typedef epoch_mgr::epoch_num epoch_num;

// oids that got updated, ie need to cleanup the overwritten versions
struct oid_array;
struct recycle_oid {
    fat_ptr* entry;
    recycle_oid(fat_ptr* entry) : entry(entry) {}
};

namespace MM {
    /* Object allocation and reuse:
     * The GC thread continuously removes stale versions that aren't needed any 
     * more from version chains and put these objects to a centralized pool,
     * which contains a hashtab indexed by object size. All objects are allocated
     * in aligned sizes (ie allocated size might be larger than the actual size).
     *
     * Each transaction thread has a TLS pool of objects, which is almost the
     * same as the global pool, but without any CC. Only the thread itself has
     * access. To allocate an object, the thread first checks this local pool,
     * if empty it will ask the global pool for a list of objects (object_list).
     *
     * The GC thread replenishes the central pool in units of object_lists.
     *
     * If GC is disabled, we'd always be allocating from the central memory pool
     * (not the **object** pool, which will be empty all the time).
     */

    // The GC thread returns a list of objects each time
    struct object_list {
        static const size_t CAPACITY = 200000;

        fat_ptr head;
        fat_ptr tail;
        object_list* next;
        uint32_t nobjects;

        object_list(fat_ptr h, fat_ptr t, object_list* nxt, uint32_t nr) :
            head(h), tail(t), next(nxt), nobjects(nr)  {}
        object_list() : head(NULL_PTR), tail(NULL_PTR), next(NULL), nobjects(0) {}

        inline size_t object_size() { return decode_size_aligned(head.size_code()); }
        bool put(fat_ptr objptr);
    };

    class object_pool {
        // A hashtab of objects reclaimed by the gc daemon.
        // Maps object size -> head of list of object lists which have the the same object size
        // Each thread has its own private pool, the GC thread replenishes these per-thread pools
        // in a round-robin manner. Threads are free to grab (multiple) objects from here.
        dense_hash_map<size_t, object_list*> pool[sysconf::MAX_THREADS];
        std::mutex lock;

    public:
        object_pool() {
          for (uint32_t i = 0; i < sysconf::MAX_THREADS; ++i) {
            pool[i].set_empty_key(0);
          }
        }

        // Tx threads use this to replenish their TLS pool
        object_list* get_object_list(size_t size);

        // Return a list of objects to the pool; the gc thread is the only caller.
        void put_object_list(object_list& ol, uint32_t thread_index);
    };

    // Same thing as object_pool, but for a thread; no CC whatsoever
    class thread_object_pool {
        dense_hash_map<size_t, object_list> pool;
    public:
        thread_object_pool() { pool.set_empty_key(0); }

        object* get_object(size_t size);
        void put_objects(object_list& ol);
    };

    void prepare_node_memory();
    void *allocate(size_t size, epoch_num e);
    void deallocate(fat_ptr p);
    void* allocate_onnode(size_t size);

    extern uint64_t safesnap_lsn;

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

    extern epoch_mgr mm_epochs;
    inline void register_thread() {
        mm_epochs.thread_init();
    }
    inline void deregister_thread() {
        mm_epochs.thread_fini();
    }
    inline epoch_num epoch_enter(void) {
        return mm_epochs.thread_enter();
    }

    void epoch_exit(uint64_t s, epoch_num e);
    void recycle(fat_ptr list_head, fat_ptr list_tail);
};

