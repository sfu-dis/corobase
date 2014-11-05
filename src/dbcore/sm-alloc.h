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

#define MSB_MASK (uint64_t{1} << 63)

// RA GC states. Transitions btw these states are racy
// (should be fine assuming gc finishes before the new
// active region depletes).
#define RA_NORMAL       0
#define RA_GC_REQUESTED 1
#define RA_GC_PREPARED  2
#define RA_GC_IN_PROG   3
#define RA_GC_FINISHED  4
#define RA_GC_SPARING   5

class region_allocator;
typedef epoch_mgr::epoch_num epoch_num;

namespace RA {
    extern bool system_loading;
    extern region_allocator *ra;
    void init();
    void register_thread();
    void *allocate(uint64_t size);
    void *allocate_cold(uint64_t size);
    void allocate_fat(fat_ptr *ptr, uint64_t *seg, int *sock, uint64_t size);

    struct thread_data {
        bool initialized;
    };

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

class region_allocator {
    friend void RA::epoch_reclaimed(void *cookie, void *epoch_cookie);
    friend void* RA::epoch_ended(void *cookie, epoch_num e);
private:
    enum { NUM_SEGMENT_BITS=2 };
    enum { NUM_SEGMENTS=1<<NUM_SEGMENT_BITS };

    // low-contention and read-mostly stuff
    public:
    char *_hot_data;
    char *_cold_data;
    uint64_t _segment_bits;
    uint64_t _hot_bits;
    uint64_t _hot_capacity;
    uint64_t _cold_capacity;
    uint64_t _hot_mask;
    uint64_t _reclaimed_offset;
    int _socket;

    // high contention, needs its own cache line
    uint64_t __attribute__((aligned(64))) _allocated_hot_offset;
    uint64_t __attribute__((aligned(64))) _allocated_cold_offset;

    // gc related
    std::mutex _reclaim_mutex;
    std::condition_variable _reclaim_cv;
    uint64_t _allocated;
    int _state;
public:
    int _gc_segment;

public:
    void* allocate(uint64_t size);
    void* allocate_cold(uint64_t size);
    void allocate_fat(fat_ptr *ptr, uint64_t *seg, int *sock, uint64_t size);
    void allocate_fat(fat_ptr *ptr, uint64_t size);
    region_allocator(uint64_t one_segment_bits, int skt);
    ~region_allocator();
    int state() { return volatile_read(_state); }
    bool try_set_state(int from, int to);
    inline void trigger_reclaim()  { _reclaim_cv.notify_all(); }
    static void reclaim_daemon(int socket);
};

