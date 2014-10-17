#pragma once
#include <atomic>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include "sm-defs.h"
#include "../macros.h"

class region_allocator;

class mem_region {
    friend class region_allocator;
private:
    void* _data;
    uint64_t _capacity;
    uint64_t _allocated;
    void* try_alloc(uint64_t size);

public:
    mem_region(uint64_t cap);
    ~mem_region(void);
};

class region_allocator {
private:
    mem_region* _regions[3];
    mem_region* _active;
    mem_region* _reclaiming;
    mem_region* _spare[2];
    //std::atomic<mem_region*> _active;
    //std::atomic<mem_region*> _reclaiming;
    //std::atomic<mem_region*> _spare[2];
    std::mutex _ptr_lock;   // FIXME: tzwang: possible to avoid?

public:
    void* allocate(uint64_t size);
    region_allocator(uint64_t one_region_cap);
};

namespace RA {
    // FIXME: change this to use some get_page_size function.
    static const uint64_t PAGE_SIZE = 4096;
    static const uint64_t MEM_REGION_SIZE = 1024 * 1024 * PAGE_SIZE;
    extern region_allocator ra;

    inline void *allocate(uint64_t size) {
        return ra.allocate(size);
    }
};

