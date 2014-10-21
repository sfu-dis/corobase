#include <sys/mman.h>
#include "sm-alloc.h"

namespace RA {
    static const uint64_t PAGE_SIZE = 4096;
    static const uint64_t MEM_REGION_SIZE = 384 * 1024 * PAGE_SIZE;
    
    ra_wrapper ra_w;
    region_allocator *ra;

    void init() {
        int nodes = numa_max_node() + 1;
        ra = (region_allocator *)malloc(sizeof(region_allocator) * nodes);
        for (int i = 0; i < nodes; i++)
            new (ra + i) region_allocator(MEM_REGION_SIZE, i);
    }
};

mem_region::mem_region(uint64_t cap, int skt) : _capacity(cap), _allocated(0)
{
    _data = numa_alloc_onnode(cap, skt);
    //_data = mmap(NULL, _capacity, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    ASSERT(_data);
    std::cout << "memory region: faulting " << cap << " bytes"
              << " on node " << skt << std::endl;
    memset(_data, '\0', _capacity);
}

mem_region::~mem_region(void)
{
    free(_data);
}

// This should only be called by region_allocator's allocate() as it
// could return null; region_allocator's allocate() will do the necessary
// switching to spare etc.
void*
mem_region::try_alloc(uint64_t size)
{
    uint64_t alloc_pos = __sync_fetch_and_add(&_allocated, size);

    // succeeded, get the memory if we still have space
    if (_allocated < _capacity)
        return (char *)_data + alloc_pos;
    return NULL;
}

region_allocator::region_allocator(uint64_t one_region_cap, int skt)
{
    _regions[0] = new mem_region(one_region_cap, skt);
    _regions[1] = new mem_region(one_region_cap, skt);
    _regions[2] = new mem_region(one_region_cap, skt);
    _active = _regions[0];
    _reclaiming = NULL;
    _spare[0] = _regions[1];
    _spare[1] = _regions[2];
}

void*
region_allocator::allocate(uint64_t size)
{
retry:
    if (void *mem = _active->try_alloc(size))
        return mem;

    _ptr_lock.lock();

    // check if somebody already did it before we acquired the lock
    if (void *mem = _active->try_alloc(size)) {
        _ptr_lock.unlock();
        return mem;
    }

    // do the switch: active -> reclaiming, spare -> active
    if (likely(!_reclaiming)) { // last gc finished, expect two spares
        ASSERT(_spare[0] && _spare[1]);
        _reclaiming = _active;
        _active = _spare[0];    // pick any spare
        // FIXME: signal gc
        std::cout << "region allocator: switched regions." << std::endl;
    }
    // else, last gc might be on-going (or not yet re-assigned ptr),
    // we can spin or abort or retry. For now just retry, so need to do
    // nothing here, just unlock and goto retry;

    _ptr_lock.unlock();
    goto retry;
}

