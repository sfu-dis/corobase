#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"

#include <future>
#include <new>

class region_allocator {
private:
    enum { NUM_SEGMENT_BITS=2 };
    enum { NUM_SEGMENTS=1<<NUM_SEGMENT_BITS };

    // low-contention and read-mostly stuff
    char *_data;
    uint64_t _segment_bits;
    uint64_t _total_bits;
    uint64_t _total_capacity;
    uint64_t _total_mask;
    uint64_t _reclaimed_offset;
    int _socket;

    // high contention, needs its own cache line
    uint64_t __attribute__((aligned(64))) _allocated_offset;

public:
    void* allocate(uint64_t size);
    region_allocator(uint64_t one_segment_bits, int skt);
    ~region_allocator();
};


namespace RA {
    static const uint64_t PAGE_SIZE_BITS = 16; // Windows uses 64kB pages...
    static const uint64_t MEM_SEGMENT_BITS = 30; // 1GB/segment (16 GB total on 4-socket machine)
    static_assert(MEM_SEGMENT_BITS > PAGE_SIZE_BITS,
                  "Region allocator segments can't be smaller than a page");
    
    ra_wrapper ra_w;
    region_allocator *ra;
    int ra_nsock;
    int ra_nthreads;

    __thread region_allocator *tls_ra = 0;

    void init() {
        if (ra_nsock)
            return;
        
        int nodes = numa_max_node() + 1;
        ra = (region_allocator *)malloc(sizeof(region_allocator) * nodes);
        std::future<region_allocator*> futures[nodes];
        for (int i = 0; i < nodes; i++) {
            auto f = [=]{ return new (ra + i) region_allocator(MEM_SEGMENT_BITS, i); };
            futures[i] = std::async(std::launch::async, f);
        }

        // make sure the threads finish before we leave
        for (auto &f : futures)
            (void*) f.get();

        ra_nsock = nodes;
    }

    void register_thread() {
        if (tls_ra)
            return;

        auto rnum = __sync_fetch_and_add(&ra_nthreads, 1);
        auto snum = rnum % ra_nsock;
        numa_run_on_node(snum);
        tls_ra = &ra[snum];
    }
    void *allocate(uint64_t size) {
        auto *myra = tls_ra;
        if (not myra)
            myra = &ra[sched_getcpu() % ra_nsock];
        return myra->allocate(size);
    }
};

region_allocator::region_allocator(uint64_t one_segment_bits, int skt)
    : _segment_bits(one_segment_bits)
    , _total_bits(NUM_SEGMENT_BITS + _segment_bits)
    , _total_capacity(uint64_t{1} << _total_bits)
    , _total_mask(_total_capacity - 1)
    , _reclaimed_offset(_total_capacity)
    , _socket(skt)
    , _allocated_offset(0)
{
#warning Thread that runs region_allocator::region_allocator will be pinned to that socket
    numa_run_on_node(skt);
    _data = (char*) numa_alloc_local(_total_capacity);
    //_data = mmap(NULL, _capacity, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    ASSERT(_data);
    std::cout << "memory region: faulting " << _total_capacity << " bytes"
              << " on node " << skt << std::endl;
    memset(_data, '\0', _total_capacity);
}

region_allocator::~region_allocator()
{
    numa_free(_data, _total_capacity);
}

void*
region_allocator::allocate(uint64_t size)
{
    __builtin_prefetch(&_segment_bits);
    
 retry:
    auto noffset = __sync_add_and_fetch(&_allocated_offset, size);
    THROW_IF(volatile_read(_reclaimed_offset) < noffset, std::bad_alloc);
    
    auto sbits = _segment_bits;
    if (((noffset-1) >> sbits) != ((noffset-size)  >> sbits)) {
        // chunk spans a segment boundary, unusable
        std::cout << "opening segment " << (noffset >> sbits) << " of memory region for socket " << _socket << std::endl;
        goto retry;
    }
    
    return &_data[(noffset - size) & _total_mask];
}

