#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"
#include "../masstree_btree.h"

namespace RA {
    static const uint64_t PAGE_SIZE_BITS = 16; // Windows uses 64kB pages...
    static const uint64_t MEM_SEGMENT_BITS = 30; // 1GB/segment (16 GB total on 4-socket machine)
    static_assert(MEM_SEGMENT_BITS > PAGE_SIZE_BITS,
                  "Region allocator segments can't be smaller than a page");
    static const uint64_t TRIM_MARK = 4 * 1024 * 1024;

    std::vector<concurrent_btree*> tables;
    std::vector<std::string> table_names;
    ra_wrapper ra_w;
    region_allocator *ra;
    int ra_nsock;
    int ra_nthreads;
    LSN trim_lsn;
    bool system_loading;
    __thread region_allocator *tls_ra = 0;

    void register_table(concurrent_btree *t, std::string name) {
        tables.push_back(t);
        table_names.push_back(name);
    }

    void init() {
        if (ra_nsock)
            return;
        
        trim_lsn = INVALID_LSN;
        system_loading = true;
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
        if (likely(!system_loading))
            return myra->allocate(size);
        return myra->allocate_cold(size);
    }

    void *allocate_cold(uint64_t size) {
        auto *myra = tls_ra;
        if (not myra)
            myra = &ra[sched_getcpu() % ra_nsock];
        return myra->allocate_cold(size);
    }

    void allocate_fat(fat_ptr *ptr, uint64_t *seg, int *sock, uint64_t size) {
        auto *myra = tls_ra;
        if (not myra)
            myra = &ra[sched_getcpu() % ra_nsock];
        ASSERT(!system_loading);
        return myra->allocate_fat(ptr, seg, sock, size);
    }

    // epochs related
    __thread struct thread_data epoch_tls;
    epoch_mgr ra_epochs {{nullptr, &global_init, &get_tls,
                        &thread_registered, &thread_deregistered,
                        &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

    // epoch mgr callbacks
    epoch_mgr::tls_storage *
    get_tls(void*)
    {
        static __thread epoch_mgr::tls_storage s;
        return &s;
    }

    void global_init(void*)
    {
    }

    void*
    thread_registered(void*)
    {
        epoch_tls.initialized = true;
        return &epoch_tls;
    }

    void
    thread_deregistered(void *cookie, void *thread_cookie)
    {
        auto *t = (thread_data*) thread_cookie;
        ASSERT(t == &epoch_tls);
        t->initialized = false;
    }

    void*
    epoch_ended(void *cookie, epoch_num e)
    {
        // So we need this rcu_is_active here because
        // epoch_ended is called not only when an epoch is eneded,
        // but also when threads exit (see epoch.cpp:274-283 in function
        // epoch_mgr::thread_init(). So we need to avoid the latter case
        // as when thread exits it will no longer be in the rcu region
        // created by the scoped_rcu_region in the transaction class.
        for (int i = 0; i < ra_nsock; i++) {
            region_allocator *r = RA::ra + i;
            int s = r->state();
            if (s == RA_GC_REQUESTED || s == RA_GC_PREPARED || s == RA_GC_FINISHED) {
                LSN *lsn = (LSN *)malloc(sizeof(LSN));
                if (likely(RCU::rcu_is_active()))
                    *lsn = transaction_base::logger->cur_lsn();
                else
                    *lsn = INVALID_LSN;
                return lsn;
            }
        }
        return NULL;
    }

    void*
    epoch_ended_thread(void *cookie, void *epoch_cookie, void *thread_cookie)
    {
        return epoch_cookie;
    }

    void
    epoch_reclaimed(void *cookie, void *epoch_cookie)
    {
        LSN lsn = *(LSN *)epoch_cookie;
        if (lsn != INVALID_LSN)
            trim_lsn = *(LSN *)epoch_cookie;
        free(epoch_cookie);

        // setting the state should be atomic, as two allocators might trigger
        // this function at the **same** time (note this is called when all
        // stragglers left) and peek at others' state.
        for (int i = 0; i < ra_nsock; i++) {
            region_allocator *r = RA::ra + i;
            int s = r->state();
            if (s == RA_GC_PREPARED) {
                if (r->try_set_state(RA_GC_PREPARED, RA_GC_IN_PROG))
                    r->trigger_reclaim();
            }
            else if (s == RA_GC_REQUESTED) {
                if (r->try_set_state(RA_GC_REQUESTED, RA_GC_PREPARED))
                    std::cout << "socket " << r->_socket << ": GC requested\n";
            }
            else if (s == RA_GC_FINISHED) {
                r->try_set_state(RA_GC_FINISHED, RA_GC_SPARING);
            }
            else if (s == RA_GC_SPARING) {
#if CHECK_INVARIANTS
                    // wait one more epoch??
                    uint64_t seg_size = 1 << r->_segment_bits;
                    memset(&r->_hot_data[r->_gc_segment * seg_size], '\0', seg_size);
#endif
                    uint64_t curr_offset = volatile_read(r->_reclaimed_offset);
                    DIE_IF(!__sync_bool_compare_and_swap(&r->_reclaimed_offset,
                            curr_offset, curr_offset + (1 << r->_segment_bits)),
                            "sparing for socket %d failed\n", i);
                    DIE_IF(!r->try_set_state(RA_GC_SPARING, RA_NORMAL),
                            "socket %d: state transition failed: RA_GC_SPARING -> RA_NORMAL\n", i);
                    std::cout << "socket " << r->_socket << ": segment spared\n";
            }
#if CHECK_INVARIANTS
            else {
                ASSERT(s == RA_NORMAL || s == RA_GC_IN_PROG);
            }
#endif
        }
    }

    void
    epoch_enter(void)
    {
        if (!epoch_tls.initialized) {
            ra_epochs.thread_init();
        }
        ra_epochs.thread_enter();
    }

    void
    epoch_exit(void)
    {
        ra_epochs.thread_quiesce();
        ra_epochs.thread_exit();
    }

    void
    epoch_thread_quiesce(void)
    {
        ra_epochs.thread_quiesce();
    }
};

region_allocator::region_allocator(uint64_t one_segment_bits, int skt)
    : _segment_bits(one_segment_bits)
    , _hot_bits(NUM_SEGMENT_BITS + _segment_bits)
    , _hot_capacity(uint64_t{1} << _hot_bits)
    , _cold_capacity((uint64_t{1} << _segment_bits) * 6)
    , _hot_mask(_hot_capacity - 1)
    , _reclaimed_offset(_hot_capacity)
    , _socket(skt)
    , _allocated_hot_offset(0)
    , _allocated_cold_offset(0)
    , _allocated(0)
    , _state(RA_NORMAL)
    , _gc_segment(-1)
{
#warning Thread that runs region_allocator::region_allocator will be pinned to that socket
    numa_run_on_node(skt);
    _hot_data = (char*) numa_alloc_local(_hot_capacity);
    _cold_data = (char*) numa_alloc_local(_cold_capacity);
    ASSERT(_hot_data);
    std::cout << "memory region: faulting " << _hot_capacity << " bytes"
              << " on node " << skt << std::endl;
    memset(_hot_data, '\0', _hot_capacity);
    memset(_cold_data, '\0', _cold_capacity);
    std::thread reclaim_thd(reclaim_daemon, skt);
    reclaim_thd.detach();
}

region_allocator::~region_allocator()
{
    numa_free(_hot_data, _hot_capacity);
    numa_free(_cold_data, _cold_capacity);
}

void*
region_allocator::allocate(uint64_t size)
{
    __builtin_prefetch(&_segment_bits);
    
 retry:
    auto noffset = __sync_add_and_fetch(&_allocated_hot_offset, size);
    THROW_IF(volatile_read(_reclaimed_offset) < noffset, std::bad_alloc);
    __sync_add_and_fetch(&_allocated, size);

    auto sbits = _segment_bits;
    ASSERT(size <= ((uint64_t{1} << sbits)));
    if (((noffset-1) >> sbits) != ((noffset-size)  >> sbits)) {
        // chunk spans a segment boundary, unusable
        std::cout << "socket " << _socket << ": opening segment " << (noffset >> sbits) << "\n";
        if (state() != RA_NORMAL)
            throw std::runtime_error("socket %d: GC requested before last round finishes.");
        DIE_IF(!try_set_state(RA_NORMAL, RA_GC_REQUESTED),
                "socket %d: state transition failed: RA_GC_REQUESTED\n", _socket);
        goto retry;
    }

    ASSERT((noffset-1) >> sbits != (uint64_t)_gc_segment);

    if (_allocated >= RA::TRIM_MARK) {
        if (RA::ra_epochs.new_epoch_possible()) {
            if (RA::ra_epochs.new_epoch())
                __sync_add_and_fetch(&_allocated, -_allocated);
        }
    }

    return &_hot_data[(noffset - size) & _hot_mask];
}

void
region_allocator::allocate_fat(fat_ptr *ptr, uint64_t *seg, int *sock, uint64_t size)
{
    allocate_fat(ptr, size);
    *seg = (ptr->offset() - (uint64_t)_hot_data) >> _segment_bits;
    *sock = _socket;
}

void
region_allocator::allocate_fat(fat_ptr *ptr, uint64_t size)
{
    void *mem = allocate(size);
    *ptr = fat_ptr::make(mem, INVALID_SIZE_CODE, fat_ptr::ASI_HOT_FLAG);
}

void*
region_allocator::allocate_cold(uint64_t size)
{
    auto noffset = __sync_add_and_fetch(&_allocated_cold_offset, size);
    if (_cold_capacity < noffset)
        throw std::runtime_error("No enough space in cold store.");
    return &_cold_data[(noffset - size)];
}

bool
region_allocator::try_set_state(int from, int to)
{
    return __sync_bool_compare_and_swap(&_state, from, to);
}

void
region_allocator::reclaim_daemon(int socket)
{
    region_allocator *myra = RA::ra + socket;
    std::unique_lock<std::mutex> lock(myra->_reclaim_mutex);
    uint64_t seg_size = 1 << myra->_segment_bits;
    char __attribute__((aligned(64))) *base_addr = myra->_hot_data;

forever:
    myra->_reclaim_cv.wait(lock);
    LSN tlsn = volatile_read(RA::trim_lsn);
    volatile_write(myra->_gc_segment, (myra->_gc_segment + 1) % 4);
    uint64_t start_offset = myra->_gc_segment * seg_size;
    uint64_t end_offset = start_offset + seg_size;
    ASSERT(!(start_offset & (seg_size - 1)));
    ASSERT(!(end_offset & (seg_size - 1)));

    std::cout << "socket " << socket << ": start to reclaim\n";

    for (uint i = 0; i < RA::tables.size(); i++) {
        concurrent_btree *t = RA::tables[i];
        concurrent_btree::tuple_vector_type *v = t->get_tuple_vector();
        INVARIANT(v);
        uint64_t total_oids = v->size();
        oid_type oid = 0;

        while (oid < total_oids) {
            uint64_t bitmap_off = oid / v->_oids_per_byte / sizeof(uint64_t) * sizeof(uint64_t);
            uint64_t *bitmap = v->bitmap_ptr(socket, myra->_gc_segment, bitmap_off);
            bool eot = false;
            while (oid < total_oids && !(*(bitmap++))) {
#if CHECK_INVARIANTS
                for (uint o = 0; o < v->_oids_per_bit * 64; o++) {
                    if (oid + o < total_oids) {
                        uint64_t off = v->begin(oid + o).offset();
                        uint64_t offset = off - (uint64_t)base_addr;
                        ASSERT(offset < start_offset || offset > end_offset);
                    }
                }
#endif
                oid += 64 * v->_oids_per_bit;
                if (oid >= total_oids) {
                    eot = true;
                    break;
                }
            }
            if (eot)    // end of table
                break;

            bitmap--;
            volatile_write(*bitmap, 0);
            oid_type start_oid = oid;
            for (; oid < start_oid + v->_oids_per_bit * 64; oid++) {
                if (oid >= total_oids)
                    break;
                if (unlikely(oid == 0))
                    continue;
start_over:
                fat_ptr head = v->begin(oid), cur = head;
                fat_ptr *prev_next = v->begin_ptr(oid);
                if (!head.offset() || !(head._ptr & fat_ptr::ASI_HOT_FLAG))
                    continue;

                while (cur.offset()) {
                    object *new_obj = NULL;
                    object *curr_obj = (object *)cur.offset();
                    dbtuple *version = reinterpret_cast<dbtuple *>(curr_obj->payload());
                    auto clsn = volatile_read(version->clsn);
                    fat_ptr new_ptr = NULL_PTR;
                    uint64_t offset = cur.offset() - (uint64_t)base_addr;
                    if (offset < start_offset || offset + curr_obj->_size > end_offset)
                        goto next;

                    ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
                    volatile_write(curr_obj->_next._ptr,
                                   curr_obj->_next._ptr | fat_ptr::DIRTY_MASK);
                    new_ptr = NULL_PTR;
                    if (LSN::from_ptr(clsn) < tlsn) {
                        if (cur.offset() == head.offset()) {
                            new_obj = (object *)myra->allocate_cold(curr_obj->_size);
                            memcpy(new_obj, curr_obj, curr_obj->_size);
                            new_obj->_next = NULL_PTR;
                            new_ptr = fat_ptr::make(new_obj, INVALID_SIZE_CODE);
                        }   // else new_obj = NULL
                    }
                    else {
                        new_obj = (object *)myra->allocate(curr_obj->_size);
                        memcpy(new_obj, curr_obj, curr_obj->_size);
                        volatile_write(new_obj->_next._ptr,
                                       curr_obj->_next._ptr & (~fat_ptr::DIRTY_MASK));
                    }
                    // will fail if sb. else claimed prev_next
                    if (!__sync_bool_compare_and_swap(
                            &prev_next->_ptr, cur._ptr, new_ptr._ptr)) {
                        volatile_write(curr_obj->_next._ptr,
                                curr_obj->_next._ptr & ~fat_ptr::DIRTY_MASK);
                        goto start_over;
                    }

                    // set group's bit
                    if (new_ptr._ptr & fat_ptr::ASI_HOT_FLAG)
                        volatile_write(*bitmap, 1);//(*bitmap) |= ((uint64_t){1} << group_bit));

                    // !new_obj => trimmed in the middle of the chain;
                    // !new_obj->_next => the last element or trimmed at head;
                    // so break in either case.
                    if (!new_obj || !new_obj->_next.offset())
                        break;
                    curr_obj = new_obj;
    next:
                    prev_next = &curr_obj->_next;
                    volatile_write(cur._ptr, prev_next->_ptr & (~fat_ptr::DIRTY_MASK));
                }
            }
        }
    }
    ASSERT(myra->state() == RA_GC_IN_PROG);
    myra->try_set_state(RA_GC_IN_PROG, RA_GC_FINISHED);
    std::cout << "socket " << socket << ": GC finished\n";

    goto forever;
}

