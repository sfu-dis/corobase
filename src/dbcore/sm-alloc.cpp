#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"
#include "../masstree_btree.h"
#include <future>
#include <new>

// RA GC states. Transitions btw these states are racy
// (should be fine assuming gc finishes before the new
// active region depletes).
#define RA_NORMAL       0
#define RA_GC_REQUESTED 1
#define RA_GC_IN_PROG   2
#define RA_GC_FINISHED  3

class region_allocator {
    friend void RA::epoch_reclaimed(void *cookie, void *epoch_cookie);
    friend void* RA::epoch_ended(void *cookie, epoch_num e);
private:
    enum { NUM_SEGMENT_BITS=2 };
    enum { NUM_SEGMENTS=1<<NUM_SEGMENT_BITS };

    // low-contention and read-mostly stuff
    char *_hot_data;
    char *_cold_data;
    uint64_t _segment_bits;
    uint64_t _hot_bits;
    uint64_t _hot_capacity;
    uint64_t _cold_capacity;
    uint64_t _hot_mask;
    uint64_t _cold_mask;
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
    void* allocate(uint64_t size);
    void* allocate_cold(uint64_t size);
    fat_ptr allocate_fat(uint64_t size);
    region_allocator(uint64_t one_segment_bits, int skt);
    ~region_allocator();
    int state() { return volatile_read(_state); }
    void set_state(int s)   { volatile_write(_state, s); }
    inline void trigger_reclaim()  { _reclaim_cv.notify_all(); }
    static void reclaim_daemon(int socket);
};


namespace RA {
    static const uint64_t PAGE_SIZE_BITS = 16; // Windows uses 64kB pages...
    static const uint64_t MEM_SEGMENT_BITS = 30; // 1GB/segment (16 GB total on 4-socket machine)
    static_assert(MEM_SEGMENT_BITS > PAGE_SIZE_BITS,
                  "Region allocator segments can't be smaller than a page");
    static const uint64_t TRIM_MARK = 1 * 1024 * 1024;

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

    fat_ptr allocate_fat(uint64_t size) {
        auto *myra = tls_ra;
        if (not myra)
            myra = &ra[sched_getcpu() % ra_nsock];
        ASSERT(!system_loading);
        return myra->allocate_fat(size);
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
            if (s == RA_GC_REQUESTED || s == RA_GC_FINISHED) {
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
        //return NULL;
        return epoch_cookie;
        //return thread_cookie;
    }

    void
    epoch_reclaimed(void *cookie, void *epoch_cookie)
    {
        LSN lsn = *(LSN *)epoch_cookie;
        if (lsn != INVALID_LSN)
            trim_lsn = *(LSN *)epoch_cookie;
        free(epoch_cookie);

        for (int i = 0; i < ra_nsock; i++) {
            region_allocator *r = RA::ra + i;
            int s = r->state();
            if (s == RA_GC_REQUESTED) {
                r->set_state(RA_GC_IN_PROG);
                r->trigger_reclaim();
            }
            else if (s == RA_GC_FINISHED) {
                std::cout << "region allocator: spared for socket " << r->_socket << "\n";
                volatile_write(r->_reclaimed_offset,
                    r->_reclaimed_offset + (1 << r->_segment_bits));  // no need to %
                r->set_state(RA_NORMAL);
            }
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
    , _cold_capacity((uint64_t{1} << _segment_bits) * 4)
    , _hot_mask(_hot_capacity - 1)
    , _cold_mask(_cold_capacity - 1)
    , _reclaimed_offset(_hot_capacity)
    , _socket(skt)
    , _allocated_hot_offset(0)
    , _allocated_cold_offset(0)
    , _allocated(0)
    , _state(RA_NORMAL)
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
    //THROW_IF(volatile_read(_reclaimed_offset) < noffset, std::bad_alloc);
    __sync_add_and_fetch(&_allocated, size);

    auto sbits = _segment_bits;
    if (((noffset-1) >> sbits) != ((noffset-size)  >> sbits)) {
        // chunk spans a segment boundary, unusable
        std::cout << "opening segment " << (noffset >> sbits) << " of memory region for socket " << _socket << std::endl;
        if (state() != RA_NORMAL)
            throw std::runtime_error("GC requested before last round finishes.");
        set_state(RA_GC_REQUESTED);
        goto retry;
    }

    if (_allocated >= RA::TRIM_MARK) {
        if (RA::ra_epochs.new_epoch_possible()) {
            if (RA::ra_epochs.new_epoch())
                __sync_add_and_fetch(&_allocated, -_allocated);
        }
    }

    return &_hot_data[(noffset - size) & _hot_mask];
}

fat_ptr
region_allocator::allocate_fat(uint64_t size)
{
    void *mem = allocate(size);
    int seg_nr = ((uintptr_t)mem - (uintptr_t)_hot_data) >> _segment_bits;
    uint64_t seg_id_mask = 0;
    switch (seg_nr) {
    case 1:
        seg_id_mask = fat_ptr::ASI_SEG_LO_FLAG;
        break;
    case 2:
        seg_id_mask = fat_ptr::ASI_SEG_LO_FLAG;
        break;
    case 3:
        seg_id_mask = fat_ptr::ASI_SEG_LO_FLAG | fat_ptr::ASI_SEG_HI_FLAG;
        break;
    }
    return fat_ptr::make(mem, INVALID_SIZE_CODE, seg_id_mask);
}

void*
region_allocator::allocate_cold(uint64_t size)
{
    auto noffset = __sync_add_and_fetch(&_allocated_cold_offset, size);
    if (_cold_capacity < noffset)
        throw std::runtime_error("No enough space in cold store.");
    return &_cold_data[(noffset - size) & _cold_mask];
}

void
region_allocator::reclaim_daemon(int socket)
{
    region_allocator *myra = RA::ra + socket;
    std::unique_lock<std::mutex> lock(myra->_reclaim_mutex);
    uint64_t seg_size = 1 << myra->_segment_bits;
    char __attribute__((aligned(64))) *base_addr = myra->_hot_data;
	uint64_t offset;
    int gc_segment = 0; // the segment that's being gc'ed.

forever:
    myra->_reclaim_cv.wait(lock);
    LSN tlsn = volatile_read(RA::trim_lsn);
    uint64_t cold_head = 0, hot_head = 0, empty_oid = 0;
    uint64_t start_offset = (volatile_read(myra->_reclaimed_offset)) & myra->_hot_mask;
    uint64_t end_offset = start_offset + seg_size;
    ASSERT(!(start_offset & (seg_size - 1)));
    ASSERT(!(end_offset & (seg_size - 1)));
	fat_ptr new_ptr;

    std::cout << "region allocator: start to reclaim for socket "
              << socket << std::endl;

	for (uint i = 0; i < RA::tables.size(); i++) {
		concurrent_btree::tuple_vector_type *v = RA::tables[i]->get_tuple_vector();
		INVARIANT(v);

		// OID group 
		for (uint64_t g = 0; g < v->size() / v->oid_group_sz() + 1; g++) {
			if (!v->is_hot_group(g, gc_segment))
                continue;

            oid_type group_start_oid = g * v->oid_group_sz();
			oid_type group_end_oid = std::min((uint64_t)group_start_oid + v->oid_group_sz(),
                                              (uint64_t)v->size() + 1);

			if( unlikely(group_start_oid == 0) )		// always 0
				group_start_oid++;

            v->set_temperature(group_start_oid, false, gc_segment);
			for( oid_type oid = group_start_oid; oid < group_end_oid; oid++ )
			{
start_over:
				fat_ptr head = v->begin(oid), cur = head;
				fat_ptr *prev_next = v->begin_ptr(oid);
				if (head.offset() == 0 ) {
					empty_oid++;
					continue;
				}

				while (cur.offset() != 0 ) {
					if (!cur._ptr & fat_ptr::ASI_HOT_FLAG)
						break;

					object *cur_obj = (object*)cur.offset(), *new_obj = NULL;
					dbtuple *version = reinterpret_cast<dbtuple *>(cur_obj->payload());
					auto clsn = volatile_read(version->clsn);

					offset = (char *)cur_obj - base_addr;
					if (offset < start_offset || offset + cur_obj->_size > end_offset)
						goto next;

					ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
					ASSERT( not cur_obj->_next.is_dirty() );
					volatile_write( cur_obj->_next._ptr, cur_obj->_next._ptr |fat_ptr::DIRTY_MASK );

					if (LSN::from_ptr(clsn) < tlsn) {
						if (cur == head) {
							new_obj = (object *)myra->allocate_cold(cur_obj->_size);
							memcpy(new_obj, cur_obj, cur_obj->_size);
							new_obj->_next= fat_ptr::make((void*)0, INVALID_SIZE_CODE);
							new_ptr = fat_ptr::make(new_obj, INVALID_SIZE_CODE);
						}   
						else
						{
							new_ptr = fat_ptr::make((void*)0, INVALID_SIZE_CODE);
							if (!__sync_bool_compare_and_swap((uint64_t*)prev_next, cur._ptr, new_ptr._ptr)) {
								volatile_write( cur_obj->_next._ptr, cur_obj->_next._ptr & ~fat_ptr::DIRTY_MASK);
								//cas_failures++;
								goto start_over;
							}
							break;
						}
					}
					else {
						new_obj = (object *)myra->allocate(cur_obj->_size);
						memcpy(new_obj, cur_obj, cur_obj->_size);
						// already hot data
						volatile_write( new_obj->_next._ptr, cur_obj->_next._ptr & ~fat_ptr::DIRTY_MASK);
						new_obj->_next = cur_obj->_next;
						new_ptr = fat_ptr::make(new_obj, INVALID_SIZE_CODE, fat_ptr::ASI_HOT_FLAG);
					}
					// will fail if sb. else claimed prev_next
					if (!__sync_bool_compare_and_swap((uint64_t*)prev_next, cur._ptr, new_ptr._ptr)) {
						volatile_write( cur_obj->_next._ptr, cur_obj->_next._ptr & ~fat_ptr::DIRTY_MASK);
						//cas_failures++;
						goto start_over;
					}
					// !new_obj => trimmed in the middle of the chain;
					// !new_obj->_next => the last element or trimmed at head;
					// so break in either case.
					if (!new_obj || new_obj->_next.offset() == 0 )
						break;
					cur_obj = new_obj;
next:
					prev_next = &cur_obj->_next;
					fat_ptr next = volatile_read( *prev_next );
					volatile_write(cur._ptr, next._ptr &~ fat_ptr::DIRTY_MASK );
				}
			}
		}
	}
    ASSERT(myra->state() == RA_GC_IN_PROG);
    myra->set_state(RA_GC_FINISHED);
    std::cout << "socket " << socket << " empty_oid=" << empty_oid
              << " cold_head=" << cold_head << " hot_head=" << hot_head << "\n";
    gc_segment++;
    gc_segment = gc_segment % region_allocator::NUM_SEGMENTS;
    goto forever;
}

