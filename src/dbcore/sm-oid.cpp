#include "sm-oid-impl.h"

#include "sc-hash.h"
#include "burt-hash.h"

#include <map>

namespace {
#if 0
} // enter namespace, disable autoindent
#endif

struct thread_data {
    /* Use a 64-entry hash table to store OID caches. That may sound
       small for a typical hash table, but occupancy of this
       implementation can achieve 90% or higher occupancy for
       uniformly distributed FIDs.
     */
    static size_t const NENTRIES = 64;
    
    struct hasher : burt_hash {
        using burt_hash::burt_hash;
        using burt_hash::operator();
        uint32_t operator()(sm_allocator::thread_cache const &x) const {
            return operator()(x.f);
        }
    };
    struct cmpeq {
        using tcache = sm_allocator::thread_cache;
        bool operator()(tcache const &a, tcache const &b) const {
            return a.f == b.f;
        }
        bool operator()(tcache const &a, FID const &f) const {
            return a.f == f;
        }
    };
    typedef sc_hash_set<NENTRIES, sm_allocator::thread_cache, hasher, cmpeq> cache_map;
    cache_map caches;

    /* Threads stockpile stacks of free OIDs for files they have
       allocated from in the past. Stockpiled OIDs count as
       "allocated" during forward processing, but it's best to account
       for them during a checkpoint (GC will eventually find and
       recover allocated-but-unused OIDs, but we'd rather not have too
       many OIDs missing in the meantime)

       The checkpoint thread holds the allocator's mutex while copying
       its state, and holds the thread's mutex while scanning through
       the stockpiled OIDs at various threads to update the
       allocator's snapshot. This means the user thread can allocate
       and free OIDs while the scan is in progress (we rely on GC to
       reclaim any OIDs that go missing as a result).
       
       The user must grab a mutex to insert or remove FID caches from
       the set, though. Otherwise all kinds of "fun" things could
       happen. Like an insertion moving an existing entry so the
       checkpoint scan misses it. Or an eviction freeing a pile of
       OIDs that the checkpoint thread never learns about.
    */
    os_mutex mutex;
};

__thread thread_data *tls;

/* Used to make sure threads give back their caches on exit */
os_mutex_pod oid_mutex = os_mutex_pod::static_init();
sm_oid_mgr_impl *master;
std::map<pthread_t, thread_data*> *threads;
pthread_key_t pthread_key;

/* Wipe out all caches held by the calling thread, returning any OIDs
   they contain to the owning allocators. Separate the cache-draining
   from the cache-destroying so we only hold one latch at a time.
   Otherwise we risk deadlock: we would acquire the thread mutex
   before the allocator bucket mutexes, while a checkpoint thread
   would tend to do the opposite.
 */
void
thread_revoke_caches(sm_oid_mgr_impl *om) {
    for (auto it=tls->caches.begin(); it != tls->caches.end(); ++it) {
        om->lock_file(it->f);
        DEFER(om->unlock_file(it->f));
        auto *alloc = om->get_allocator(it->f);
        alloc->drain_cache(&*it);
    }

    // now nuke the htab
    tls->mutex.lock();
    DEFER(tls->mutex.unlock());
    tls->caches.clear();
}

void thread_fini(sm_oid_mgr_impl *om) {
    auto tid = pthread_self();
    thread_revoke_caches(om);
    
    oid_mutex.lock();
    DEFER(oid_mutex.unlock());

    threads->erase(tid);
}

void thread_init() {
    auto tid = pthread_self();
    auto tmp = new thread_data{};
    DEFER_UNLESS(success, delete tmp);
    
    oid_mutex.lock();
    DEFER(oid_mutex.unlock());

    threads->emplace(tid, tmp);

    int err = pthread_setspecific(pthread_key, tmp);
    THROW_IF(err, os_error, errno, "pthread_setspecific failed");
    success = true;
    
    tls = tmp;
}

/* Find (or create, if necessary) the thread-local cache for the
   requested FID
 */
thread_data::cache_map::iterator
thread_cache(sm_oid_mgr_impl *om, FID f)
{
    if (not tls)
        thread_init();

    /* Attempt to insert [f] into the cache. If present, return the
       corresponding cache. If not present, create a new cache and
       return it. If the cache is full, we'll find that out, too.
     */
    auto rval = tls->caches.find_and_emplace(f, f);
    if (rval.second == -1) {
        /* Not present, cache full. Time to clean house. We could get
           fancy and remove entries strategically... or we can nuke it
           and start over. Overflow should be rare with most
           workloads, so we go with the latter for now.
         */
        thread_revoke_caches(om);
        rval = tls->caches.find_and_emplace(f, f);
        ASSERT(not rval.second);
    }

    return rval.first;
}

OID
thread_allocate(sm_oid_mgr_impl *om, FID f)
{
    // correct cache entry definitely exists now... but may be empty
    auto it = thread_cache(om, f);
    ASSERT(it->f == f);
    if (not it->nentries) {
        om->lock_file(f);
        DEFER(om->unlock_file(f));
        auto *alloc = om->get_allocator(f);
        if (not alloc->fill_cache(&*it)) {
            auto cbump = alloc->propose_capacity(1);
            sm_oid_mgr_impl::oid_array *oa = om->get_array(f);
            oa->ensure_size(cbump);
            alloc->head.capacity_mark = cbump;
            alloc->fill_cache(&*it);
        }
        ASSERT(it->nentries);
    }
    return it->entries[--it->nentries];
}

void
thread_free(sm_oid_mgr_impl *om, FID f, OID o)
{
    // cache entry exists, but may be full
    auto it = thread_cache(om, f);
    if (not it->space_remaining()) {
        om->lock_file(it->f); 
        DEFER(om->unlock_file(it->f));
        auto *alloc = om->get_allocator(it->f);
        alloc->drain_cache(&*it);
        ASSERT(it->space_remaining());
    }

    it->entries[it->nentries++] = o;
}

# if 0
{ // exit namespace, disable autoindent
#endif
}


fat_ptr
sm_oid_mgr_impl::oid_array::make() {
    /* Ask for a dynarray with size 1 byte, which gets rounded up to
       one page.
     */
    dynarray d = make_oid_dynarray();
    void *ptr = d.data();
    auto *rval = new (ptr) oid_array(std::move(d));
    return fat_ptr::make(rval, 1);
}

void
sm_oid_mgr_impl::oid_array::destroy(oid_array *oa) {
    oa->~oid_array();
}

sm_oid_mgr_impl::oid_array::oid_array(dynarray &&self)
    : _backing_store(std::move(self))
{
    ASSERT(this == (void*) _backing_store.data());
}

void
sm_oid_mgr_impl::oid_array::ensure_size(size_t n) {
    _backing_store.ensure_size(OFFSETOF(oid_array, _entries[n]));
}
                                        
sm_oid_mgr_impl::sm_oid_mgr_impl()
{
    /* Bootstrap the OBJARRAY, which contains everything (including
       itself). Then seed it with OID arrays for allocators and
       metadata
     */
#warning TODO DEFER deletion of these arrays if constructor throws
    fat_ptr ptr = oid_array::make();
    files = ptr;
    *files->get(OBJARRAY_FID) = ptr;
    ASSERT(oid_get(OBJARRAY_FID, OBJARRAY_FID) == ptr);
    oid_put(OBJARRAY_FID, ALLOCATOR_FID, oid_array::make());
    oid_put(OBJARRAY_FID, METADATA_FID, oid_array::make());

    /* Instantiate the FID allocator and pre-allocate internal files.
     */
    auto *alloc = sm_allocator::make();
    alloc->head.hiwater_mark = FIRST_FREE_FID;
    auto cap = alloc->head.capacity_mark;
    oid_array *oa = ptr;
    oa->ensure_size(cap);
    auto p = fat_ptr::make(alloc, 1);
    oid_put(ALLOCATOR_FID, OBJARRAY_FID, p);
    ASSERT(get_allocator(OBJARRAY_FID) == p);

    // initialize (or reclaim) thread-local machinery
    oid_mutex.lock();
    DEFER(oid_mutex.unlock());
    DIE_IF(master, "Multiple OID managers found.");
    if (not threads) {
        auto fini = [](void *arg)->void {
            auto *om = (sm_oid_mgr_impl*) arg;
            if (om == master)
                thread_fini(om);
        };

        threads = make_new();
        
        int err = pthread_key_create(&pthread_key, fini);
        DIE_IF(err, "pthread_key_create failed with errno=%d", errno);
    }
    master = this;
}

sm_oid_mgr_impl::~sm_oid_mgr_impl()
{
    oid_mutex.lock();
    DEFER(oid_mutex.unlock());

    master = NULL;
    
    /* Nuke the contents of any lingering thread-local caches for this
       FID. Don't remove the entry because that would require grabbing
       more mutexes.
    */

    for(auto &kv : *threads) {
        auto *tdata = kv.second;
        tdata->mutex.lock();
        DEFER(tdata->mutex.unlock());
        for (auto &tc : tdata->caches) 
            tc.nentries = 0;
    }
}

FID
sm_oid_mgr_impl::create_file(bool needs_alloc)
{
    /* Let the thread-local allocator choose an FID; with that in
       hand, we create the corresponding OID array and allocator.
     */
    auto f = thread_allocate(this, OBJARRAY_FID);
    auto ptr = oid_array::make();
    oid_put(OBJARRAY_FID, f, ptr);
    if (needs_alloc) {
        auto *alloc = sm_allocator::make();
        auto p = fat_ptr::make(alloc, 1);
        oid_put(ALLOCATOR_FID, f, p);
        
        auto cap = alloc->head.capacity_mark;
        oid_array *oa = ptr;
        oa->ensure_size(cap);
    }
    return f;
}

void
sm_oid_mgr_impl::destroy_file(FID f)
{
    /* As with most resources, a file should only be reclaimed once it
       has not only been deleted logically, but also become
       unreachable by any in-flight transaction. At that point, only
       GC will see it and so there is no need for low-level CC, other
       than avoiding structural hazards in the allocator that manages
       FIDs.
     */
    {
        /* Nuke the contents of any lingering thread-local caches for
           this FID. Don't remove the entry because that would require
           grabbing more mutexes.
         */
        oid_mutex.lock();
        DEFER(oid_mutex.unlock());

        for(auto &kv : *threads) {
            auto *tdata = kv.second;
            tdata->mutex.lock();
            DEFER(tdata->mutex.unlock());
            auto it = tdata->caches.find(f);
            if (it != tdata->caches.end())
                it->nentries = 0;
        }
    }        
    {
        fat_ptr *ptr = oid_access(OBJARRAY_FID, f);
        oid_array *oa = *ptr;
        ASSERT(oa);
        oid_array::destroy(oa);
        *ptr = NULL_PTR;
    }
    {
        fat_ptr *ptr = oid_access(ALLOCATOR_FID, f);
        sm_allocator *alloc = *ptr;
        if (alloc) {
            sm_allocator::destroy(alloc);
            *ptr = NULL_PTR;
        }
    }
#warning TODO: delete metadata? or force caller to do it before now?

    // one allocator controls all three files
    thread_free(this, OBJARRAY_FID, f);
}

fat_ptr*
sm_oid_mgr_impl::oid_access(FID f, OID o)
{
    auto *oa = get_array(f);
    return oa->get(o);
}

sm_allocator*
sm_oid_mgr_impl::get_allocator(FID f)
{
#warning TODO: allow allocators to be paged out
    sm_allocator *alloc = oid_get(ALLOCATOR_FID, f);
    THROW_IF(not alloc, illegal_argument,
             "No allocator for FID %d", f);
    return alloc;
}

sm_oid_mgr_impl::oid_array*
sm_oid_mgr_impl::get_array(FID f)
{
#warning TODO: allow allocators to be paged out
    sm_oid_mgr_impl::oid_array *oa = *files->get(f);
    THROW_IF(not oa, illegal_argument,
             "No such file: %d", f);
    return oa;
}

void
sm_oid_mgr_impl::lock_file(FID f)
{
    mutexen[f % MUTEX_COUNT].lock();
}
void
sm_oid_mgr_impl::unlock_file(FID f)
{
    mutexen[f % MUTEX_COUNT].unlock();
}


sm_oid_mgr *
sm_oid_mgr::create(sm_heap_mgr *hm, log_tx_scan *chkpt_scan)
{
#warning TODO: use the scan to recover
    return new sm_oid_mgr_impl{};
}

void
sm_oid_mgr::log_chkpt(sm_heap_mgr *hm, sm_tx_log *tx)
{
#warning TODO: take checkpoint
}

FID
sm_oid_mgr::create_file(bool needs_alloc)
{
    return get_impl(this)->create_file(needs_alloc);
}


void
sm_oid_mgr::destroy_file(FID f)
{
    get_impl(this)->destroy_file(f);
}

OID
sm_oid_mgr::alloc_oid(FID f)
{
    return thread_allocate(get_impl(this), f);
}

fat_ptr
sm_oid_mgr::free_oid(FID f, OID o)
{
    auto *self = get_impl(this);
    auto *ptr = self->oid_access(f, o);
    auto rval = *ptr;
    *ptr = NULL_PTR;
    thread_free(self, f, o);
    return rval;
}

fat_ptr
sm_oid_mgr::oid_get(FID f, OID o)
{
    return *get_impl(this)->oid_access(f, o);
}

void
sm_oid_mgr::oid_put(FID f, OID o, fat_ptr p)
{
    auto *ptr = get_impl(this)->oid_access(f, o);
    *ptr = p;
}
 
