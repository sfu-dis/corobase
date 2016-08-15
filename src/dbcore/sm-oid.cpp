#include <fcntl.h>
#include <unistd.h>

#include <map>

#include "../util.h"
#include "../txn.h"

#include "burt-hash.h"
#include "sc-hash.h"
#include "sm-alloc.h"
#include "sm-chkpt.h"
#include "sm-config.h"
#include "sm-file.h"
#include "sm-log-recover-impl.h"
#include "sm-oid-impl.h"

sm_oid_mgr *oidmgr = NULL;

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
            oid_array *oa = om->get_array(f);
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
oid_array::make() {
    /* Ask for a dynarray with size 1 byte, which gets rounded up to
       one page.
     */
    dynarray d = make_oid_dynarray();
    void *ptr = d.data();
    auto *rval = new (ptr) oid_array(std::move(d));
    return fat_ptr::make(rval, 1);
}

void
oid_array::destroy(oid_array *oa) {
    oa->~oid_array();
}

oid_array::oid_array(dynarray &&self)
    : _backing_store(std::move(self))
{
    ASSERT(this == (void*) _backing_store.data());
}

void
oid_array::ensure_size(size_t n) {
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
    ASSERT(not file_exists(f));
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

/* Create a file with given FID f.
 * WARNING: this is for recovery use only; there's no CC for it.
 * Caller has full responsibility.
 */
void
sm_oid_mgr_impl::recreate_file(FID f)
{
    ASSERT(not file_exists(f));
    auto ptr = oid_array::make();
    oid_put(OBJARRAY_FID, f, ptr);
    ASSERT(file_exists(f));
    // Allocator doesn't exist for now, need to call
    // recreate_allocator(f) later after we figured
    // out the high watermark by scanning the log.
}

/* Create the allocator for a given FID f.
 * This also bootstraps the OID array that contains FIDs.
 * WARNING: for recovery only; no CC for it.
 * Caller has full responsibility.
 */
void
sm_oid_mgr_impl::recreate_allocator(FID f, OID m)
{
    static std::mutex recreate_lock;

    recreate_lock.lock();
    DEFER(recreate_lock.unlock());

    // Callers might be
    // 1. oidmgr when recovering from a chkpt
    // 2. logmgr when recovering from the log
    // Whomever calls later will need to avoid allocating new allocators
    sm_allocator *alloc = (sm_allocator *)oid_get(ALLOCATOR_FID, f).offset();
    if (not alloc) {
        alloc = sm_allocator::make();
        auto p = fat_ptr::make(alloc, 1);
#ifndef NDEBUG
        // Special case: internal files are already initialized
        if (f != ALLOCATOR_FID and f != OBJARRAY_FID and f != METADATA_FID)
            ASSERT(oid_get(ALLOCATOR_FID, f) == NULL_PTR);
#endif
        oid_put(ALLOCATOR_FID, f, p);
    }
    else {
        // else the oidmgr should already recreated this allocator for f
        // when recovering from a chkpt file, then if we're entering here
        // that means we got some newer himark by scanning the log.
        // But note this new himark might be larger than the one we got
        // from the chkpt: this is the one that really got allocated, while
        // the one we got from chkpt was the real *himark* at runtime -
        // not all of the OIDs below it were allocated,  so chances are we
        // will still find the existing mark is high enough already, unless
        // after the chkpt the OID cache was depleted and we allocated after
        // refilling it.
    }

    // if m == 0, then the table hasn't been inserted, no need to mess with it
    if (not m)
        return;

    if (m < alloc->head.capacity_mark and m < alloc->head.hiwater_mark)
        return;

    // Set capacity_mark = hiwater_mark = m, the cache machinery
    // will do the rest (note the +64 to avoid duplicates)
    alloc->head.capacity_mark = alloc->head.hiwater_mark = (m + 64);
    ASSERT(file_exists(f));
    fat_ptr ptr = oid_get(OBJARRAY_FID, f);
    oid_array *oa = ptr;
    ASSERT(oa);
    oa->ensure_size(alloc->head.capacity_mark);
    printf("[Recovery] recreate allocator %d, himark=%d\n", f, m);
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

oid_array*
sm_oid_mgr_impl::get_array(FID f)
{
#warning TODO: allow allocators to be paged out
    oid_array *oa = *files->get(f);
    THROW_IF(not oa, illegal_argument,
             "No such file: %d", f);
    return oa;
}

bool
sm_oid_mgr_impl::file_exists(FID f)
{
    oid_array *oa = *files->get(f);
    return oa != NULL;
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

oid_array*
sm_oid_mgr::get_array(FID f)
{
    return get_impl(this)->get_array(f);
}

void
sm_oid_mgr::create(LSN chkpt_start, sm_log_recover_mgr *lm)
{
    // Create an empty oidmgr, with initial internal files
    oidmgr = new sm_oid_mgr_impl{};
    oidmgr->dfd = dirent_iterator(sysconf::log_dir.c_str()).dup();
    chkptmgr = new sm_chkpt_mgr(chkpt_start);

    if (not sm_log::need_recovery or chkpt_start.offset() == 0)
        return;

    // Find the chkpt file and recover from there
    char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
    size_t n = os_snprintf(buf, sizeof(buf),
                           CHKPT_DATA_FILE_NAME_FMT, chkpt_start._val);
    printf("[Recovery.chkpt] %s\n", buf);
    ASSERT(n < sizeof(buf));
    int fd = os_openat(oidmgr->dfd, buf, O_RDONLY);

    while (1) {
        // Read himark
        OID himark = 0;
        n = read(fd, &himark, sizeof(OID));
        if (not n)  // EOF
            break;

        ASSERT(n == sizeof(OID));

        // Read the table's name
        size_t len = 0;
        n = read(fd, &len, sizeof(size_t));
        THROW_IF(n != sizeof(size_t), illegal_argument,
                 "Error reading tabel name length");
        char name_buf[256];
        n = read(fd, name_buf, len);
        std::string name(name_buf, len);

        // FID
        FID f = 0;
        n = read(fd, &f, sizeof(FID));

        // Recover fid_map and recreate the empty file
        ASSERT(sm_file_mgr::get_index(name));
        sm_file_mgr::name_map[name]->fid = f;
        sm_file_mgr::fid_map[f] = new sm_file_descriptor(f, name, sm_file_mgr::get_index(name));
        ASSERT(not oidmgr->file_exists(f));
        oidmgr->recreate_file(f);
        printf("[Recovery.chkpt] FID=%d %s\n", f, name.c_str());

        // Recover allocator status
        oid_array *oa = oidmgr->get_array(f);
        oa->ensure_size(oa->alloc_size(himark));
        oidmgr->recreate_allocator(f, himark);

        // Populate the OID array
        while (1) {
            OID o = 0;
            n = read(fd, &o, sizeof(OID));
            if (o == himark)
                break;

            fat_ptr ptr = NULL_PTR;
            n = read(fd, &ptr, sizeof(fat_ptr));
            if (sysconf::eager_warm_up()) {
                ptr = object::create_tuple_object(ptr, NULL_PTR, 0, lm);
                ASSERT(ptr.asi_type() == 0);
            }
            else {
                object *obj = new (MM::allocate(sizeof(object), 0)) object(ptr, NULL_PTR, 0);
                ptr = fat_ptr::make(obj, INVALID_SIZE_CODE, fat_ptr::ASI_LOG_FLAG);
                ASSERT(ptr.asi_type() == fat_ptr::ASI_LOG);
            }
            oidmgr->oid_put_new(f, o, ptr);
        }
    }
}

void
sm_oid_mgr::take_chkpt(LSN cstart)
{
    // Now the real work. The format of a chkpt file is:
    // [table 1 himark]
    // [table 1 name length, table 1 name, table 1 FID]
    // [OID1, ptr for table 1]
    // [OID2, ptr for table 1]
    // [table 1 himark]
    // ...
    // [table 2 himark]
    // [table 2 name length, table 2 name, table 2 FID]
    // [OID1, ptr for table 2]
    // [OID2, ptr for table 2]
    // [table 2 himark]
    // ...
    //
    // The table's himark denotes its start and end

    for (auto &fm : sm_file_mgr::fid_map) {
        auto* fd = fm.second;
        FID fid = fd->fid;
        // Find the high watermark of this file and dump its
        // backing store up to the size of the high watermark
        auto *alloc = get_impl(this)->get_allocator(fid);
        OID himark = alloc->head.hiwater_mark;

        // Write the himark to denote a table start
        chkptmgr->write_buffer(&himark, sizeof(OID));

        // [Name length, name, FID]
        size_t len = fd->name.length();
        chkptmgr->write_buffer(&len, sizeof(size_t));
        chkptmgr->write_buffer((void *)fd->name.c_str(), len);
        chkptmgr->write_buffer(&fd->fid, sizeof(FID));

        // Now write the [OID, ptr] pairs
        for (OID oid = 0; oid < himark; oid++) {
            auto ptr = oid_get(fid, oid);
        find_pdest:
            if (not ptr.offset())
                continue;
            object *obj = (object *)ptr.offset();
            auto pdest = volatile_read(obj->_pdest);
            // Three cases:
            // 1. Tuple is in memory, not in storage (or doesn't have a valid
            //    pdest yet). Skip and continue to look at an older version
            //    which should be committed or nothing
            // 2. Tuple is in memory and in storage (has a valid pdest)
            // 3. Tuple is not in memory but in storage
            //    For both 2 and 3, use the object's _pdest directly
            if (pdest.asi_type() != fat_ptr::ASI_LOG or LSN::from_ptr(pdest) >= cstart) {
                ptr = volatile_read(obj->_next);
                goto find_pdest;
            }

            ASSERT(pdest.offset() and pdest.asi_type() == fat_ptr::ASI_LOG);
            // Now write this out
            // XXX (tzwang): perhaps we'll also need obj._next later?
            chkptmgr->write_buffer(&oid, sizeof(OID));
            chkptmgr->write_buffer(&pdest, sizeof(fat_ptr));
        }
        // Write himark as end of fid
        chkptmgr->write_buffer(&himark, sizeof(OID));
        std::cout << "[Checkpoint] FID(" << fd->fid << ") = "
                  << fid << ", himark = " << himark << std::endl;
    }

    chkptmgr->sync_buffer();
}

sm_allocator*
sm_oid_mgr::get_allocator(FID f)
{
    return get_impl(this)->get_allocator(f);
}

void
sm_oid_mgr::start_warm_up()
{
    std::thread t(sm_oid_mgr::warm_up);
    t.detach();
}

void
sm_oid_mgr::warm_up()
{
    ASSERT(oidmgr);
    std::cout << "[Warm-up] Started\n";
    {
        util::scoped_timer t("data warm-up");
        // Go over each OID entry and ensure_tuple there
        for (auto &fm : sm_file_mgr::fid_map) {
            auto* fd = fm.second;
            auto *alloc = oidmgr->get_allocator(fd->fid);
            OID himark = alloc->head.hiwater_mark;
            oid_array *oa = oidmgr->get_array(fd->fid);
            for (OID oid = 0; oid < himark; oid++)
                oidmgr->ensure_tuple(oa, oid, 0);
        }
    }
}

FID
sm_oid_mgr::create_file(bool needs_alloc)
{
    return get_impl(this)->create_file(needs_alloc);
}

void
sm_oid_mgr::recreate_file(FID f)
{
    return get_impl(this)->recreate_file(f);
}

void
sm_oid_mgr::recreate_allocator(FID f, OID m)
{
    return get_impl(this)->recreate_allocator(f, m);
}

bool
sm_oid_mgr::file_exists(FID f)
{
    return get_impl(this)->file_exists(f);
}

void
sm_oid_mgr::destroy_file(FID f)
{
    get_impl(this)->destroy_file(f);
}

OID
sm_oid_mgr::alloc_oid(FID f)
{
    /* FIXME (tzwang): OID=0's original intent was to denote
     * NULL, but I'm not sure what it's for currently; probably
     * it's used by the index (e.g., if we have OID=0 recorded
     * in some leaf node, that means the tuple was deleted).
     * So I just skip OID=1 to be allocated here. Need to find
     * out how the index treats OID=0. Meanwhile, it's totally
     * OK for the version chain's OID array to use OID=0.
     */
alloc:
    auto o = thread_allocate(get_impl(this), f);
    if (unlikely(o == 0))
        goto alloc;

    ASSERT(oid_get(f, o) == NULL_PTR);
    return o;
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

fat_ptr
sm_oid_mgr::oid_get(oid_array *oa, OID o)
{
    return *oa->get(o);
}

fat_ptr*
sm_oid_mgr::oid_get_ptr(FID f, OID o)
{
    return get_impl(this)->oid_access(f, o);
}

fat_ptr*
sm_oid_mgr::oid_get_ptr(oid_array *oa, OID o)
{
    return oa->get(o);
}

void
sm_oid_mgr::oid_put(FID f, OID o, fat_ptr p)
{
    auto *ptr = get_impl(this)->oid_access(f, o);
    *ptr = p;
}

void
sm_oid_mgr::oid_put(oid_array *oa, OID o, fat_ptr p)
{
    auto *ptr = oa->get(o);
    *ptr = p;
}

void
sm_oid_mgr::oid_put_new(FID f, OID o, fat_ptr p)
{
    auto *ptr = get_impl(this)->oid_access(f, o);
    ASSERT(*ptr == NULL_PTR);
    *ptr = p;
}

void
sm_oid_mgr::oid_put_new(oid_array *oa, OID o, fat_ptr p)
{
    auto *ptr = oa->get(o);
    ASSERT(*ptr == NULL_PTR);
    *ptr = p;
}

fat_ptr
sm_oid_mgr::oid_put_update(FID f,
                           OID o,
                           const varstr *value,
                           xid_context *updater_xc,
                           fat_ptr *new_obj_ptr)
{
    return oid_put_update(get_impl(this)->get_array(f), o, value, updater_xc, new_obj_ptr);
}

fat_ptr
sm_oid_mgr::oid_put_update(oid_array *oa,
                           OID o,
                           const varstr *value,
                           xid_context *updater_xc,
                           fat_ptr *new_obj_ptr)
{
#if CHECK_INVARIANTS
    int attempts = 0;
#endif
    auto *ptr = ensure_tuple(oa, o, updater_xc->begin_epoch);
    // No need to call ensure_tuple() below start_over - it returns a ptr,
    // a dereference is enough to capture the content.
    // Note: this is different from oid_get_version where start_over must
    // appear *above* ensure_tuple: here we don't change the value of ptr.
start_over:
    fat_ptr head = volatile_read(*ptr);
    object *old_desc = (object *)head.offset();
    ASSERT(head.size_code() != INVALID_SIZE_CODE);
    dbtuple *version = (dbtuple *)old_desc->payload();
    bool overwrite = false;

    auto clsn = volatile_read(old_desc->_clsn);
    if (clsn == NULL_PTR) {
        // stepping on an unlinked version?
        goto start_over;
    } else if (clsn.asi_type() == fat_ptr::ASI_XID) {
        /* Grab the context for this XID. If we're too slow,
           the context might be recycled for a different XID,
           perhaps even *while* we are reading the
           context. Copy everything we care about and then
           (last) check the context's XID for a mismatch that
           would indicate an inconsistent read. If this
           occurs, just start over---the version we cared
           about is guaranteed to have a LSN now.
         */
        auto holder_xid = XID::from_ptr(clsn);
        XID updater_xid = volatile_read(updater_xc->owner);

        // in-place update case (multiple updates on the same record  by same transaction)
        if (holder_xid == updater_xid) {
            overwrite = true;
            goto install;
        }

        xid_context *holder= xid_get_context(holder_xid);
        if (not holder) {
#if CHECK_INVARIANTS
            auto t = volatile_read(old_desc->_clsn).asi_type();
            ASSERT(t == fat_ptr::ASI_LOG or oid_get(oa, o) != head);
#endif
            goto start_over;
        }
        INVARIANT(holder);
        auto state = volatile_read(holder->state);
        auto owner = volatile_read(holder->owner);
        holder = NULL; // use cached values instead!

        // context still valid for this XID?
        if (unlikely(owner != holder_xid)) {
#if CHECK_INVARIANTS
            ASSERT(attempts < 2);
            attempts++;
#endif
            goto start_over;
        }
        ASSERT(holder_xid != updater_xid);
        if (state == TXN_CMMTD) {
            // Allow installing a new version if the tx committed (might
            // still hasn't finished post-commit). Note that the caller
            // (ie do_tree_put) should look at the clsn field of the
            // returned version (prev) to see if this is an overwrite
            // (ie xids match) or not (xids don't match).
            ASSERT(holder_xid != updater_xid);
            goto install;
        }
        return NULL_PTR;
    }
    // check dirty writes
    else {
        ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG );
#ifndef USE_READ_COMMITTED
        // First updater wins: if some concurrent tx committed first,
        // I have to abort. Same as in Oracle. Otherwise it's an isolation
        // failure: I can modify concurrent transaction's writes.
        if (LSN::from_ptr(clsn) > updater_xc->begin)
            return NULL_PTR;
#endif
        goto install;
    }

install:
    // remove uncommitted overwritten version
    // (tx's repetitive updates, keep the latest one only)
    // Note for this to be correct we shouldn't allow multiple txs
    // working on the same tuple at the same time.

    *new_obj_ptr = object::create_tuple_object(value, false, updater_xc->begin_epoch);
    object* new_object = (object *)new_obj_ptr->offset();
    ASSERT(not new_object->tuple()->is_defunct());
    new_object->_clsn = updater_xc->owner.to_ptr();

    if (overwrite) {
        volatile_write(new_object->_next, old_desc->_next);
        // I already claimed it, no need to use cas then
        volatile_write(ptr->_ptr, new_obj_ptr->_ptr);
        version->mark_defunct();
        __sync_synchronize();
        return head;
    }
    else {
        volatile_write(new_object->_next, head);
        if (__sync_bool_compare_and_swap(&ptr->_ptr, head._ptr, new_obj_ptr->_ptr)) {
            return head;
        }
    }
    return NULL_PTR;
}

dbtuple*
sm_oid_mgr::oid_get_latest_version(FID f, OID o)
{
    return oid_get_latest_version(get_impl(this)->get_array(f), o);
}

dbtuple*
sm_oid_mgr::oid_get_latest_version(oid_array *oa, OID o)
{
    auto head_offset = oa->get(o)->offset();
    if (head_offset)
        return (dbtuple *)((object *)head_offset)->payload();
    return NULL;
}

// Load the version from log if not present in memory
// TODO: anti-caching
fat_ptr*
sm_oid_mgr::ensure_tuple(FID f, OID o, epoch_num e)
{
    return ensure_tuple(get_impl(this)->get_array(f), o, e);
}

fat_ptr*
sm_oid_mgr::ensure_tuple(oid_array *oa, OID o, epoch_num e)
{
    fat_ptr *ptr = oa->get(o);
    fat_ptr p = *ptr;
    if (p.asi_type() == fat_ptr::ASI_LOG)
        ensure_tuple(ptr, e);
    return ptr;
}

// @ptr: address of an object_header
// Returns address of an object
fat_ptr
sm_oid_mgr::ensure_tuple(fat_ptr *ptr, epoch_num epoch)
{
    fat_ptr p = *ptr;
    if (p.asi_type() != fat_ptr::ASI_LOG) {
        ASSERT(p.asi_type() == 0);
        return p;
    }

    auto *obj = (object *)p.offset();

    // obj->_pdest should point to some location in the log
    ASSERT(obj->_pdest != NULL_PTR);
    fat_ptr new_ptr = object::create_tuple_object(obj->_pdest, obj->_next, epoch);
    ASSERT(new_ptr.offset());

    // Now new_ptr should point to some location in memory
    if (not __sync_bool_compare_and_swap(&ptr->_ptr, p._ptr, new_ptr._ptr)) {
        MM::deallocate(new_ptr);
        // somebody might acted faster, no need to retry
    }
    // FIXME: handle ASI_HEAP and ASI_EXT too
    return *ptr;
}

dbtuple*
sm_oid_mgr::oid_get_version(FID f, OID o, xid_context *visitor_xc)
{
    ASSERT(f);
    return oid_get_version(get_impl(this)->get_array(f), o, visitor_xc);
}

dbtuple*
sm_oid_mgr::oid_get_version(oid_array *oa, OID o, xid_context *visitor_xc)
{
start_over:
    // must pui start_over above this, because we'll update pp later
    fat_ptr *pp = oa->get(o);
    fat_ptr ptr = *pp;
    while (1) {
        if (ptr.asi_type() != fat_ptr::ASI_LOG) {
            ASSERT(ptr.asi_type() == 0);  // must be in memory
        } else {
            ptr = ensure_tuple(pp, visitor_xc->begin_epoch);
        }

        if (not ptr.offset())
            break;

        auto *cur_obj = (object*)ptr.offset();
        pp = &cur_obj->_next;

        // Must dereference this before reading cur_obj->_clsn:
        // the version we're currently reading (ie cur_obj) might be unlinked
        // and thus recycled by the memory allocator at any time if it's not
        // a committed version. If so, cur_obj->_next will be pointing to some
        // other object in the allocator's free object pool - we'll probably
        // end up at la-la land if we followed this _next pointer value...
        // Here we employ some flavor of OCC to solve this problem:
        // the aborting transaction that will unlink cur_obj will update
        // cur_obj->_clsn to NULL_PTR, then deallocate(). Before reading
        // cur_obj->_clsn, we (as the visitor), first dereference pp to get
        // a stable value that "should" contain the right address of the next
        // version. We then read cur_obj->_clsn to verify: if it's NULL_PTR
        // that means we might have read a wrong _next value that's actually
        // pointing to some irrelevant object in the allocator's memory pool,
        // hence must start over from the beginning of the version chain.
        ptr = volatile_read(*pp);
        auto clsn = volatile_read(cur_obj->_clsn);
        if (clsn == NULL_PTR) {
            // dead tuple that was (or about to be) unlinked, start over
            goto start_over;
        }

        ASSERT(clsn.asi_type() == fat_ptr::ASI_XID or clsn.asi_type() == fat_ptr::ASI_LOG);
        // xid tracking & status check
        if (clsn.asi_type() == fat_ptr::ASI_XID) {
            /* Same as above: grab and verify XID context,
               starting over if it has been recycled
             */
            auto holder_xid = XID::from_ptr(clsn);

            // dirty data made by me is visible!
            if (holder_xid == visitor_xc->owner) {
                ASSERT(not cur_obj->_next.offset() or ((object *)cur_obj->_next.offset())->_clsn.asi_type() == fat_ptr::ASI_LOG);
                return cur_obj->tuple();
            }
            xid_context *holder = xid_get_context(holder_xid);
            if (not holder) // invalid XID (dead tuple, must retry than goto next in the chain)
                goto start_over;

            auto state = volatile_read(holder->state);
            auto owner = volatile_read(holder->owner);

            // context still valid for this XID?
            if (unlikely(owner != holder_xid))
                goto start_over;

            if (state == TXN_CMMTD) {
                ASSERT(volatile_read(holder->end).offset());
                ASSERT(owner == holder_xid);
#ifdef USE_READ_COMMITTED
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
                if (sysconf::enable_safesnap and visitor_xc->xct->flags & transaction::TXN_FLAG_READ_ONLY) {
                    if (holder->end < visitor_xc->begin)
                        return cur_obj->tuple();
                }
                else {
                    return cur_obj->tuple();
                }
#else
                return cur_obj->tuple();
#endif
#else
                if (holder->end < visitor_xc->begin) {
                    return cur_obj->tuple();
                }
#if defined(USE_PARALLEL_SSI) or defined(USE_PARALLEL_SSN)
                else {
                    oid_check_phantom(visitor_xc, holder->end.offset());
                }
#endif
#endif
            }
#ifdef READ_COMMITTED_SPIN
            else {
                // spin until the tx is settled (either aborted or committed)
                if (wait_for_commit_result(holder))
                    return cur_obj->tuple();
            }
#endif
        }
        else if (clsn.asi_type() == fat_ptr::ASI_LOG) {
#ifdef USE_READ_COMMITTED
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
            if (sysconf::enable_safesnap and visitor_xc->xct->flags & transaction::TXN_FLAG_READ_ONLY) {
                if (LSN::from_ptr(clsn) <= visitor_xc->begin)
                    return cur_obj->tuple();
            }
            else
                return cur_obj->tuple();
#else
            return cur_obj->tuple();
#endif
#else
            if (LSN::from_ptr(clsn) <= visitor_xc->begin) {
                return cur_obj->tuple();
            }
#if defined(USE_PARALLEL_SSI) or defined(USE_PARALLEL_SSN)
            else {
                oid_check_phantom(visitor_xc, clsn.offset());
            }
#endif
#endif
        }
        else {
            ALWAYS_ASSERT(false);
        }
    }

    return NULL;    // No Visible records
}

void
sm_oid_mgr::oid_check_phantom(xid_context *visitor_xc, uint64_t vcstamp) {
  /*
   * tzwang (May 05, 2016): Preventing phantom:
   * Consider an example:
   *
   * Assume the database has tuples B (key=1) and C (key=2).
   *
   * Time      T1             T2
   * 1        ...           Read B
   * 2        ...           Insert A
   * 3        ...           Commit
   * 4       Scan key > 1
   * 5       Update B
   * 6       Commit (?)
   *
   * At time 6 T1 should abort, but checking index version changes
   * wouldn't make T1 abort, since its scan happened after T2's
   * commit and yet its begin timestamp is before T2 - T1 wouldn't
   * see A (oid_get_version will skip it even it saw it from the tree)
   * but the scanning wouldn't record a version change in tree structure
   * either (T2 already finished all SMOs).
   *
   * Under SSN/SSI, this essentially requires we update the corresponding
   * stamps upon hitting an invisible version, treating it like some
   * successor updated our read set. For SSI, this translates to updating
   * ct3; for SSN, update the visitor's sstamp.
   */
#ifdef USE_PARALLEL_SSI
  auto vct3 = volatile_read(visitor_xc->ct3);
  if (not vct3 or vct3 > vcstamp) {
      volatile_write(visitor_xc->ct3, vcstamp);
  }
#elif defined USE_PARALLEL_SSN
  visitor_xc->sstamp = std::min(visitor_xc->sstamp.load(), vcstamp);
  // TODO(tzwang): do early SSN check here
#endif  // USE_PARALLEL_SSI/SSN
}

void
sm_oid_mgr::oid_unlink(FID f, OID o, void *object_payload)
{
    return oid_unlink(get_impl(this)->get_array(f), o, object_payload);
}

void
sm_oid_mgr::oid_unlink(oid_array *oa, OID o, void *object_payload)
{
    // Now the head is guaranteed to be the only dirty version
    // because we unlink the overwritten dirty version in put,
    // essentially this function ditches the head directly.
    // Otherwise use the commented out old code.
    auto *ptr = oa->get(o);
    object *head_obj = (object *)ptr->offset();
    ASSERT(head_obj->payload() == object_payload);
    // using a CAS is overkill: head is guaranteed to be the (only) dirty version
    volatile_write(ptr->_ptr, head_obj->_next._ptr);
    __sync_synchronize();
    // tzwang: The caller is responsible for deallocate() the head version
    // got unlinked - a update of own write will record the unlinked version
    // in the transaction's write-set, during abortor commit the version's
    // pvalue needs to be examined. So oid_unlink() shouldn't deallocate()
    // here. Instead, the transaction does it in during commit or abort.
}

