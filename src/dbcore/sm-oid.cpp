#include <fcntl.h>
#include <unistd.h>

#include <map>

#include "../benchmarks/ndb_wrapper.h"
#include "../txn.h"
#include "../util.h"

#include "burt-hash.h"
#include "sc-hash.h"
#include "sm-alloc.h"
#include "sm-chkpt.h"
#include "sm-config.h"
#include "sm-index.h"
#include "sm-log-recover-impl.h"
#include "sm-object.h"
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
    static size_t const NENTRIES = 4096;
    
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
    // TODO: DEFER deletion of these arrays if constructor throws
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
    if(file_exists(f)) {
      LOG(FATAL) << "File already exists. Is this a secondary index?";
    }
    auto ptr = oid_array::make();
    oid_put(OBJARRAY_FID, f, ptr);
    ASSERT(file_exists(f));
    DLOG(INFO) << "[Recovery] recreate file " << f;
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

    if (m <= alloc->head.capacity_mark and m <= alloc->head.hiwater_mark)
        return;

    // Set capacity_mark = hiwater_mark = m, the cache machinery
    // will do the rest (note the +64 to avoid duplicates)
    alloc->head.capacity_mark = alloc->head.hiwater_mark = (m + 64);
    ASSERT(file_exists(f));
    fat_ptr ptr = oid_get(OBJARRAY_FID, f);
    oid_array *oa = ptr;
    ASSERT(oa);
    oa->ensure_size(alloc->head.capacity_mark);
    DLOG(INFO) << "[Recovery] recreate allocator " << f << ", himark=" << m;
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
    // TODO: delete metadata? or force caller to do it before now?

    // one allocator controls all three files
    thread_free(this, OBJARRAY_FID, f);
}

oid_array*
sm_oid_mgr::get_array(FID f)
{
    return get_impl(this)->get_array(f);
}

void
sm_oid_mgr::create()
{
  // Create an empty oidmgr, with initial internal files
  oidmgr = new sm_oid_mgr_impl{};
  oidmgr->dfd = dirent_iterator(config::log_dir.c_str()).dup();
}

void
sm_oid_mgr::take_chkpt(uint64_t chkpt_start_lsn)
{
    // Now the real work. The format of a chkpt file is:
    // [number of indexes]
    // [primary index 1 name length, name, tuple/key FID, himark]
    // [primary index 2 name length, name, tuple/key FID, himark]
    // ...
    // [2nd index 1 name length, name, FID, himark]
    // [2nd index 2 name length, name, FID, himark]
    // ...
    //
    // [index 1 himark]
    // [index 1 tuple_fid, key_fid]
    // [OID1, key and/or data]
    // [OID2, key and/or data]
    // [index 1 himark]
    // ...
    // same thing for index 2
    // ...

    // Write the number of indexes 
    // TODO(tzwang): handle dynamically created tables/indexes
    uint64_t chkpt_size = 0;
    uint32_t num_idx = IndexDescriptor::NumIndexes();
    chkptmgr->write_buffer(&num_idx, sizeof(uint32_t));
    chkpt_size += sizeof(uint32_t);

    // Write details about each primary index, then secondary index
    // so that during recovery we have primary indexes first and then
    // 2nd indexes can refer to the FID of the corresponding primary.
    bool handling_2nd = false;
iterate_index:
    for(auto& fm : IndexDescriptor::name_map) {
      IndexDescriptor* id = fm.second;
      if(!((id->IsPrimary() && !handling_2nd) || !id->IsPrimary() && handling_2nd)) {
        continue;
      }
      size_t len = id->GetName().length();
      FID tuple_fid = id->GetTupleFid();
      FID key_fid = id->GetKeyFid();
      auto *alloc = get_impl(this)->get_allocator(tuple_fid);
      OID himark = alloc->head.hiwater_mark;

      // [Name length, name, tuple/key FID, himark]
      chkptmgr->write_buffer(&len, sizeof(size_t));
      chkptmgr->write_buffer((void *)id->GetName().c_str(), len);
      chkptmgr->write_buffer(&tuple_fid, sizeof(FID));
      chkptmgr->write_buffer(&key_fid, sizeof(FID));
      chkptmgr->write_buffer(&himark, sizeof(OID));
      chkpt_size += (sizeof(size_t) + len + sizeof(FID) + sizeof(OID));
    }
    if(!handling_2nd) {
      handling_2nd = true;
      goto iterate_index;
    }
    LOG(INFO) << "[Checkpoint] header size: " << chkpt_size;

    // Write keys and/or tuples for each index, primary first
    for(auto& fm : IndexDescriptor::name_map) {
        IndexDescriptor* id = fm.second;
        FID tuple_fid = id->GetTupleFid();
        // Find the high watermark of this file and dump its
        // backing store up to the size of the high watermark
        auto *alloc = get_impl(this)->get_allocator(tuple_fid);
        OID himark = alloc->head.hiwater_mark;

        // Write himark
        chkptmgr->write_buffer(&himark, sizeof(OID));

        // Write the tuple FID to note which table these OIDs to follow belongs to
        chkptmgr->write_buffer(&tuple_fid, sizeof(FID));

        // Key FID
        FID key_fid = id->GetKeyFid();
        chkptmgr->write_buffer(&key_fid, sizeof(FID));

        auto* oa = fm.second->GetTupleArray();
        auto* ka = fm.second->GetKeyArray();
        ASSERT(oa);
        ASSERT(ka);

        // Now write the [OID, payload] pairs. Record both OID and keys for
        // primary indexes; keys only for 2nd indexes.
        uint64_t nrecords = 0;
        bool is_primary = id->IsPrimary();
        for (OID oid = 0; oid < himark; oid++) {
            // Checkpoints need not be consistent: grab the latest committed
            // version and leave.
            fat_ptr ptr = oid_get(oa, oid);
        retry:
            if (not ptr.offset()) {
                continue;
            }

            Object* obj = (Object*)ptr.offset();
            if(obj->IsInMemory()) {
              fat_ptr clsn = obj->GetClsn();
              ASSERT(clsn.asi_type() != fat_ptr::ASI_CHK);
              // Someone is working on this version
              if (clsn.asi_type() != fat_ptr::ASI_LOG) {
                  ptr = obj->GetNext();
                  goto retry;
              }
            }

            ASSERT(obj->GetClsn().offset());
            ASSERT(obj->GetClsn().asi_type() == fat_ptr::ASI_LOG);

            // ptr now will point to the persistent location
            ptr = obj->GetPersistentAddress();
            if(ptr.offset() == 0) {
              // must be a delete, skip it
              continue;
            }

            nrecords++;
            // Write OID
            chkptmgr->write_buffer(&oid, sizeof(OID));

            // Key
            fat_ptr key_ptr = oid_get(ka, oid);
            varstr* key = (varstr*)key_ptr.offset();
            ALWAYS_ASSERT(key);
            ALWAYS_ASSERT(key->l);
            ALWAYS_ASSERT(key->p);
            chkptmgr->write_buffer(&key->l, sizeof(uint32_t));
            chkptmgr->write_buffer(key->data(), key->size());

            // Tuple data if it's the primary index
            if(fm.second->IsPrimary()) {
              if(!obj->IsInMemory()) {
                obj->Pin();
              }
              uint8_t size_code = ptr.size_code();
              ALWAYS_ASSERT(size_code != INVALID_SIZE_CODE);
              auto data_size = decode_size_aligned(size_code);
              data_size = decode_size_aligned(size_code);
              ALWAYS_ASSERT(obj->GetTuple()->size <= data_size - sizeof(Object) - sizeof(dbtuple));
              chkptmgr->write_buffer(&size_code, sizeof(uint8_t));
              // It's already there if we're digging out a tuple from a previous chkpt
              chkptmgr->write_buffer((char*)obj, data_size);
              ALWAYS_ASSERT(obj->GetClsn().asi_type() == fat_ptr::ASI_LOG);
              ALWAYS_ASSERT(obj->GetTuple()->size <= data_size - sizeof(Object) - sizeof(dbtuple));
              chkpt_size += (sizeof(OID) + sizeof(uint8_t) + data_size);
            }
        }
        // Write himark to denote end
        chkptmgr->write_buffer(&himark, sizeof(OID));
        LOG(INFO) << "[Checkpoint] "<< id->GetName() << " (" << tuple_fid << ", "
          << key_fid << ") himark=" << himark << ", wrote " << chkpt_size
          << " bytes, " << nrecords << " records";
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
  // REVISIT
  /*
    ASSERT(oidmgr);
    std::cout << "[Warm-up] Started\n";
    {
        util::scoped_timer t("data warm-up");
        // Go over each OID entry and ensure_tuple there
        for (auto &fm : IndexDescriptor::fid_map) {
            auto* id = fm.second;
            auto *alloc = oidmgr->get_allocator(id->GetTupleFid());
            OID himark = alloc->head.hiwater_mark;
            FID fid = fm.first;
            oid_array *oa = oidmgr->get_array(fid);
            for (OID oid = 0; oid < himark; oid++)
                oidmgr->ensure_tuple(oa, oid, 0);
        }
    }
    */
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

fat_ptr*
sm_oid_mgr::oid_get_ptr(FID f, OID o)
{
    return get_impl(this)->oid_access(f, o);
}

void
sm_oid_mgr::oid_put(FID f, OID o, fat_ptr p)
{
    *get_impl(this)->oid_access(f, o) = p;
}

void
sm_oid_mgr::oid_put_new(FID f, OID o, fat_ptr p)
{
    auto *entry = get_impl(this)->oid_access(f, o);
    ALWAYS_ASSERT(*entry == NULL_PTR);
    *entry = p;
}

void
sm_oid_mgr::oid_put_new_if_absent(FID f, OID o, fat_ptr p)
{
  auto *entry = get_impl(this)->oid_access(f, o);
  if(*entry == NULL_PTR) {
    *entry = p;
  }
}

bool
sm_oid_mgr::oid_put_latest(FID f, OID o, fat_ptr p, varstr* k, uint64_t lsn_offset)
{
  ASSERT(config::is_backup_srv() || config::loading);
  return oid_put_latest(get_impl(this)->get_array(f), o, p, k, lsn_offset);
}

bool
sm_oid_mgr::oid_put_latest(oid_array* oa, OID o, fat_ptr p, varstr* k, uint64_t lsn_offset) {
  ASSERT(config::is_backup_srv() || config::loading);
  auto* entry = oa->get(o);
retry:
  fat_ptr ptr = *entry;
  bool do_it = false;
  if(ptr == NULL_PTR) {
    do_it = true;
  } else {
    uint32_t type = ptr.asi_type();
    if(type == fat_ptr::ASI_CHK) {
      // Must be in chkpt file
      // TODO(tzwang): so far we don't chkpt on the backup, so it's safe to
      // just overwrite (a CAS is needed still) because if it remains to be
      // ASI_CHK then it's truely not touched by query threads.
      ALWAYS_ASSERT(type == fat_ptr::ASI_CHK);
      do_it = true;
    } else {
      // Must go into the object to see lsn
      Object *obj = (Object*)ptr.offset();
      if(obj->GetClsn().offset() < lsn_offset) {
        do_it = true;
        obj->SetNext(ptr);
      }
    }
  }

  if(do_it) {
    uint64_t expected = ptr._ptr;
    if(__sync_bool_compare_and_swap(&entry->_ptr, expected, p._ptr)) {
      if(k && !config::is_backup_srv() && expected == 0) {
        //volatile_write(IndexDescriptor::Get(f)->GetKeyArray()->get(o), k);
      }
      return true;
    } else {
      goto retry;
    }
  }
  return false;
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
    auto *ptr = oa->get(o);
start_over:
    fat_ptr head = volatile_read(*ptr);
    Object* old_desc = (Object*)head.offset();
    ASSERT(head.size_code() != INVALID_SIZE_CODE);
    dbtuple* version = (dbtuple *)old_desc->GetPayload();
    bool overwrite = false;

    auto clsn = old_desc->GetClsn();
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
#ifndef NDEBUG
            auto t = old_desc->GetClsn().asi_type();
            ASSERT(t == fat_ptr::ASI_LOG or oid_get(oa, o) != head);
#endif
            goto start_over;
        }
        ASSERT(holder);
        auto state = volatile_read(holder->state);
        auto owner = volatile_read(holder->owner);
        holder = NULL; // use cached values instead!

        // context still valid for this XID?
        if (unlikely(owner != holder_xid)) {
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
        ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
#ifndef RC
        // First updater wins: if some concurrent tx committed first,
        // I have to abort. Same as in Oracle. Otherwise it's an isolation
        // failure: I can modify concurrent transaction's writes.
        if (LSN::from_ptr(clsn).offset() >= updater_xc->begin)
            return NULL_PTR;
#endif
        goto install;
    }

install:
    // remove uncommitted overwritten version
    // (tx's repetitive updates, keep the latest one only)
    // Note for this to be correct we shouldn't allow multiple txs
    // working on the same tuple at the same time.

    *new_obj_ptr = Object::Create(value, false, updater_xc->begin_epoch);
    Object *new_object = (Object*)new_obj_ptr->offset();
    new_object->SetClsn(updater_xc->owner.to_ptr());

    if (overwrite) {
        new_object->SetNext(old_desc->GetNext());
        // I already claimed it, no need to use cas then
        volatile_write(ptr->_ptr, new_obj_ptr->_ptr);
        __sync_synchronize();
        return head;
    } else {
        new_object->SetNext(head);
        if (__sync_bool_compare_and_swap(&ptr->_ptr, head._ptr, new_obj_ptr->_ptr)) {
            // Succeeded installing a new version, now only I can modify the
            // chain, try recycle some objects
            if(config::enable_gc) {
              MM::gc_version_chain(ptr);
            }
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

// Load the tuple pointed to by [ptr] from storage to memory, must
// handle cases where we run into an concurrenty update.
fat_ptr
sm_oid_mgr::ensure_version(fat_ptr* prev, fat_ptr ptr, epoch_num epoch)
{
  /*
  fat_ptr p = *ptr;
  auto asi_type = p.asi_type();
  if(asi_type == 0) {
    // Main-memory tuple
    return p;
  }
  ASSERT(asi_type == fat_ptr::ASI_LOG || asi_type == fat_ptr::ASI_CHK);

  // *ptr should point to some location in the log or chkpt file
  fat_ptr new_ptr = object::load_durable_object(p, NULL_PTR, epoch, nullptr);
  ASSERT(new_ptr.offset());

  // Now new_ptr should point to some location in memory
  if(not __sync_bool_compare_and_swap(&ptr->_ptr, p._ptr, new_ptr._ptr)) {
    MM::deallocate(new_ptr);
    // somebody might acted faster, no need to retry
  }
  // FIXME: handle ASI_HEAP and ASI_EXT too
  return *ptr;
  */
}

dbtuple*
sm_oid_mgr::oid_get_version(FID f, OID o, xid_context *visitor_xc)
{
    ASSERT(f);
    return oid_get_version(get_impl(this)->get_array(f), o, visitor_xc);
}

dbtuple*
sm_oid_mgr::oid_get_version(oid_array *oa, OID o, xid_context *visitor_xc) {
start_over:
  fat_ptr* entry = oa->get(o);
  fat_ptr ptr = volatile_read(*entry);
  while (1) {
    ASSERT(ptr.asi_type() == 0);
    if(ptr.offset() == 0) {
      break;
    }

    Object *cur_obj = (Object*)ptr.offset();
    // Must read next_ before reading cur_obj->_clsn:
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
    fat_ptr tentative_next = cur_obj->GetNext();
    auto clsn = cur_obj->GetClsn();
    if (clsn == NULL_PTR) {
      // dead tuple that was (or about to be) unlinked, start over
      goto start_over;
    }

    uint16_t asi_type = clsn.asi_type();
    ALWAYS_ASSERT(asi_type == fat_ptr::ASI_XID || asi_type == fat_ptr::ASI_LOG);
    if(asi_type == fat_ptr::ASI_XID) {  // in-flight
      XID holder_xid = XID::from_ptr(clsn);

      // dirty data made by me is visible!
      if(holder_xid == visitor_xc->owner) {
        ASSERT(!cur_obj->GetNext().offset() ||
               ((Object*)cur_obj->GetNext().offset())->GetClsn().asi_type() == fat_ptr::ASI_LOG);
        return cur_obj->GetTuple();
      }
      auto* holder = xid_get_context(holder_xid);
      if(!holder) {  // invalid XID (dead tuple, must retry than goto next in the chain)
        goto start_over;
      }

      auto state = volatile_read(holder->state);
      auto owner = volatile_read(holder->owner);

      // context still valid for this XID?
      if(owner != holder_xid) {
        goto start_over;
      }

      if(state == TXN_CMMTD) {
        ASSERT(volatile_read(holder->end));
        ASSERT(owner == holder_xid);
#if defined(RC) || defined(RC_SPIN)
#ifdef SSN
        if(config::enable_safesnap && (visitor_xc->xct->flags & transaction::TXN_FLAG_READ_ONLY)) {
          if(holder->end < visitor_xc->begin) {
            return cur_obj->GetTuple();
          }
        } else {
          return cur_obj->GetTuple();
        }
#else  // SSN
        return cur_obj->GetTuple();
#endif  // SSN
#else  // not RC/RC_SPIN
        if(holder->end < visitor_xc->begin) {
          return cur_obj->GetTuple();
        } else {
          oid_check_phantom(visitor_xc, holder->end);
        }
#endif
      }
    } else {
      // Already committed, now do visibility test and also
      // see OID entry's ASI to determine where the data is.
      ASSERT(asi_type == fat_ptr::ASI_LOG || asi_type == fat_ptr::ASI_CHK);
      uint64_t lsn_offset = LSN::from_ptr(clsn).offset();
#if defined(RC) || defined(RC_SPIN)
#if defined(SSN)
      if(config::enable_safesnap && (visitor_xc->xct->flags & transaction::TXN_FLAG_READ_ONLY)) {
        if(lsn_offset <= visitor_xc->begin) {
          return cur_obj->PinAndGetTuple();
        } else {
          oid_check_phantom(visitor_xc, clsn.offset());
        }
      } else {
        return cur_obj->PinAndGetTuple();
      }
#else
      return cur_obj->PinAndGetTuple();
#endif
#else  // Not RC
      if(lsn_offset <= visitor_xc->begin) {
        return cur_obj->PinAndGetTuple();
      } else {
        oid_check_phantom(visitor_xc, clsn.offset());
      }
#endif
    }
    ptr = tentative_next;
  }
  return nullptr;    // No Visible records
}

void
sm_oid_mgr::oid_unlink(FID f, OID o)
{
    return oid_unlink(get_impl(this)->get_array(f), o);
}

