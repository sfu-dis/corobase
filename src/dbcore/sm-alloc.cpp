#include <numa.h>
#include <sched.h>
#include <sys/mman.h>

#include <atomic>
#include <condition_variable>
#include <future>

#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"

/*
 * tzwang (2015-10-31): continued with the CAS bottleneck found in log LSN
 * offset acquisition, this is a problem too with GC when a tx tries to
 * append its updated set of OIDs to the centralized GC list.
 * So we make it an atomic-swap (XCHG) to append to the centralized list.
 * Let's see when this will become a problem later.
 *
 * Garbage collection: maintain a centralized queue for updated tuples.
 * Writers add updated OIDs in the form of <fid, oid> to the queue upon
 * commit, to the tail of the queue. A GC thread periodically consumes
 * queue nodes from the head of the queue and recycle old versions if
 * their clsn < trim_lsn. This way avoids scanning the whole OID array
 * which might be increasing quite fast. Amount of scanning is only
 * related to update footprint.
 */

namespace MM {
std::condition_variable gc_trigger;
std::mutex gc_lock;
void gc_daemon();

// This is the list head; a committing tx atomically swaps the head of
// its queue of updated OIDs with recycle_oid_list, and links its tail
// to the old head which it got as the return value of the XCHG.
std::atomic<fat_ptr> recycle_oid_list(NULL_PTR);

// tzwang (2015-11-01):
// trim_lsn is the LSN of the no-longer-needed versions, which is the end LSN
// of the epoch that most recently reclaimed (i.e., all **readers** are gone).
// It corresponds to two epochs ago of the most recently reclaimed epoch (in
// which all the **creators** are gone). Note that trim_lsn only records the
// LSN when the *creator* of the versions left---not the *visitor* of those
// versions. So consider the versions created in epoch N, we can have stragglers
// up to at most epoch N+2; to start a new epoch N+3, N must be reclaimed. This
// means versions last *visited* (not created!) in epoch N+2 are no longer
// needed after epoch N+4 is reclaimed.
//
// Note: when really recycling versions, we also need to make sure we leave
// one committed versions that is older than the begin LSN of any transaction.
// This "begin LSN of any tx" translates to the current log LSN for normal
// transactions, and the safesnap LSN for read-only transactions using a safe
// snapshot. For simplicity, the daemon simply leave one version with LSN < the
// trim_lsn available (so no need to figure out which safesnap lsn is the
// appropriate one to rely on - it changes as the daemon does its work).
LSN trim_lsn = INVALID_LSN;

uint64_t EPOCH_SIZE_NBYTES = 1 << 28;
uint64_t EPOCH_SIZE_COUNT = 200000;

// epoch_excl_begin_lsn belongs to the previous **ending** epoch, and we're
// using it as the **begin** lsn of the new epoch.
LSN epoch_excl_begin_lsn[3] = {INVALID_LSN, INVALID_LSN, INVALID_LSN};
LSN epoch_reclaim_lsn[3] = {INVALID_LSN, INVALID_LSN, INVALID_LSN};
LSN safesnap_lsn = INVALID_LSN;

object_pool central_object_pool CACHE_ALIGNED;
static __thread thread_object_pool *tls_object_pool CACHE_ALIGNED;
static __thread fat_ptr tls_unlinked_objects CACHE_ALIGNED;

char **node_memory = NULL;
uint64_t *allocated_node_memory = NULL;
static uint64_t __thread tls_allocated_node_memory CACHE_ALIGNED;
static const uint64_t tls_node_memory_gb = 1;

void
prepare_node_memory() {
    ALWAYS_ASSERT(sysconf::numa_nodes);
    allocated_node_memory = (uint64_t *)malloc(sizeof(uint64_t) * sysconf::numa_nodes);
    node_memory = (char **)malloc(sizeof(char *) * sysconf::numa_nodes);
    std::vector<std::future<void> > futures;
    std::cout << "Will run and allocate on " << sysconf::numa_nodes << " nodes, "
              << sysconf::node_memory_gb << "GB each\n";
    for (int i = 0; i < sysconf::numa_nodes; i++) {
        std::cout << "Allocating " << sysconf::node_memory_gb << "GB on node " << i << std::endl;
        auto f = [=]{
            ALWAYS_ASSERT(sysconf::node_memory_gb);
            allocated_node_memory[i] = 0;
            numa_set_preferred(i);
            node_memory[i] = (char *)mmap(NULL,
                sysconf::node_memory_gb * sysconf::GB,
                PROT_READ | PROT_WRITE,
                MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE,
                -1,
                0);
            THROW_IF(node_memory[i] == nullptr or node_memory[i] == MAP_FAILED,
                os_error, errno, "Unable to allocate huge pages");
            ALWAYS_ASSERT(node_memory[i]);
            std::cout << "Allocated " << sysconf::node_memory_gb << "GB on node " << i << std::endl;
        };
        futures.push_back(std::async(std::launch::async, f));
    }

    for (auto& f : futures) {
        f.get();
    }
}

void global_init(void*)
{
    if (sysconf::enable_gc) {
        std::thread t(gc_daemon);
        t.detach();
    }
}

// epochs related
static __thread struct thread_data epoch_tls CACHE_ALIGNED;
epoch_mgr mm_epochs {{nullptr, &global_init, &get_tls,
                    &thread_registered, &thread_deregistered,
                    &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

void *allocate(size_t size, epoch_num e) {
    size = align_up(size);
    void *p = NULL;

    // Don't bother TLS if it's still loading
    if (sysconf::loading) {
        p = allocate_onnode(size);
        goto out;
    }

    ALWAYS_ASSERT(not p);
    // Can we reuse some of our own unlinked object?
    // A bit expensive to go over all of them, take a look at the first a few
    static const int rounds = 5;
    if (tls_unlinked_objects != NULL_PTR) {
        fat_ptr ptr = tls_unlinked_objects;
        int n = 0;
        while (ptr != NULL_PTR and n++ < rounds) {
            object *obj = (object *)ptr.offset();
            if (e < 4) {
              continue;  // Note we later will compare e - 4 and with obj's alloc epoch...
            }
            if (obj->_alloc_epoch <= e - 4 and decode_size_aligned(ptr.size_code()) == size) {
                p = (void *)ptr.offset();
                tls_unlinked_objects = obj->_next;
                goto out;
            }
            ptr = ((object *)ptr.offset())->_next;
        }
    }

    ALWAYS_ASSERT(not p);
    // Have to ask the TLS pool (possibly the central pool)
    if (unlikely(!tls_object_pool)) {
        tls_object_pool = new thread_object_pool();
    }
    ASSERT(tls_object_pool);
    p = (void *)tls_object_pool->get_object(size);
    if (p) {
        goto out;
    } else {
        auto* ol = central_object_pool.get_object_list(size);
        if (ol) {
            ASSERT(ol->head != NULL_PTR);
            ASSERT(ol->tail != NULL_PTR);
            tls_object_pool->put_objects(*ol);
            p = (void *)tls_object_pool->get_object(size);
            ALWAYS_ASSERT(p);
            goto out;
        }
    }

    ALWAYS_ASSERT(not p);
    // Have to use the vanilla bump allocator, hopefully later we reuse them
    static __thread char* tls_node_memory CACHE_ALIGNED;
    if (unlikely(not tls_node_memory) or
        tls_allocated_node_memory + size >= tls_node_memory_gb * sysconf::GB) {
        tls_node_memory = (char *)allocate_onnode(tls_node_memory_gb * sysconf::GB);
        tls_allocated_node_memory = 0;
    }

    if (likely(tls_node_memory)) {
        p = tls_node_memory + tls_allocated_node_memory;
        tls_allocated_node_memory += size;
        goto out;
    }

out:
    ALWAYS_ASSERT(p);
    epoch_tls.nbytes += size;
    epoch_tls.counts += 1;
    return p;
}

// Allocate memory directly from the node pool (only loader does this so far)
void* allocate_onnode(size_t size) {
    size = align_up(size);
    auto node = numa_node_of_cpu(sched_getcpu());
    ALWAYS_ASSERT(node < sysconf::numa_nodes);
    auto offset = __sync_fetch_and_add(&allocated_node_memory[node], size);
    if (likely(offset + size <= sysconf::node_memory_gb * sysconf::GB)) {
        return node_memory[node] + offset;
    }
    return NULL;
}

void deallocate(fat_ptr p)
{
    ASSERT(p != NULL_PTR);
    ASSERT(p.size_code());
    ASSERT(p.size_code() != INVALID_SIZE_CODE);
    object *obj = (object *)p.offset();
    obj->_next = tls_unlinked_objects;
    tls_unlinked_objects = p;
}

// epoch mgr callbacks
epoch_mgr::tls_storage *
get_tls(void*)
{
    static __thread epoch_mgr::tls_storage s;
    return &s;
}
void*
thread_registered(void*)
{
    epoch_tls.initialized = true;
    epoch_tls.nbytes = 0;
    epoch_tls.counts = 0;
    return &epoch_tls;
}

void
thread_deregistered(void *cookie, void *thread_cookie)
{
    auto *t = (thread_data*) thread_cookie;
    ASSERT(t == &epoch_tls);
    t->initialized = false;
    t->nbytes = 0;
    t->counts = 0;
}

void*
epoch_ended(void *cookie, epoch_num e)
{
    // remember the epoch number so we can find it out when it's reclaimed later
    epoch_num *epoch = (epoch_num *)malloc(sizeof(epoch_num));
    *epoch = e;
    return (void *)epoch;
}

void*
epoch_ended_thread(void *cookie, void *epoch_cookie, void *thread_cookie)
{
    return epoch_cookie;
}

void
epoch_reclaimed(void *cookie, void *epoch_cookie)
{
    epoch_num e = *(epoch_num *)epoch_cookie;
    free(epoch_cookie);
    LSN my_begin_lsn = epoch_excl_begin_lsn[e % 3];
    if (not sysconf::enable_gc or my_begin_lsn == INVALID_LSN)
        return;
    epoch_reclaim_lsn[e % 3] = my_begin_lsn;
    if (sysconf::enable_safesnap) {
        // Make versions created during epoch N available for transactions
        // using safesnap. All transactions that created something in this
        // epoch has gone, so it's impossible for a reader using that
        // epoch's end lsn as both begin and commit timestamp to have
        // conflicts with these writer transactions. But future transactions
        // need to start with a pstamp of safesnap_lsn.
        // Take max here because after the loading phase we directly set
        // safesnap_lsn to log.cur_lsn, which might be actually larger than
        // safesnap_lsn generated during the loading phase, which is too
        // conservertive that some tx might not be able to see the tuples
        // because they were not loaded at that time...
        // Also use epoch e+1's begin LSN = epoch e's end LSN
        auto new_safesnap_lsn = epoch_excl_begin_lsn[(e + 1) % 3];
        volatile_write(safesnap_lsn._val, std::max(safesnap_lsn._val, new_safesnap_lsn._val));
    }
    if (e >= 2) {
        trim_lsn = epoch_reclaim_lsn[(e - 2) % 3];
        epoch_reclaim_lsn[(e - 2) % 3] = INVALID_LSN;
        gc_trigger.notify_all();
    }
}

void
epoch_exit(LSN s, epoch_num e)
{
    // Transactions under a safesnap will pass s = INVALID_LSN
    if (s != INVALID_LSN and
        (epoch_tls.nbytes >= EPOCH_SIZE_NBYTES or
         epoch_tls.counts >= EPOCH_SIZE_COUNT)) {
        // epoch_ended() (which is called by new_epoch() in its critical
        // section) will pick up this safe lsn if new_epoch() succeeded
        // (it could also be set by somebody else who's also doing epoch_exit(),
        // but this is captured before new_epoch succeeds, so we're fine).
        // If instead, we do this in epoch_ended(), that cur_lsn captured
        // actually belongs to the *new* epoch - not safe to trim based on
        // this lsn. The real trim_lsn should be some lsn at the end of the
        // ending epoch, not some lsn after the next epoch.
        epoch_excl_begin_lsn[(e + 1) % 3] = s;
        if (mm_epochs.new_epoch_possible() and mm_epochs.new_epoch())
            epoch_tls.nbytes = epoch_tls.counts = 0;
    }
    mm_epochs.thread_exit();
}

void recycle(fat_ptr list_head, fat_ptr list_tail)
{
    fat_ptr succ_head = recycle_oid_list.exchange(list_head, std::memory_order_seq_cst);
    object *tail_obj = (object *)list_tail.offset();
    ASSERT(tail_obj->_next == NULL_PTR);
    tail_obj->_next = succ_head;
    std::atomic_thread_fence(std::memory_order_release);    // Let the GC thread know
}

void gc_daemon()
{
    std::unique_lock<std::mutex> lock(gc_lock);
    dense_hash_map<size_t, object_list> scavenged_object_lists;
    scavenged_object_lists.set_empty_key(0);

try_recycle:
    uint64_t reclaimed_count = 0;
    uint64_t reclaimed_nbytes = 0;
    gc_trigger.wait(lock);
    fat_ptr r = recycle_oid_list.load();
    object *r_obj = (object *)r.offset();

    // FIXME(tzwang): for now always start after the head, so we don't have to
    // worry about resetting the head list pointer...
    if (not r_obj or r_obj->_next == NULL_PTR)
        goto try_recycle;
    auto r_prev = r;
    r = r_obj->_next;
    ASSERT(r != NULL_PTR);
    ASSERT(r != r_prev);

    while (1) {
        // need to update tlsn each time, to make sure we can make
        // progress when diving deeper in the list: in general the
        // deeper we dive in it, the newer versions we will see;
        // then we might never get out of the loop if the tlsn is
        // too old, unless we have a threshold of "examined # of
        // oids" or like here, update it at each iteration.
        LSN tlsn = volatile_read(trim_lsn);
        if (tlsn == INVALID_LSN)
            break;

        if (r == NULL_PTR or
            reclaimed_count >= EPOCH_SIZE_COUNT or
            reclaimed_nbytes >= EPOCH_SIZE_NBYTES) {
            break;
        }

        r_obj = (object *)r.offset();
        recycle_oid *r_oid = (recycle_oid *)r_obj->payload();
        ASSERT(r_oid->oa);
        fat_ptr head = oidmgr->oid_get(r_oid->oa, r_oid->oid);
        auto r_next = r_obj->_next;
        ASSERT(r_next != r);
        object *cur_obj = (object *)head.offset();
        if (not cur_obj) {
            // in case it's a delete... remove the oid as if we trimmed it
            deallocate(r);
            ASSERT(r_prev != NULL_PTR);
            object *r_prev_obj = (object *)r_prev.offset();
            volatile_write(r_prev_obj->_next, r_next);
            r = r_next;
            continue;
        }

        // need to start from the first **committed** version, and start
        // trimming after its next, because the head might be still being
        // modified (hence its _next field) and might be gone (tx abort).
        auto clsn = volatile_read(cur_obj->_clsn);
        if (clsn.asi_type() == fat_ptr::ASI_XID)
            cur_obj = (object *)cur_obj->_next.offset();

        // now cur_obj should be the fisrt committed version, continue
        // to the version that can be safely trimmed (the version after
        // cur_obj).
        fat_ptr cur = cur_obj->_next;
        fat_ptr *prev_next = &cur_obj->_next;

        bool trimmed = false;

        // the tx only recycle()s updated oids, so each chain we poke at
        // here *should* have at least 2 *committed* versions. But note
        // that say, two txs, can update the same OID, and they will
        // both add the OID to this list - rmb we don't dedup the list,
        // there might be duplicates; if we trimmed one entry already,
        // the next time we'll probably see cur.offset() == 0. So just
        // remove it, as if it were trimmed (again).
        if (not cur.offset())
            trimmed = true;

        // Fast forward to the **second** version < trim_lsn. Consider that we
        // set safesnap lsn to 1.8, and trim_lsn to 1.6. Assume we have two
        // versions with LSNs 2 and 1.5. We need to keep the one with LSN=1.5
        // although its < trim_lsn; otherwise the tx using safesnap won't be
        // able to find any version available.
        while (cur.offset()) {
            cur_obj = (object *)cur.offset();
            ASSERT(cur_obj);
            clsn = volatile_read(cur_obj->_clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
            prev_next = &cur_obj->_next;
            cur = volatile_read(*prev_next);
            if (LSN::from_ptr(clsn) <= tlsn)
                break;
        }

        while (cur.offset()) {
            cur_obj = (object *)cur.offset();
            ASSERT(cur_obj);
            clsn = volatile_read(cur_obj->_clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
            if (LSN::from_ptr(clsn) <= tlsn) {
                // no need to CAS here if we only have one gc thread
                volatile_write(prev_next->_ptr, 0);
                std::atomic_thread_fence(std::memory_order_release);
                trimmed = true;
                while (cur.offset()) {
                    cur_obj = (object *)cur.offset();
                    reclaimed_nbytes += cur_obj->tuple()->size;
                    reclaimed_count++;
                    ASSERT(cur.size_code() != INVALID_SIZE_CODE);
                    auto size = decode_size_aligned(cur.size_code());
                    ASSERT(size);
                    auto& sol = scavenged_object_lists[size];
                    if (not sol.put(cur)) {
                        central_object_pool.put_object_list(sol);
                        sol.head = sol.tail = NULL_PTR;
                        sol.nobjects = 0;
                        ALWAYS_ASSERT(sol.put(cur));
                    }
                    cur = cur_obj->_next;
                }
                break;
            }
            prev_next = &cur_obj->_next;
            cur = volatile_read(*prev_next);
        }

        if (trimmed) {
            // really recycled something, detach the node, but don't update r_prev
            ASSERT(r_prev != NULL_PTR);
            deallocate(r);
            object *r_prev_obj = (object *)r_prev.offset();
            volatile_write(r_prev_obj->_next, r_next);
        }
        else {
            r_prev = r;
        }
        r = r_next;
    }
#if CHECK_INVARIANTS
    if (reclaimed_nbytes or reclaimed_count)
        printf("GC: reclaimed %lu bytes, %lu objects\n", reclaimed_nbytes, reclaimed_count);
#endif
    goto try_recycle;
}

bool
object_list::put(fat_ptr objptr) {
    if (nobjects == CAPACITY)
        return false;
    ((object *)objptr.offset())->_next = NULL_PTR;
    if (tail == NULL_PTR) {
        head = tail = objptr;
    } else {
        ASSERT(head.offset());
        object* tail_obj = (object *)tail.offset();
        tail_obj->_next = objptr;
        tail = objptr;
    }
    nobjects++;
    return true;
}

object_list*
object_pool::get_object_list(size_t size) {
    size_t aligned_size = align_up(size);
    // peek at it first, don't bother if there's nothing
    if (pool.find(aligned_size) == pool.end()) {
        return NULL;
    }
    lock.lock();
    object_list* ol = pool[aligned_size];
    if (ol and ol->head != NULL_PTR) {
        pool[aligned_size] = ol->next;
    }
    lock.unlock();
    return ol;
}

void
object_pool::put_object_list(object_list& ol) {
    size_t aligned_size = align_up(decode_size_aligned(ol.head.size_code()));
    object_list *new_ol = new object_list();
    memcpy(new_ol, &ol, sizeof(ol));
    lock.lock();
    new_ol->next = pool[aligned_size];
    pool[aligned_size] = new_ol;
    lock.unlock();
}

object*
thread_object_pool::get_object(size_t size) {
    size_t aligned_size = align_up(size);
    auto& list = pool[aligned_size];
    if (list.nobjects == 0) {
        ASSERT(list.head == NULL_PTR);
        ASSERT(list.tail == NULL_PTR);
        // It's the caller's responsibility to refill using put_objects()
        return NULL;
    }
    fat_ptr ptr = list.head;
    ASSERT(ptr != NULL_PTR);
    ASSERT(decode_size_aligned(ptr.size_code()) == aligned_size);
    object *head_obj = (object *)list.head.offset();
    list.head = head_obj->_next;
    if (--list.nobjects == 0) {
        ASSERT(list.head == NULL_PTR);
        list.tail = NULL_PTR;
    }
    return (object *)ptr.offset();
}

void
thread_object_pool::put_objects(object_list& ol) {
    size_t size = decode_size_aligned(ol.head.size_code());
    auto& list = pool[size];
    if (list.head == NULL_PTR) {
        ASSERT(list.tail == NULL_PTR);
        list.head = ol.head;
        list.tail = ol.tail;
    } else {
        object* tail_object = (object *)list.tail.offset();
        ASSERT(tail_object->_next == NULL_PTR);
        tail_object->_next = ol.head;
        list.tail = ol.tail;
    }
    list.nobjects += ol.nobjects;
}
};  // end of namespace
