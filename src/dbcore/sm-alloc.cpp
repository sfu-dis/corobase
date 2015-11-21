#include <sys/mman.h>
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
std::atomic<recycle_oid*> recycle_oid_list(NULL);

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

void global_init(void*)
{
    if (sysconf::enable_gc) {
        std::thread t(gc_daemon);
        t.detach();
    }
}

// epochs related
static __thread struct thread_data epoch_tls;
epoch_mgr mm_epochs {{nullptr, &global_init, &get_tls,
                    &thread_registered, &thread_deregistered,
                    &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

void *allocate(uint64_t size) {
    void* p = malloc(size);
    ASSERT(p);
    epoch_tls.nbytes += size;
    epoch_tls.counts += 1;
    return p;
}

void deallocate(void *p)
{
    ASSERT(p);
    free(p);
}

// epoch mgr callbacks
epoch_mgr::tls_storage *
get_tls(void*)
{
    static __thread epoch_mgr::tls_storage s;
    return &s;
}

void register_thread()
{
    mm_epochs.thread_init();
}

void deregister_thread()
{
    mm_epochs.thread_fini();
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

    ASSERT(epoch_reclaim_lsn[e % 3] == INVALID_LSN);
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

epoch_num
epoch_enter(void)
{
    return mm_epochs.thread_enter();
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

void recycle(recycle_oid *list_head, recycle_oid *list_tail)
{
    recycle_oid *succ_head = recycle_oid_list.exchange(list_head, std::memory_order_seq_cst);
    ASSERT(not list_tail->next);
    list_tail->next = succ_head;
    std::atomic_thread_fence(std::memory_order_release);    // Let the GC thread know
}

void gc_daemon()
{
    std::unique_lock<std::mutex> lock(gc_lock);
try_recycle:
    uint64_t reclaimed_count = 0;
    uint64_t reclaimed_nbytes = 0;
    gc_trigger.wait(lock);
    recycle_oid *r = recycle_oid_list.load();

    // FIXME(tzwang): for now always start after the head, so we don't have to
    // worry about resetting the head list pointer...
    if (not r or not r->next)
        goto try_recycle;
    auto *r_prev = r;
    r = r->next;
    ASSERT(r);

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

        if (not r or reclaimed_count >= EPOCH_SIZE_COUNT or reclaimed_nbytes >= EPOCH_SIZE_NBYTES)
            break;

        ASSERT(r->oa);
        fat_ptr head = oidmgr->oid_get(r->oa, r->oid);
        auto* r_next = r->next;
        ASSERT(r_next != r);
        object *cur_obj = (object *)head.offset();
        if (not cur_obj) {
            // in case it's a delete... remove the oid as if we trimmed it
            delete r;
            ASSERT(r_prev);
            volatile_write(r_prev->next, r_next);
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
                    cur = cur_obj->_next;
                    reclaimed_nbytes += cur_obj->tuple()->size;
                    reclaimed_count++;
                    deallocate(cur_obj);
                }
                break;
            }
            prev_next = &cur_obj->_next;
            cur = volatile_read(*prev_next);
        }

        if (trimmed) {
            // really recycled something, detach the node, but don't update r_prev
            ASSERT(r_prev);
            delete r;
            volatile_write(r_prev->next, r_next);
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

#ifdef REUSE_OBJECTS
object_pool *get_object_pool()
{
    static __thread object_pool myop;
    return &myop;
}

object *
object_pool::get(size_t size)
{
    int order = get_order(size);
    reuse_object *r = head[order];
    reuse_object *r_prev = NULL;
    uint64_t tlsn = volatile_read(MM::trim_lsn).offset();

    while (r and r->cstamp > tlsn) {
        object *p = r->obj;
        ASSERT(r->cstamp);
        if (p->_size >= size) {
            if (r_prev)
                r_prev->next = r->next;
            else {
                head[order] = r->next;
                if (not head[order])
                    tail[order] = NULL;
            }
            delete r;
            return p;
        }
        r_prev = r;
        r = r->next;
    };
    return NULL;
}

void
object_pool::put(uint64_t c, object *p)
{
    int order = get_order(p->_size);
    reuse_object *r = new reuse_object(c, p);
    if (not tail[order])
        head[order] = tail[order] = r;
    else {
        tail[order]->next = r;
        tail[order] = r;
    }
}
#endif

};  // end of namespace
