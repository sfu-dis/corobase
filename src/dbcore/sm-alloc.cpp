#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"

/* Garbage collection: maintain a lock-free queue for updated tuples.
 * Writers add updated OIDs in the form of <fid, oid> to the queue upon
 * commit, to the tail of the queue. A GC thread periodically consumes
 * queue nodes from the head of the queue and recycle old versions if
 * their clsn < trim_lsn. This way avoids scanning the whole OID array
 * which might be increasing quite fast. Amount of scanning is only
 * related to update footprint.
 *
 * The queue is for now multi-producer-single-consumer, because we only
 * have one GC thread.
 */

namespace MM {

void global_init(void*)
{
#ifdef ENABLE_GC
    std::thread t(gc_daemon);
    t.detach();
#endif
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

uint64_t EPOCH_SIZE_NBYTES = 1 << 28;
uint64_t EPOCH_SIZE_COUNT = 200000;

// global_trim_lsn is the max lsn of versions that are no longer needed,
// i.e., the trim_lsn corresponding to two epochs ago of the most recently
// reclaimed epoch. Note that trim_lsn only records the LSN when the
// *creator* of the versions are left---not the *visitor* of those versions.
// So consider the versions created in epoch N, we can have stragglers up to
// at most epoch N+2; to start a new epoch N+3, N must be reclaimed. This
// means versions last *visited* (not created!) in epoch N are no longer
// needed after epoch N+2 is reclaimed.
#ifdef ENABLE_GC
LSN global_trim_lsn;
LSN trim_lsn[3];
#endif
LSN epoch_end_lsn[3];
LSN safesnap_lsn = INVALID_LSN;

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
    // remember the epoch's LSN and return them together
    char *lsn_epoch = (char *)malloc(sizeof(LSN) + sizeof(epoch_num));
    *((LSN *)lsn_epoch) = epoch_end_lsn[e % 3];
    *((epoch_num *)(lsn_epoch + sizeof(LSN))) = e;
    return (void *)lsn_epoch;
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
    if (lsn != INVALID_LSN) {
        // make epoch N available for transactions using safe snapshot.
        // All transactions that created something in this epoch has gone,
        // so it's impossible for a reader using that epoch's end lsn as
        // both begin and commit timestamp to have conflicts with these
        // transactions. But future transactions needs to start with a
        // pstamp of safesnap_lsn.
        volatile_write(safesnap_lsn._val, lsn._val);
#ifdef ENABLE_GC
        epoch_num epoch = *((epoch_num *)((char *)epoch_cookie + sizeof(LSN)));
        volatile_write(trim_lsn[(epoch + 2) % 3]._val, lsn._val);
        // make epoch N-2 available for recycle after epoch N is reclaimed
        if (epoch >= 2)
            global_trim_lsn = trim_lsn[(epoch - 2) % 3];
#endif
        free(epoch_cookie);
    }
#ifdef ENABLE_GC
    if (global_trim_lsn != INVALID_LSN)
        gc_trigger.notify_all();
#endif
}

epoch_num
epoch_enter(void)
{
    return mm_epochs.thread_enter();
}

void
epoch_exit(LSN s, epoch_num e)
{
    if (epoch_tls.nbytes >= EPOCH_SIZE_NBYTES or epoch_tls.counts >= EPOCH_SIZE_COUNT) {
        // epoch_ended() (which is called by new_epoch() in its critical
        // section) will pick up this safe lsn if new_epoch() succeeded
        // (it could also be set by somebody else who's also doing epoch_exit(),
        // but this is captured before new_epoch succeeds, so we're fine).
        // If instead, we do this in epoch_ended(), that cur_lsn captured
        // actually belongs to the *new* epoch - not safe to trim based on
        // this lsn. The real trim_lsn should be some lsn at the end of the
        // ending epoch, not some lsn after the next epoch.
        epoch_end_lsn[e % 3] = s;
        if (mm_epochs.new_epoch_possible() and mm_epochs.new_epoch())
            epoch_tls.nbytes = epoch_tls.counts = 0;
    }
    mm_epochs.thread_exit();
}

#ifdef ENABLE_GC
std::condition_variable gc_trigger;
std::mutex gc_lock;
void gc_daemon();

static recycle_oid *recycle_oid_head = NULL;
static recycle_oid *recycle_oid_tail = NULL;

#define MSB_MARK (~((~uint64_t{0}) >> 1))

void recycle(oid_array *oa, OID oid)
{
    recycle_oid *r = new recycle_oid(oa, oid);
try_append:
    recycle_oid *tail = volatile_read(recycle_oid_tail);
    if ((uintptr_t)tail & MSB_MARK)
        goto try_append;
    recycle_oid *claimed_tail = (recycle_oid *)((uintptr_t)tail | MSB_MARK);
    ASSERT(not ((uintptr_t)tail & MSB_MARK));
    ASSERT(((uintptr_t)claimed_tail & (~MSB_MARK)) == (uintptr_t)tail);

    // cliam the field so nobody else can change the tail and its next field
    if (not __sync_bool_compare_and_swap(&recycle_oid_tail, tail, claimed_tail))
        goto try_append;

    if (tail)
        volatile_write(tail->next, r);
    else {
        ASSERT(not volatile_read(recycle_oid_head));
        volatile_write(recycle_oid_head, r);
    }

    __sync_synchronize();
    volatile_write(recycle_oid_tail, r);
    __sync_synchronize();   // publish the new tail
}

// fast version, add a chain of updates each time
void recycle(recycle_oid *list_head, recycle_oid *list_tail)
{
try_append:
    recycle_oid *tail = volatile_read(recycle_oid_tail);
    if ((uintptr_t)tail & MSB_MARK)
        goto try_append;
    recycle_oid *claimed_tail = (recycle_oid *)((uintptr_t)tail | MSB_MARK);
    ASSERT(not ((uintptr_t)tail & MSB_MARK));
    ASSERT(((uintptr_t)claimed_tail & (~MSB_MARK)) == (uintptr_t)tail);

    // cliam the field so nobody else can change the tail and its next field
    if (not __sync_bool_compare_and_swap(&recycle_oid_tail, tail, claimed_tail))
        goto try_append;

    if (tail)
        volatile_write(tail->next, list_head);
    else {
        ASSERT(not volatile_read(recycle_oid_head));
        volatile_write(recycle_oid_head, list_head);
    }

    __sync_synchronize();
    volatile_write(recycle_oid_tail, list_tail);
    __sync_synchronize();   // publish the new tail
}

void gc_daemon()
{
    std::unique_lock<std::mutex> lock(gc_lock);
try_recycle:
    uint64_t reclaimed_count = 0;
    uint64_t reclaimed_nbytes = 0;
    gc_trigger.wait(lock);
    recycle_oid *r_prev = NULL;
    recycle_oid *r = volatile_read(recycle_oid_head);
    while (1) {
        // need to update tlsn each time, to make sure we can make
        // progress when diving deeper in the list: in general the
        // deeper we dive in it, the newer versions we will see;
        // then we might never get out of the loop if the tlsn is
        // too old, unless we have a threshold of "examined # of
        // oids" or like here, update it at each iteration.
        LSN tlsn = volatile_read(global_trim_lsn);

        if (not r or reclaimed_count >= EPOCH_SIZE_COUNT or reclaimed_nbytes >= EPOCH_SIZE_NBYTES)
            break;
        recycle_oid *r_next = volatile_read(r->next);
        if (not r_next)
            break;

        fat_ptr head = oidmgr->oid_get(r->oa, r->oid);
        if (not head.offset()) {
            // in case it's a delete... remove the oid as if we trimmed it
            delete r;
            if (r_prev) {
                volatile_write(r_prev->next, r_next);
                r = r_next;
            }
            else {  // recycled the head
                volatile_write(recycle_oid_head, r_next);
                r = recycle_oid_head;
            }
            continue;
        }

        // need to start from the first **committed** version, and start
        // trimming after its next, because the head might be still being
        // modified (hence its _next field) and might be gone (tx abort).
        object *cur_obj = (object *)head.offset();
        dbtuple *head_version = (dbtuple *)cur_obj->payload();
        auto clsn = volatile_read(head_version->clsn);
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

        while (cur.offset()) {
            cur_obj = (object *)cur.offset();
            ASSERT(cur_obj);
            dbtuple *version = (dbtuple *)cur_obj->payload();
            clsn = volatile_read(version->clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
            if (LSN::from_ptr(clsn) <= tlsn) {
                // no need to CAS here if we only have one gc thread
                volatile_write(prev_next->_ptr, 0);
                __sync_synchronize();
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
            cur = volatile_read( *prev_next );
        }

        if (trimmed) {
            // really recycled something, detach the node
            delete r;
            if (r_prev) {
                volatile_write(r_prev->next, r_next);
                r = r_next;
            }
            else {  // recycled the head
                volatile_write(recycle_oid_head, r_next);
                r = recycle_oid_head;
            }
        }
        else {
            r_prev = r;
            r = r_next;
        }
    }
    if (reclaimed_nbytes or reclaimed_count)
        printf("GC: reclaimed %lu bytes, %lu objects\n", reclaimed_nbytes, reclaimed_count);
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
    uint64_t tlsn = volatile_read(MM::global_trim_lsn).offset();

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
#endif

};  // end of namespace
