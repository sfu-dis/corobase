#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"

/* Garbage collection: maintain a lock-free queue for updated tuples.
 * Writers add updated OIDs in the form of <btr, oid> to the queue upon
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

void deallocate(object *p)
{
    ASSERT(p);
    free(p);
}

#ifdef ENABLE_GC
uint64_t EPOCH_SIZE_NBYTES = 1 << 28;
uint64_t EPOCH_SIZE_COUNT = 200000;
int nr_sockets = -1;

static recycle_oid *recycle_oid_head = NULL;
static recycle_oid *recycle_oid_tail = NULL;

#define MSB_MARK (~((~uint64_t{0}) >> 1))

void recycle(uintptr_t table, oid_type oid)
{
    recycle_oid *r = new recycle_oid(table, oid);
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

LSN trim_lsn;
std::condition_variable gc_trigger;
std::mutex gc_lock;
void gc_daemon();

// epochs related
static __thread struct thread_data epoch_tls;
epoch_mgr mm_epochs {{nullptr, &global_init, &get_tls,
                    &thread_registered, &thread_deregistered,
                    &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

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

void global_init(void*)
{
    std::thread t(gc_daemon);
    t.detach();
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
    // So we need this rcu_is_active here because
    // epoch_ended is called not only when an epoch is eneded,
    // but also when threads exit (see epoch.cpp:274-283 in function
    // epoch_mgr::thread_init(). So we need to avoid the latter case
    // as when thread exits it will no longer be in the rcu region
    // created by the scoped_rcu_region in the transaction class.
    LSN *lsn = (LSN *)malloc(sizeof(LSN));
    RCU::rcu_enter();
    *lsn = transaction_base::logger->cur_lsn();
    RCU::rcu_exit();
    return lsn;
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
        volatile_write(trim_lsn._val, lsn._val);
        free(epoch_cookie);
        gc_trigger.notify_all();
    }
}

void
epoch_enter(void)
{
    mm_epochs.thread_enter();
}

void
epoch_exit(void)
{
    mm_epochs.thread_exit();
}

void gc_daemon()
{
    std::unique_lock<std::mutex> lock(gc_lock);
try_recycle:
    uint64_t reclaimed_count = 0;
    uint64_t reclaimed_nbytes = 0;
    gc_trigger.wait(lock);
    LSN tlsn = volatile_read(trim_lsn);
    recycle_oid *r_prev = NULL;
    recycle_oid *r = volatile_read(recycle_oid_head);
    while (1) {
        if (not r or reclaimed_count >= EPOCH_SIZE_COUNT or reclaimed_nbytes >= EPOCH_SIZE_NBYTES)
            break;
        recycle_oid *r_next = volatile_read(r->next);
        if (not r_next)
            break;

        concurrent_btree::tuple_vector_type *v = ((concurrent_btree *)r->btr)->get_tuple_vector();
        oid_type oid = r->oid;

        fat_ptr head = v->begin(oid);
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

        // now cur_obj should be the fisrt committed versions, continue
        // to the version that can be safely trimmed (the version after
        // cur_obj).
        fat_ptr cur = cur_obj->_next;
        fat_ptr *prev_next = &cur_obj->_next;

        bool trimmed = false;
        while (cur.offset()) {
            cur_obj = (object *)cur.offset();
            ASSERT(cur_obj);
            dbtuple *version = (dbtuple *)cur_obj->payload();
            clsn = volatile_read(version->clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
            if (LSN::from_ptr(clsn) < tlsn) {
                // no need to CAS here if we only have one gc thread
                volatile_write(prev_next->_ptr, 0);
                __sync_synchronize();
                trimmed = true;
                while (cur.offset()) {
                    cur_obj = (object *)cur.offset();
                    cur = cur_obj->_next;
                    reclaimed_nbytes += cur_obj->_size;
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
    printf("GC: reclaimed %lu bytes\n", reclaimed_nbytes);
    goto try_recycle;
}
#endif

void *allocate(uint64_t size) {
    void* p = malloc(size);
    ASSERT(p);
#ifdef ENABLE_GC
    if (epoch_tls.nbytes >= EPOCH_SIZE_NBYTES or epoch_tls.counts >= EPOCH_SIZE_COUNT) {
        if (mm_epochs.new_epoch_possible() and mm_epochs.new_epoch())
            epoch_tls.nbytes = epoch_tls.counts = 0;
    }
    epoch_tls.nbytes += size;
    epoch_tls.counts += 1;
#endif
    return p;
}

};  // end of namespace

