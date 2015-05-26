#include "macros.h"
#include "amd64.h"
#include "txn.h"
#include "lockguard.h"
#include "scopedperf.hh"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;
using namespace util;
using namespace TXN;

transaction::transaction(uint64_t flags, str_arena &sa)
  : flags(flags), xid(TXN::xid_alloc()), xc(xid_get_context(xid)), sa(&sa)
{
#ifdef ENABLE_GC
    epoch = MM::epoch_enter();
#ifdef REUSE_OBJECTS
    op = MM::get_object_pool();
#endif
#endif
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::NodeLockRegionBegin();
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    serial_register_tx(xid);
#endif
#ifdef PHANTOM_PROT_NODE_SET
    absent_set.set_empty_key(NULL);    // google dense map
#endif
    RCU::rcu_enter();
    log = logmgr->new_tx_log();
}

transaction::~transaction()
{
#ifdef PHANTOM_PROT_TABLE_LOCK
    // release table locks
    for (auto l : table_locks)
        object_vector::unlock(l);
#endif

    // transaction shouldn't fall out of scope w/o resolution
    // resolution means TXN_EMBRYO, TXN_CMMTD, and TXN_ABRTD
    INVARIANT(state() != TXN_ACTIVE && state() != TXN_COMMITTING);

    RCU::rcu_exit();
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::AssertAllNodeLocksReleased();
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    serial_deregister_tx(xid);
#endif
#ifdef ENABLE_GC
    MM::epoch_exit(xc->end, epoch);
#endif
    xid_free(xid);    // must do this after epoch_exit, which uses xc.end
}

void
transaction::abort_impl()
{
    // Mark the dirty tuple as invalid, for oid_get_version to
    // move on more quickly.
    volatile_write(xc->state, TXN_ABRTD);

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    // Go over the read set first, to deregister from the tuple
    // asap so the updater won't wait for too long.
    for (auto &r : read_set) {
        ASSERT(not r->is_defunct());
        ASSERT(r->clsn.asi_type() == fat_ptr::ASI_LOG);
        // remove myself from reader list
        serial_deregister_reader_tx(r);
    }
#endif

    for (auto &w : write_set) {
        dbtuple *tuple = w.new_tuple;
        ASSERT(tuple);
        ASSERT(XID::from_ptr(tuple->clsn) == xid);
        if (tuple->is_defunct())   // for repeated overwrites
            continue;
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
        if (tuple->next())
            volatile_write(tuple->next()->sstamp, NULL_PTR);
#endif
        oidmgr->oid_unlink(w.oa, w.oid, tuple);
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
        object *obj = (object *)((char *)tuple - sizeof(object));
        op->put(xc->end.offset(), obj);
#endif
    }

#ifdef USE_PARALLEL_SSI
    // PLEASE REALLY DO FIXME (tzwang): Theoretically skipping the pre_commit()
    // here should give better performance because it saves a CAS in the log,
    // but for SSI doing the pre_commit() gives better performance, no matter
    // with or without --retry-aborted-transactions, because SSI tends to abort
    // the unlucky T2 (updater), putting tremendous pressure on
    // register/deregister_reader_tx. See also the comment in do_tree_put().
    //
    // For SSN and SI, the above doesn't apply; avoiding pre_commit() slightly
    // improvers performance.
    if (likely(state() != TXN_COMMITTING))
        log->pre_commit();
#endif
    log->discard();
}

namespace {
inline const char *
transaction_state_to_cstr(txn_state state)
{
    switch (state) {
        case TXN_EMBRYO: return "TXN_EMBRYO";
        case TXN_ACTIVE: return "TXN_ACTIVE";
        case TXN_ABRTD: return "TXN_ABRTD";
        case TXN_CMMTD: return "TXN_CMMTD";
        case TXN_COMMITTING: return "TXN_COMMITTING";
        case TXN_INVALID: return "TXN_INVALID";
    }
    ALWAYS_ASSERT(false);
    return 0;
}

inline std::string
transaction_flags_to_str(uint64_t flags)
{
    bool first = true;
    std::ostringstream oss;
    if (flags & transaction::TXN_FLAG_LOW_LEVEL_SCAN) {
        oss << "TXN_FLAG_LOW_LEVEL_SCAN";
        first = false;
    }
    if (flags & transaction::TXN_FLAG_READ_ONLY) {
        if (first)
            oss << "TXN_FLAG_READ_ONLY";
        else
            oss << " | TXN_FLAG_READ_ONLY";
        first = false;
    }
    return oss.str();
}
};  // end of namespace

void
transaction::dump_debug_info() const
{
    std::cerr << "Transaction (obj=" << util::hexify(this) << ") -- state "
              << transaction_state_to_cstr(state()) << std::endl;
    std::cerr << "  Flags: " << transaction_flags_to_str(flags) << std::endl;
}

rc_t
transaction::commit()
{
    switch (state()) {
        case TXN_EMBRYO:
        case TXN_ACTIVE:
            volatile_write(xc->state, TXN_COMMITTING);
            break;
        case TXN_CMMTD:
        case TXN_COMMITTING:
        case TXN_ABRTD:
        case TXN_INVALID:
            ALWAYS_ASSERT(false);
    }

    INVARIANT(log);
    // get clsn, abort if failed
    xc->end = log->pre_commit();
    if (xc->end == INVALID_LSN)
        return rc_t{RC_ABORT_INTERNAL};

#ifdef USE_PARALLEL_SSN
    return parallel_ssn_commit();
#elif defined USE_PARALLEL_SSI
    return parallel_ssi_commit();
#else
    return si_commit();
#endif
}

#ifdef USE_PARALLEL_SSN
rc_t
transaction::parallel_ssn_commit()
{
    auto cstamp = xc->end.offset();

    // note that sstamp comes from reads, but the read optimization might
    // ignore looking at tuple's sstamp at all, so if tx sstamp is still
    // the initial value so far, we need to initialize it as cstamp. (so
    // that later we can fill tuple's sstamp as cstamp in case sstamp still
    // remained as the initial value.) Consider the extreme case where
    // old_version_threshold = 0: means no read set at all...
    if (xc->sstamp > cstamp)
        xc->sstamp = cstamp;

    // find out my largest predecessor (\eta) and smallest sucessor (\pi)
    // for reads, see if sb. has written the tuples - look at sucessor lsn
    // for writes, see if sb. has read the tuples - look at access lsn

    for (auto &w : write_set) {
        dbtuple *tuple = w.new_tuple;
        if (tuple->is_defunct())    // repeated overwrites
            continue;

        // go to the precommitted or committed version I (am about to)
        // overwrite for the reader list
        dbtuple *overwritten_tuple = tuple->next();
        ASSERT(not overwritten_tuple or
               ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
               (uint64_t)((char *)overwritten_tuple - sizeof(object)));
        if (not overwritten_tuple) // insert
            continue;

        ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);

        // need access stamp , i.e., who read this version that I'm trying to overwrite?
        readers_list::bitmap_t readers = serial_get_tuple_readers(overwritten_tuple);
        while (readers) {
            int i = __builtin_ctz(readers);
            ASSERT(i >= 0 and i < 24);
            readers &= (readers-1);
        get_reader:
            XID rxid = volatile_read(rlist.xids[i]);
            if (not rxid._val or rxid == xc->owner)
                continue; // ignore invalid entries and ignore my own reads
            xid_context *reader_xc = xid_get_context(rxid);
            if (not reader_xc)
                continue;
            // copy everything before doing anything
            auto reader_end = volatile_read(reader_xc->end).offset();
            ASSERT(reader_end != cstamp);
            auto reader_begin = volatile_read(reader_xc->begin).offset();
            auto reader_owner = volatile_read(reader_xc->owner);
            if (reader_owner != rxid)
                goto get_reader;    // XXX (tzwang): context change - basically this
                                    // means the next get_reader will yield the result
                                    // that the reader's destined to commit after me
            if (not overwritten_tuple->is_old(xc)) {
                // There's no need to see the actual result and then set
                // pstamp to reader_end if reader_end is larger: readers
                // on the bitmap might go at any time. If by the time we
                // drill down to see the real context represented by the
                // bit and found it should commit before me, we wait for
                // it to finish; otherwise it might already be another
                // transactions that's destined to commit after me - we
                // will have to re-read the version's xstamp anyway to
                // not miss this (possible) pstamp change.
                //
                // So here we just need to wait for all those we found on
                // the list and those who should commit before me to go away.
                if (reader_end and reader_end < cstamp)
                    wait_for_commit_result(rxid, reader_xc);
            }
            else {  // old version
                auto tuple_bs = volatile_read(overwritten_tuple->bstamp);
                // I (as the writer) need to backoff if the reader has the
                // possibility of having read the version, and it is or will
                // be serialized after me (ie in-flight, end=0).
                if ((not reader_end or reader_end >= cstamp) and reader_begin <= tuple_bs)
                    return rc_t{RC_ABORT_RW_CONFLICT};
                xc->pstamp = cstamp - 1;
            }
        }

        // do this after scanned all readers to catch the case where
        // a tx got cstamp < my cstamp but left the readers list earlier
        // before I can get a hold on it in the above loop. For example,
        // if my cstamp=5, another reader R has cstamp=3; when I grabbed the
        // readers bitmap, R's bit is set. But before I looped to R's position
        // and start checking, R already left, now I'll be seeing another tx's
        // context and might see it will be committed after me (or not even
        // in precommit) and then miss the xstamp R set on the version. So I
        // need to read the version's xstamp again to make sure.
        //
        // Note: version.xstamp is set during post-commit, then the above
        // reasoning is actually assuming we cannot mark a tx as committed
        // before stamping read versions' xstamp!
        //
        // Also note: we only care about transactions (readers) who committed
        // **before** me, so it's enough to capture the readers bitmap once in
        // the above loop, because any tx change in the bitmap means the "new"
        // tx who inherited the position in the bitmap will commit after me.
        // So there's no need to "lock" the readers bitmap whatsoever.
        //
        // But if xc->pstamp is already >= my cstamp, I'll have to set my pstamp
        // to cstamp-1 to account for those context changes... this is a source
        // of false +ves.
        auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
        if (tuple_xstamp >= cstamp)
            xc->pstamp = cstamp - 1;
        else if (xc->pstamp < tuple_xstamp)
            xc->pstamp = tuple_xstamp;

    }
    ASSERT(xc->pstamp <= cstamp - 1);

    for (auto &r : read_set) {
    try_get_sucessor:
        // so tuple should be the committed version I read
        ASSERT(r->clsn.asi_type() == fat_ptr::ASI_LOG);

        // read tuple->slsn to a local variable before doing anything relying on it,
        // it might be changed any time...
        fat_ptr sucessor_clsn = volatile_read(r->sstamp);
        if (sucessor_clsn == NULL_PTR)
            continue;

        // overwriter in progress?
        if (sucessor_clsn.asi_type() == fat_ptr::ASI_XID) {
            XID successor_xid = XID::from_ptr(sucessor_clsn);
            xid_context *sucessor_xc = xid_get_context(successor_xid);
            if (not sucessor_xc)
                goto try_get_sucessor;
            auto successor_owner = volatile_read(sucessor_xc->owner);
            if (successor_owner == xc->owner)  // myself
                continue;

            // read everything before doing anything
            auto sucessor_end = volatile_read(sucessor_xc->end).offset();
            if (successor_owner != successor_xid)
                goto try_get_sucessor;

            // overwriter might haven't committed, be commited after me, or before me
            // we only care if the successor is committed *before* me.
            if (sucessor_end and sucessor_end < cstamp) {
                auto cr = wait_for_commit_result(successor_xid, sucessor_xc);
                // context change, previous overwriter must be gone,
                // re-read sstamp (should see ASI_LOG this time).
                if (cr == TXN_INVALID)
                    goto try_get_sucessor;
                else if (cr == TXN_CMMTD) {
                    if (sucessor_end < xc->sstamp)
                        xc->sstamp = sucessor_end;
                }
            }
        }
        else {
            // overwriter already fully committed/aborted or no overwriter at all
            ASSERT(sucessor_clsn.asi_type() == fat_ptr::ASI_LOG);
            uint64_t tuple_sstamp = sucessor_clsn.offset();
            if (tuple_sstamp and tuple_sstamp < xc->sstamp)
                xc->sstamp = tuple_sstamp;
        }
    }

    if (not ssn_check_exclusion(xc))
        return rc_t{RC_ABORT_SERIAL};

#ifdef PHANTOM_PROT_NODE_SET
    if (not check_phantom())
        return rc_t{RC_ABORT_PHANTOM};
#endif

    // ok, can really commit if we reach here
    log->commit(NULL);

#ifdef ENABLE_GC
    recycle_oid *updated_oids_head = NULL;
    recycle_oid *updated_oids_tail = NULL;
#endif
    // post-commit: stuff access stamps for reads; init new versions
    for (auto &w : write_set) {
        dbtuple *tuple = w.new_tuple;
        if (tuple->is_defunct())
            continue;
        tuple->do_write();
        dbtuple *next_tuple = tuple->next();
        ASSERT(not next_tuple or
               ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
               (uint64_t)((char *)next_tuple - sizeof(object)));
        if (next_tuple) {   // update, not insert
            ASSERT(volatile_read(next_tuple->clsn).asi_type());
            ASSERT(xc->sstamp and xc->sstamp != ~uint64_t{0});
            ASSERT(XID::from_ptr(next_tuple->sstamp) == xid);
            volatile_write(next_tuple->sstamp, LSN::make(xc->sstamp, 0).to_log_ptr());
            ASSERT(next_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
        }
        volatile_write(tuple->xstamp, cstamp);
        volatile_write(tuple->clsn, xc->end.to_log_ptr());
        ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
        if (tuple->next()) {
            // construct the (sub)list here so that we have only one CAS per tx
            recycle_oid *r = new recycle_oid(w.fid, w.oid);
            if (not updated_oids_head)
                updated_oids_head = updated_oids_tail = r;
            else {
                updated_oids_tail->next = r;
                updated_oids_tail = r;
            }
        }
#endif
    }

#ifdef ENABLE_GC
    if (updated_oids_head) {
        ASSERT(updated_oids_tail);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }
#endif

    for (auto &r : read_set) {
        ASSERT(r->clsn.asi_type() == fat_ptr::ASI_LOG);
        uint64_t xlsn;
        do {
            xlsn = volatile_read(r->xstamp);
        }
        while (xlsn < cstamp and not __sync_bool_compare_and_swap(&r->xstamp, xlsn, cstamp));
        // remove myself from readers set, so others won't see "invalid XID" while enumerating readers
        serial_deregister_reader_tx(r);
    }

    // change state
    // Must do it here, after setting xstamps; see an explanation above
    // when iterating the write set.
    volatile_write(xc->state, TXN_CMMTD);

    return rc_t{RC_TRUE};
}
#elif defined(USE_PARALLEL_SSI)
rc_t
transaction::parallel_ssi_commit()
{
    // tzwang: The race between the updater (if any) and me (as the reader) -
    // A reader publishes its existence by calling serial_register_reader().
    // The updater might not notice this if it already checked this tuple
    // before the reader published its existence. In this case, the writer
    // actually thinks nobody is reading the version that's being overwritten.
    // So as the reader, we need to make sure we know the existence of the
    // updater during precommit. We do this by checking the tuple's sstamp
    // (and it's basically how this parallel pre-commit works).
    //
    // Reader's protocol:
    // 1. If sstamp is not set: no updater yet, but if later an updater comes,
    //    it will enter precommit after me (if it can survive). So by the time
    //    the updater started to look at the readers bitmap, if I'm still in
    //    precommit, I need to make sure it sees me on the bitmap (trivial, as
    //    I won't change any bitmaps after enter precommit); or if I committed,
    //    I need to make sure it sees my updated xstamp. This essentially means
    //    the reader shouldn't deregister from the bitmap before setting xstamp.
    //
    // 2. If sstamp is ASI_LOG: the easy case, updater already committed, do
    //    usually SSI checks.
    //
    // 3. If sstamp is ASI_XID: the most complicated case, updater still active.
    //    a) If the updater has a cstamp < my cstamp: it will commit before
    //       me, so reader should spin on it and find out the final sstamp
    //       result, then do usual SSI checks.
    //
    //    b) If the updater has a cstamp > my cstamp: the writer entered
    //       precommit after I did, so it definitely knows my existence -
    //       b/c when it entered precommit, I won't change any bitmap's
    //       bits (b/c I'm already in precommit). The updater should check
    //       the reader's cstamp (which will be the version's xstamp), i.e.,
    //       the updater will spin if if finds the reader is in pre-commit;
    //       otherwise it will read the xstamp directly from the version.
    //
    //    c) If the updater doesn't have a cstamp: similar to 1 above),
    //       it will know about my existence after entered precommit. The
    //       rest is the same - reader has the responsibility to make sure
    //       xstamp or bitmap reflect its visibility.
    //
    //    d) If the updater has a cstamp < my cstamp: I need to spin until
    //       the updater has left to hold my position in the bitmap, so
    //       that the updater can know that I'm a concurrent reader. This
    //       can be done in post-commit, right before I have to pull myself
    //       out from the bitmap.
    //
    //  The writer then doesn't have much burden, it just needs to take a look
    //  at the readers bitmap and abort if any reader is still active or any
    //  xstamp > ct3; overwritten versions' xstamp are guaranteed to be valid
    //  because the reader won't remove itself from the bitmap unless it updated
    //  v.xstamp.

    auto cstamp = xc->end.offset();

    // get the smallest s1 in each tuple we have read (ie, the smallest cstamp
    // of T3 in the dangerous structure that clobbered our read)
    uint64_t ct3 = xc->ct3;   // this will be the s2 of versions I clobbered

    for (auto &r : read_set) {
    get_overwriter:
        fat_ptr overwriter_clsn = volatile_read(r->sstamp);
        if (overwriter_clsn == NULL_PTR)
            continue;

        uint64_t tuple_s1 = 0;
        if (overwriter_clsn.asi_type() == fat_ptr::ASI_XID) {
            XID ox = XID::from_ptr(overwriter_clsn);
            if (ox == xc->owner)    // myself
                continue;
            ASSERT(ox != xc->owner);
            xid_context *overwriter_xc = xid_get_context(ox);
            if (not overwriter_xc)
                goto get_overwriter;
            // read what i need before verifying ownership
            uint64_t overwriter_end = volatile_read(overwriter_xc->end).offset();
            if (volatile_read(overwriter_xc->owner) != ox)
                goto get_overwriter;
            // Spin if the overwriter entered precommit before me: need to
            // find out the final sstamp value.
            // => the updater maybe doesn't know my existence and commit.
            if (overwriter_end and overwriter_end < cstamp) {
                auto cr = wait_for_commit_result(ox, overwriter_xc);
                if (cr == TXN_INVALID)  // context change, retry
                    goto get_overwriter;
                else if (cr == TXN_CMMTD)
                    tuple_s1 = overwriter_end;
            }
        }
        else {    // already committed, read tuple's sstamp
            ASSERT(overwriter_clsn.asi_type() == fat_ptr::ASI_LOG);
            tuple_s1 = volatile_read(overwriter_clsn).offset();
        }

        if (tuple_s1 and (not ct3 or ct3 > tuple_s1))
            ct3 = tuple_s1;

        // Now the updater (if exists) should've already concluded and stamped
        // s2 - requires updater to change state to CMMTD only after setting
        // all s2 values.
        if (volatile_read(r->s2))
            return {RC_ABORT_SERIAL};
        // Release read lock (readers bitmap) after setting xstamp in post-commit
    }

    if (ct3) {
        // now see if I'm the unlucky T2
        uint64_t max_xstamp = 0;
        for (auto &w : write_set) {
            if (w.new_tuple->is_defunct())
                continue;
            dbtuple *overwritten_tuple = w.new_tuple->next();
            if (not overwritten_tuple)
                continue;
            // Note: the bits representing readers that will commit **after**
            // me are stable; those representing readers older than my cstamp
            // could go away any time. But I can look at the version's xstamp
            // in that case. So the reader should make sure when it goes away
            // from the bitmap, xstamp is ready to be read by the updater.
            readers_list::bitmap_t readers = serial_get_tuple_readers(overwritten_tuple, true);
            while (readers) {
                int i = __builtin_ctz(readers);
                ASSERT(i >= 0 and i < 24);
                readers &= (readers-1);
                XID rxid = volatile_read(rlist.xids[i]);
                ASSERT(rxid != xc->owner);
                if (not rxid._val)
                    continue; // ignore invalid entries and ignore my own reads
                xid_context *reader_xc = xid_get_context(rxid);
                if (not reader_xc)
                    continue;
                // copy everything before doing anything
                auto reader_end = volatile_read(reader_xc->end).offset();
                ASSERT(reader_end != cstamp);
                auto reader_owner = volatile_read(reader_xc->owner);
                if (reader_owner != rxid) {
                    // Context change: The guy I saw on the bitmap is gone - it
                    // must had a cstamp older than mine (otherwise it couldn't
                    // go away before me), or given up even before entered pre-
                    // commit. So I should read the xstamp in case it did commit.
                    //
                    // No need to worry about the new guy who occupied this bit:
                    // it'll spin on me if I'm still after pre-commit to maintain
                    // its position in the bitmap.
                    auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
                    // xstamp might still be 0 - if the reader aborted
                    if (max_xstamp < tuple_xstamp)
                        max_xstamp = tuple_xstamp;
                    if (ct3 and max_xstamp >= ct3)
                        return {RC_ABORT_SERIAL};
                }
                else if (not reader_end) {
                    // Reader not in precommit yet, and not sure if it is likely
                    // to commit. But aborting the pivot is easier here, so we
                    // just abort (betting the reader is likely to succeed).
                    //
                    // Another way is don't do anything here, reader will notice
                    // my presence or my legacy (sstamp on the version) once
                    // entered precommit; this might cause deadlock tho.
                    return {RC_ABORT_SERIAL};
                }
                else if (reader_end < cstamp) {
                    // This reader_end will be the version's xstamp if it committed
                    if (spin_for_xstamp(rxid, reader_xc)) {
                        if (max_xstamp < reader_end)
                            max_xstamp = reader_end;
                        if (ct3 and max_xstamp >= ct3)
                            return {RC_ABORT_SERIAL};
                    }
                }
                else {
                    // Reader will commit after me, ie its cstamp will be > ct3
                    // (b/c ct3 is the first committed in the dangerous structure)
                    // and will not release its seat in the bitmap until I'm gone
                    return {RC_ABORT_SERIAL};
                }
            }
        }
    }

#ifdef PHANTOM_PROT_NODE_SET
    if (not check_phantom())
        return rc_t{RC_ABORT_PHANTOM};
#endif

    // survived!
    log->commit(NULL);

#ifdef ENABLE_GC
    recycle_oid *updated_oids_head = NULL;
    recycle_oid *updated_oids_tail = NULL;
#endif
    // stamp overwritten versions, stuff clsn
    for (auto &w : write_set) {
        dbtuple* tuple = w.new_tuple;
        if (tuple->is_defunct())
            continue;
        tuple->do_write();
        dbtuple *overwritten_tuple = tuple->next();
        if (overwritten_tuple) {    // update
            ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);
            volatile_write(overwritten_tuple->sstamp, LSN::make(cstamp, 0).to_log_ptr());
            if (ct3) {
                ASSERT(not overwritten_tuple->s2);
                volatile_write(overwritten_tuple->s2, ct3);
            }
        }
        volatile_write(tuple->xstamp, cstamp);
        tuple->clsn = xc->end.to_log_ptr();
        INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
        if (tuple->next()) {
            // construct the (sub)list here so that we have only one CAS per tx
            recycle_oid *r = new recycle_oid(w.fid, w.oid);
            if (not updated_oids_head)
                updated_oids_head = updated_oids_tail = r;
            else {
                updated_oids_tail->next = r;
                updated_oids_tail = r;
            }
        }
#endif
    }

    // update xstamps in read versions
    for (auto &r : read_set) {
        // no need to look into write set and skip: do_tuple_read will
        // skip inserting to read set if it's already in write set; it's
        // possible to see a tuple in both read and write sets, only if
        // the tuple is first read, then updated - updating the xstamp
        // of such a tuple won't hurt, and it eliminates unnecessary
        // cycles spent on hashtable.
        uint64_t xstamp;
        do {
            xstamp = volatile_read(r->xstamp);
        }
        while (xstamp < cstamp and not __sync_bool_compare_and_swap(&r->xstamp, xstamp, cstamp));
    }

    // NOTE: make sure this happens after populating log block,
    // otherwise readers will see inconsistent data!
    // This is where (committed) tuple data are made visible to readers
    //
    // This needs to happen after setting overwritten tuples' s2, b/c
    // the reader needs to check this during pre-commit.
    //
    // After this point xstamp is stable and available to updaters.
    //
    // It's important to do this before doing the below spin, otherwise
    // we might deadlock with some updater that's trying to use my cstamp
    // as stable xstamp.
    volatile_write(xc->state, TXN_CMMTD);

    for (auto &r : read_set) {
        // wait for the updater that pre-committed before me to go
        // so effectively means I'm holding my position on in the bitmap
        // and preventing it from being reused by another reader before
        // the overwriter leaves. So the overwriter will be able to see
        // a stable readers bitmap and tell if there's an active reader
        // and if so then whether it has to abort because it found itself
        // being an unlucky T2.
        auto sstamp = volatile_read(r->sstamp);
        if (sstamp.asi_type() == fat_ptr::ASI_XID) {
            XID oxid = XID::from_ptr(sstamp);
            if (oxid != this->xid) {    // exclude myself
                xid_context *ox = xid_get_context(oxid);
                if (ox) {
                    auto ox_end = volatile_read(ox->end).offset();
                    auto ox_owner = volatile_read(ox->owner);
                    if (ox_owner == oxid and ox_end and ox_end < cstamp)
                        wait_for_commit_result(oxid, ox);
                }
                // if !ox or ox_owner != oxid then the guy is
                // already gone, don't bother
            }
        }
        // now it's safe to release my seat!
        // Need to do this after setting xstamp, so that the updater can
        // see the xstamp if it didn't find the bit in the bitmap is set.
        serial_deregister_reader_tx(r);
    }

    // GC stuff, do it out of precommit
#ifdef ENABLE_GC
    if (updated_oids_head) {
        ASSERT(updated_oids_tail);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }
#endif

    return rc_t{RC_TRUE};
}
#else
rc_t
transaction::si_commit()
{
#ifdef PHANTOM_PROT_NODE_SET
    if (not check_phantom())
        return rc_t{RC_ABORT_PHANTOM};
#endif

    log->commit(NULL);    // will populate log block

    // post-commit cleanup: install clsn to tuples
    // (traverse write-tuple)
    // stuff clsn in tuples in write-set
#ifdef ENABLE_GC
    recycle_oid *updated_oids_head = NULL;
    recycle_oid *updated_oids_tail = NULL;
#endif
    for (auto &w : write_set) {
        dbtuple* tuple = w.new_tuple;
        if (tuple->is_defunct())
            continue;
        tuple->do_write();
        tuple->clsn = xc->end.to_log_ptr();
        INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
        if (tuple->next()) {
            // construct the (sub)list here so that we have only one CAS per tx
            recycle_oid *r = new recycle_oid(w.fid, w.oid);
            if (not updated_oids_head)
                updated_oids_head = updated_oids_tail = r;
            else {
                updated_oids_tail->next = r;
                updated_oids_tail = r;
            }
        }
#endif
    }

    // NOTE: make sure this happens after populating log block,
    // otherwise readers will see inconsistent data!
    // This is where (committed) tuple data are made visible to readers
    volatile_write(xc->state, TXN_CMMTD);

#ifdef ENABLE_GC
    if (updated_oids_head) {
        ASSERT(updated_oids_tail);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }
#endif

    return rc_t{RC_TRUE};
}
#endif

#ifdef PHANTOM_PROT_NODE_SET
// returns true if btree versions have changed, ie there's phantom
bool
transaction::check_phantom()
{
    for (auto &r : absent_set) {
        const uint64_t v = concurrent_btree::ExtractVersionNumber(r.first);
        if (unlikely(v != r.second.version))
            return false;
    }
    return true;
}
#endif

// FIXME: tzwang: note: we only try once in this function. If it
// failed (returns false) then the caller (supposedly do_tree_put)
// should fallback to normal update procedure.
bool
transaction::try_insert_new_tuple(
    concurrent_btree *btr,
    const varstr *key,
    const varstr *value,
    FID fid)
{
    INVARIANT(key);
    object *object = object::create_tuple_object(value, false);
    dbtuple *tuple = object->tuple();
    tuple->clsn = xid.to_ptr();

#ifdef PHANTOM_PROT_TABLE_LOCK
    // here we return false if some reader is already scanning.
    // the caller (do_tree_put) will detect this return value (false)
    // and then do a tree search. If the tree search succeeded, then
    // somebody else has acquired the lock and successfully inserted
    // so we need to fallback to update; otherwise the tree search
    // will return false, ie no index, so we abort.
    //
    // drawback of this implementation is we can't differentiate aborts
    // due to failure of acquiring table lock.
    table_lock_t *l = tuple_vector->lock_ptr();
    table_lock_set_t::iterator it = std::find(table_locks.begin(), table_locks.end(), l);
    bool instant_xlock = false;

    if (it == table_locks.end()) {
        // not holding (in S/SIX mode), grab it
        if (not object_vector::lock(l, TABLE_LOCK_X)) {
            return false;
            //while (not object_vector::lock(l, true)); // spin, super slow..
        }
        // before X lock is granted, there's no reader
        // after the insert, readers can see the insert freely
        // X lock can be instant, no need to add lock to lock list
        instant_xlock = true;
    }
    else {
        if (not object_vector::upgrade_lock(l))
            return false;
    }
    ASSERT((*l & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX or
           (*l & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_X);
#endif

    OID oid = oidmgr->alloc_oid(fid);
    fat_ptr new_head = fat_ptr::make(object, INVALID_SIZE_CODE, 0);
    oidmgr->oid_put_new(btr->tuple_vec(), oid, new_head);
#ifdef PHANTOM_PROT_TABLE_LOCK
    if (not tuple_vector->put(oid, new_head)) {
        if (instant_xlock)
            object_vector::unlock(l);
        return false;
    }
#endif

    typename concurrent_btree::insert_info_t ins_info;
    if (unlikely(!btr->insert_if_absent(varkey(key), fid, oid, tuple, &ins_info))) {
        oidmgr->oid_unlink(btr->tuple_vec(), oid, tuple);
#ifdef PHANTOM_PROT_TABLE_LOCK
        if (instant_xlock)
            object_vector::unlock(l);
#endif
        return false;
    }

#ifdef PHANTOM_PROT_TABLE_LOCK
    if (instant_xlock)
        object_vector::unlock(l);
#elif defined(PHANTOM_PROT_NODE_SET)
    // update node #s
    INVARIANT(ins_info.node);
    if (!absent_set.empty()) {
        auto it = absent_set.find(ins_info.node);
        if (it != absent_set.end()) {
            if (unlikely(it->second.version != ins_info.old_version)) {
                // important: unlink the version, otherwise we risk leaving a dead
                // version at chain head -> infinite loop or segfault...
                oidmgr->oid_unlink(btr->tuple_vec(), oid, tuple);
                return false;
            }
            // otherwise, bump the version
            it->second.version = ins_info.new_version;
            SINGLE_THREADED_INVARIANT(concurrent_btree::ExtractVersionNumber(it->first) == it->second);
        }
    }
#endif

    // insert to log
    INVARIANT(log);
    ASSERT(tuple->size == value->size());
    auto record_size = align_up((size_t)tuple->size) + sizeof(varstr);
    auto size_code = encode_size_aligned(record_size);
    ASSERT(not ((uint64_t)value & ((uint64_t)0xf)));
    ASSERT(tuple->size);
    // log the whole varstr so that recovery can figure out the real size
    // of the tuple, instead of using the decoded (larger-than-real) size.
    log->log_insert(fid, oid, fat_ptr::make((void *)value, size_code),
                    DEFAULT_ALIGNMENT_BITS, NULL);

    // Note: here we log the whole key varstr so that recovery
    // can figure out the real key length with key->size(), otherwise
    // it'll have to use the decoded (inaccurate) size (and so will
    // build a different index...).
    record_size = align_up(sizeof(varstr) + key->size());
    ASSERT((char *)key->data() == (char *)key + sizeof(varstr));
    size_code = encode_size_aligned(record_size);
    log->log_insert_index(fid, oid, fat_ptr::make((void *)key, size_code),
                          DEFAULT_ALIGNMENT_BITS, NULL);

    // update write_set
    write_set.emplace_back(tuple, btr->tuple_vec(), oid);
    return true;
}

rc_t
transaction::do_tuple_read(dbtuple *tuple, value_reader &value_reader)
{
    INVARIANT(tuple);
    ASSERT(xc);
    bool read_my_own = (tuple->clsn.asi_type() == fat_ptr::ASI_XID);
    ASSERT(not read_my_own or (read_my_own and XID::from_ptr(volatile_read(tuple->clsn)) == xc->owner));

#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
    if (not read_my_own) {
        rc_t rc = {RC_INVALID};
#ifdef USE_PARALLEL_SSN
        rc = ssn_read(tuple);
#else
        rc = ssi_read(tuple);
#endif
        if (rc_is_abort(rc))
            return rc;
    } // otherwise it's my own update/insert, just read it
#endif

    // do the actual tuple read
    dbtuple::ReadStatus stat;
    {
        PERF_DECL(static std::string probe0_name(std::string(__PRETTY_FUNCTION__) + std::string(":do_read:")));
        ANON_REGION(probe0_name.c_str(), &private_::txn_btree_search_probe0_cg);
        tuple->prefetch();
        stat = tuple->do_read(value_reader, this->string_allocator(), not read_my_own);

        if (unlikely(stat == dbtuple::READ_FAILED))
            return rc_t{RC_ABORT_INTERNAL};
    }
    INVARIANT(stat == dbtuple::READ_EMPTY || stat == dbtuple::READ_RECORD);
    if (stat == dbtuple::READ_EMPTY) {
        return rc_t{RC_FALSE};
    }

    return {RC_TRUE};
}

#ifdef PHANTOM_PROT_NODE_SET
rc_t
transaction::do_node_read(
    const typename concurrent_btree::node_opaque_t *n, uint64_t v)
{
    INVARIANT(n);
    auto it = absent_set.find(n);
    if (it == absent_set.end())
        absent_set[n].version = v;
    else if (it->second.version != v)
        return rc_t{RC_ABORT_PHANTOM};
    return rc_t{RC_TRUE};
}
#endif

#ifdef USE_PARALLEL_SSN
rc_t
transaction::ssn_read(dbtuple *tuple)
{
    auto v_clsn = tuple->clsn.offset();
    // \eta - largest predecessor. So if I read this tuple, I should commit
    // after the tuple's creator (trivial, as this is committed version, so
    // this tuple's clsn can only be a predecessor of me): so just update
    // my \eta if needed.
    if (xc->pstamp < v_clsn)
        xc->pstamp = v_clsn;

    auto tuple_sstamp = volatile_read(tuple->sstamp);
    // If there's no (committed) overwrite so far, we need to track this read,
    // unless it's an old version.
    if (tuple_sstamp == NULL_PTR or tuple_sstamp.asi_type() == fat_ptr::ASI_XID) {
        // need to register as reader no matter it's an old version or not
        serial_register_reader_tx(tuple, xc->owner);
        if (tuple->is_old(xc)) {
            uint64_t bs = 0;
            do {
                bs = volatile_read(tuple->bstamp);
            }
            while (tuple->bstamp < xc->begin.offset() and
                   not __sync_bool_compare_and_swap(&tuple->bstamp, bs, xc->begin.offset()));
        }
        else {
            // Now if this tuple was overwritten by somebody, this means if I read
            // it, that overwriter will have anti-dependency on me (I must be
            // serialized before the overwriter), and it already committed (as a
            // successor of mine), so I need to update my \pi for the SSN check.
            // This is the easier case of anti-dependency (the other case is T1
            // already read a (then latest) version, then T2 comes to overwrite it).
            read_set.emplace_back(tuple);
        }
    }
    else {  // have committed overwrite
        ASSERT(tuple_sstamp.asi_type() == fat_ptr::ASI_LOG);
        if (xc->sstamp > tuple_sstamp.offset())
            xc->sstamp = tuple_sstamp.offset(); // \pi
    }

#ifdef DO_EARLY_SSN_CHECKS
    if (not ssn_check_exclusion(xc))
        return {RC_ABORT_SERIAL};
#endif
    return {RC_TRUE};
}
#endif

#ifdef USE_PARALLEL_SSI
rc_t
transaction::ssi_read(dbtuple *tuple)
{
    // Consider the dangerous structure that could lead to non-serializable
    // execution: T1 r:w T2 r:w T3 where T3 committed first. Read() needs
    // to check if I'm the T1 and do bookeeping if I'm the T2 (pivot).
    // See tuple.h for explanation on what s2 means.
    if (volatile_read(tuple->s2)) {
        ASSERT(tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);    // sstamp will be valid too if s2 is valid
        return rc_t{RC_ABORT_SERIAL};
    }

    fat_ptr tuple_s1 = volatile_read(tuple->sstamp);
    // see if there was a guy with cstamp=tuple_s1 who overwrote this version
    if (tuple_s1.asi_type() == fat_ptr::ASI_LOG) { // I'm T2
        // remember the smallest sstamp and use it during precommit
        if (xc->ct3 > tuple_s1.offset())
            xc->ct3 = tuple_s1.offset();
        // The purpose of adding a version to read-set is to re-check its
        // sstamp status at precommit and set its xstamp for updaters' (if
        // exist) reference during pre-commit thru the readers bitmap.
        // The xstamp is only useful for versions that haven't been updated,
        // or whose updates haven't been finalized. It's only used by the
        // updater at precommit. Once updated (ie v.sstamp is ASI_LOG), future
        // readers of this version (ie it started realy early and is perhaps a
        // long tx) will read the version's sstamp and s2 values. So read won't
        // need to add the version in read-set if it's overwritten.
    }
    else {
        // survived, register as a reader
        // After this point, I'll be visible to the updater (if any)
        serial_register_reader_tx(tuple, xc->owner);
        read_set.emplace_back(tuple);
    }
    return {RC_TRUE};
}
#endif

