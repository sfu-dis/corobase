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
  : flags(flags), sa(&sa)
{
    epoch = MM::epoch_enter();
#ifdef REUSE_OBJECTS
    op = MM::get_object_pool();
#endif
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::NodeLockRegionBegin();
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
#ifdef USE_PARALLEL_SSN
    if (not (flags & TXN_FLAG_READ_ONLY))
#endif
        serial_register_tx(xid);
#endif
#ifdef PHANTOM_PROT_NODE_SET
    absent_set.set_empty_key(NULL);    // google dense map
#endif
    xid = TXN::xid_alloc();
    xc = xid_get_context(xid);
#ifdef USE_PARALLEL_SSN
    // If there's a safesnap, then SSN treats the snapshot as a transaction
    // that has read all the versions, which means every update transaction
    // should have a initial pstamp of the safesnap.

    // Take a safe snapshot if read-only.
    if (flags & TXN_FLAG_READ_ONLY) {
        ASSERT(MM::safesnap_lsn.offset());
        xc->begin = volatile_read(MM::safesnap_lsn);
    }
    else {
        RCU::rcu_enter();
        log = logmgr->new_tx_log();
        xc->pstamp = volatile_read(MM::safesnap_lsn).offset();
        xc->begin = logmgr->cur_lsn();
    }
#else
    RCU::rcu_enter();
    log = logmgr->new_tx_log();
    xc->begin = logmgr->cur_lsn();
#endif
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
#ifdef USE_PARALLEL_SSN
    if (not (flags & TXN_FLAG_READ_ONLY))
        RCU::rcu_exit();
#else
    RCU::rcu_exit();
#endif
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::AssertAllNodeLocksReleased();
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
#ifdef USE_PARALLEL_SSN
    if (not (flags & TXN_FLAG_READ_ONLY))
#endif
        serial_deregister_tx(xid);
#endif
    MM::epoch_exit(xc->end, epoch);
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
        ASSERT(r->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);
        // remove myself from reader list
        serial_deregister_reader_tx(r);
    }
#endif

    for (auto &w : write_set) {
        dbtuple *tuple = w.new_object->tuple();
        ASSERT(tuple);
        ASSERT(XID::from_ptr(tuple->get_object()->_clsn) == xid);
        if (tuple->is_defunct())   // for repeated overwrites
            continue;
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
        if (tuple->next()) {
            volatile_write(tuple->next()->sstamp, NULL_PTR);
            tuple->next()->welcome_readers();
        }
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

#ifdef USE_PARALLEL_SSN
    return parallel_ssn_commit();
#elif defined USE_PARALLEL_SSI
    return parallel_ssi_commit();
#else
    return si_commit();
#endif
}

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
#define set_tuple_xstamp(tuple, s)    \
{   \
    uint64_t x;  \
    do {    \
        x = volatile_read(tuple->xstamp);    \
    }   \
    while (x < s and not __sync_bool_compare_and_swap(&tuple->xstamp, x, s));   \
}
#endif

#ifdef USE_PARALLEL_SSN
rc_t
transaction::parallel_ssn_commit()
{
    // Safe snapshot optimization for read-only transactions:
    // Use the begin ts as cstamp if it's a read-only transaction
    if (flags & TXN_FLAG_READ_ONLY) {
        ALWAYS_ASSERT(write_set.size() == 0);
        xc->end = xc->begin;
        volatile_write(xc->state, TXN_CMMTD);
        return {RC_TRUE};
    }
    else {
        xc->end = log->pre_commit();
        if (xc->end == INVALID_LSN)
            return rc_t{RC_ABORT_INTERNAL};
    }

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
        dbtuple *tuple = w.new_object->tuple();
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

        // Do this before examining the preader field and reading the readers bitmap
        overwritten_tuple->lockout_readers();

        // Now readers who think this is an old version won't be able to read it
        // Then read the readers bitmap - it's guaranteed to cover all possible
        // readers (those who think it's an old version) as we lockout_readers()
        // first. Readers who think this is a young version can still come at any
        // time - they will be handled by the orignal SSN machinery.
        readers_list::bitmap_t readers = serial_get_tuple_readers(overwritten_tuple, true);

        while (readers) {
            int i = __builtin_ctzll(readers);
            ASSERT(i >= 0 and i < readers_list::XIDS_PER_READER_KEY);
            readers &= (readers-1);
            XID rxid = volatile_read(rlist.xids[i]);
            ASSERT(rxid != xc->owner);
            xid_context *reader_xc = NULL;
            uint64_t reader_end = 0;
            XID reader_owner = INVALID_XID;

            if (rxid._val) {
                reader_xc = xid_get_context(rxid);
                if (reader_xc) {
                    // Copy everything before doing anything:
                    // reader_end for getting pstamp;
                    // reader_begin for handlling duplicate CSNs;
                    // reader_owner for further detection of context chagne
                    // Note: must get reader_owner after getting end and begin.
                    reader_end = volatile_read(reader_xc->end).offset();
                    reader_owner = volatile_read(reader_xc->owner);
                }
            }

            // Do this after copying everything we needed
            auto last_cstamp = serial_get_last_cstamp(i);

            if (not rxid._val or not reader_xc or reader_owner != rxid) {
                if (overwritten_tuple->has_persistent_reader()) {
                    // If the guy thought this was an old version, xstamp won't
                    // be accurate, the writer should then consult the most recent
                    // cstamp that was set by the (reader) transaction on this bit
                    // position. Note that we can't just set pstamp to cstamp-1
                    // because the updater here has no clue what the previous owner
                    // of this bit position did and whether it will/alread has a
                    // cstamp < or > my cstamp - could have committed before or
                    // after me, or idn't even read this verions (somebody even
                    // earlier did); or could be still in progress if reader_owner
                    // doesn't match rxid
                    if (reader_xc and not serial_request_abort(reader_xc))
                        return {RC_ABORT_RW_CONFLICT};
                    if (xc->pstamp < last_cstamp)
                        xc->pstamp = last_cstamp;
                }
                else {
                    // Context change - the guy I saw was already gone. Now I need
                    // to read the xstamp (i.e., the reader should make sure it has
                    // set the xstamp for the tuple once it deregisters from the
                    // bitmap). The new guy will spin on me b/c it'll commit after
                    // me, and it will also set xstamp after finishing spinning on
                    // me, so I'll still be reading the xstamp that really belongs
                    // to the older reader - reduces false +ves.)
                    auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
                    if (xc->pstamp < tuple_xstamp)
                        xc->pstamp = tuple_xstamp;
                }
            }
            else if (not reader_end or reader_end > cstamp) {
                ASSERT(rxid == reader_owner);
                // Not in pre-commit yet or will (attempt to) commit after me,
                // don't care... unless it's considered an old version by some
                // reader. Still there's a chance to tell it to abort if the
                // reader thinks this version is an old one
                if (overwritten_tuple->has_persistent_reader()) {
                    if (not serial_request_abort(reader_xc))
                        return {RC_ABORT_RW_CONFLICT};
                    // else the reader will abort, as if nothing happened;
                    // but still have to account to possible previous readers.
                    // So set pstamp to last_cstamp (better than using
                    // reader_end - 1)
                    if (xc->pstamp < last_cstamp)
                        xc->pstamp = last_cstamp;
                }
            }
            else {
                ASSERT(reader_end and reader_end < cstamp);
                // Some reader who thinks this is old version existed, two
                // options here: (1) spin on it and use reader_end-1 if it
                // aborted, use reader_end if committed and use last_cstamp
                // otherwise; (2) just use reader_end for simplicity
                // (betting it'll successfully commit).
                if (overwritten_tuple->has_persistent_reader()) {
                    spin_for_cstamp(rxid, reader_xc);
                    // Refresh last_cstamp here, it'll be reader_end if the
                    // reader committed; or we just need to use the previous one
                    // if it didn't commit or saw a context change.
                    last_cstamp = serial_get_last_cstamp(i);
                    if (xc->pstamp < last_cstamp)
                        xc->pstamp = last_cstamp;
                }
                else {
                    // (Pre-) committed before me, need to wait for its cstamp for
                    // a stable xstamp. But if my pstamp is already >= reader_end,
                    // there's no need to wait for it then (pstamp increases).
                    // Also in this case, no matter if the persistent reader bit
                    // is on or off, I can use the guy's pstamp.
                    auto cr = spin_for_cstamp(rxid, reader_xc);
                    if (cr == TXN_CMMTD)
                        xc->pstamp = reader_end;
                    else if (cr == TXN_INVALID) {
                        // Context change during the spin, no clue if the
                        // reader committed or aborted - read the xstamp in
                        // case it did committ (xstamp is stable now, b/c
                        // it'll only deregister from the bitmap after
                        // setting xstamp, and a context change means the
                        // reader must've concluded - either aborted or
                        // committed - and so deregistered from the bitmap)
                        auto x = volatile_read(overwritten_tuple->xstamp);
                        if (xc->pstamp < x)
                            xc->pstamp = x;
                    }   // else it's aborted, as if nothing happened
                }
            }
        }
    }

    for (auto &r : read_set) {
    try_get_sucessor:
        // so tuple should be the committed version I read
        ASSERT(r->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);

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
                auto cr = spin_for_cstamp(successor_xid, sucessor_xc);
                // context change, previous overwriter must be gone,
                // re-read sstamp (should see ASI_LOG this time).
                if (cr == TXN_INVALID)
                    goto try_get_sucessor;
                else if (cr == TXN_CMMTD) {
                    xc->set_sstamp(volatile_read(sucessor_xc->sstamp));
                }
            }
        }
        else {
            // overwriter already fully committed/aborted or no overwriter at all
            ASSERT(sucessor_clsn.asi_type() == fat_ptr::ASI_LOG);
            xc->set_sstamp(sucessor_clsn.offset());
        }
    }

    // Too bad still have to abort after gone through all this...
    if (volatile_read(xc->should_abort))
        return {RC_ABORT_RW_CONFLICT};

    if (not ssn_check_exclusion(xc))
        return rc_t{RC_ABORT_SERIAL};

#ifdef PHANTOM_PROT_NODE_SET
    if (not check_phantom())
        return rc_t{RC_ABORT_PHANTOM};
#endif

    // ok, can really commit if we reach here
    log->commit(NULL);

    // Do this before setting TXN_CMMTD state so that it'll be stable
    // no matter the guy spinning on me noticed a context change or
    // my real state (CMMTD or ABRTD)
    // XXX: one optimization might be setting this only when read some
    // old versions.
    serial_stamp_last_committed_lsn(xc->end);

#ifdef ENABLE_GC
    recycle_oid *updated_oids_head = NULL;
    recycle_oid *updated_oids_tail = NULL;
#endif
    // post-commit: stuff access stamps for reads; init new versions
    auto clsn = xc->end;
    for (auto &w : write_set) {
        dbtuple *tuple = w.new_object->tuple();
        if (tuple->is_defunct())
            continue;
        tuple->do_write();
        dbtuple *next_tuple = tuple->next();
        ASSERT(not next_tuple or
               ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
               (uint64_t)((char *)next_tuple - sizeof(object)));
        if (next_tuple) {   // update, not insert
            ASSERT(volatile_read(next_tuple->get_object()->_clsn).asi_type());
            ASSERT(xc->sstamp and xc->sstamp != ~uint64_t{0});
            ASSERT(XID::from_ptr(next_tuple->sstamp) == xid);
            volatile_write(next_tuple->sstamp, LSN::make(xc->sstamp, 0).to_log_ptr());
            ASSERT(next_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
            next_tuple->welcome_readers();
        }
        volatile_write(tuple->xstamp, cstamp);
        volatile_write(tuple->get_object()->_clsn, clsn.to_log_ptr());
        ASSERT(tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);
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

    // This state change means:
    // 1. New data generated by me are available to be read
    //    (need to this after finished post-commit for write set)
    // 2. My cstamp is valid and stable, can be used by
    //    conflicting readers as their sstamp or by conflicting
    //    writers as their pstamp.
    volatile_write(xc->state, TXN_CMMTD);

    // The availability of xtamp, solely depends on when
    // serial_deregister_reader_tx is called. So the point
    // here is to do the following strictly one by one in order:
    // 1. Spin on older successor
    // 2. Set xstamp
    // 3. Deregister from bitmap
    // Without 1, the updater might see a larger-than-it-should
    // xstamp and use it as its pstamp -> more unnecessary aborts
    for (auto &r : read_set) {
        ASSERT(r->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);

        // Spin to hold this position until the older successor is gone,
        // so the updater can get a resonable xstamp (not too high)
        auto sstamp = volatile_read(r->sstamp);
        if (sstamp.asi_type() == fat_ptr::ASI_XID) {
            XID oxid = XID::from_ptr(sstamp);
            if (oxid != this->xid) {    // exclude myself
                xid_context *ox = xid_get_context(oxid);
                if (ox) {
                    auto ox_end = volatile_read(ox->end).offset();
                    auto ox_owner = volatile_read(ox->owner);
                    if (ox_owner == oxid and ox_end and ox_end < cstamp)
                        spin_for_cstamp(oxid, ox);
                }
                // if !ox or ox_owner != oxid then the guy is
                // already gone, don't bother
            }
        }

        // Now set the access stamp - do this after the above spin so
        // the writer will read the xstamp that was really set by the
        // preceeding reader, instead of some younger reader that will
        // commit after it (like me).
        set_tuple_xstamp(r, cstamp);

        // Must deregister from the bitmap **after** set xstamp, so that
        // the updater will be able to see the correct xstamp after noticed
        // a context change; otherwise it might miss it and read a too-old
        // xstamp that was set by some earlier reader.
        serial_deregister_reader_tx(r);
    }

#ifdef ENABLE_GC
    if (updated_oids_head) {
        ASSERT(updated_oids_tail);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }
#endif

    return rc_t{RC_TRUE};
}
#elif defined(USE_PARALLEL_SSI)
rc_t
transaction::parallel_ssi_commit()
{
    // get clsn, abort if failed
    xc->end = log->pre_commit();
    if (xc->end == INVALID_LSN)
        return rc_t{RC_ABORT_INTERNAL};

    // tzwang: The race between the updater (if any) and me (as the reader) -
    // A reader publishes its existence by calling serial_register_reader().
    // The updater might not notice this if it already checked this tuple
    // before the reader published its existence. In this case, the writer
    // actually thinks nobody is reading the version that's being overwritten.
    // So as the reader, we need to make sure we know the existence of the
    // updater during precommit. We do this by checking the tuple's sstamp
    // (and it's basically how this parallel pre-commit works).
    //
    // Side note: this "writer-oversights-reader" problem pertains to only
    // SSI, because it needs to track "concurrent reads" or some read that
    // belongs to an evil T1 (assuming I'm the unlucky T2), while for SSN
    // it doesn't matter - SSN only cares about things got committed before,
    // and the reader that registers after writer entered precommit will
    // definitely precommit after the writer.
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
                auto cr = spin_for_cstamp(ox, overwriter_xc);
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
        for (auto &w : write_set) {
            if (w.new_object->tuple()->is_defunct())
                continue;
            dbtuple *overwritten_tuple = w.new_object->tuple()->next();
            if (not overwritten_tuple)
                continue;
            // Note: the bits representing readers that will commit **after**
            // me are stable; those representing readers older than my cstamp
            // could go away any time. But I can look at the version's xstamp
            // in that case. So the reader should make sure when it goes away
            // from the bitmap, xstamp is ready to be read by the updater.
            readers_list::bitmap_t readers = serial_get_tuple_readers(overwritten_tuple, true);
            while (readers) {
                int i = __builtin_ctzll(readers);
                ASSERT(i >= 0 and i < readers_list::XIDS_PER_READER_KEY);
                readers &= (readers-1);
                XID rxid = volatile_read(rlist.xids[i]);
                ASSERT(rxid != xc->owner);
                XID reader_owner = INVALID_XID;
                uint64_t reader_begin = 0, reader_end = 0;
                xid_context *reader_xc = NULL;
                if (rxid._val) {
                    reader_xc = xid_get_context(rxid);
                    if (reader_xc) {
                        // copy everything before doing anything
                        reader_end = volatile_read(reader_xc->end).offset();
                        reader_begin = volatile_read(reader_xc->begin).offset();
                        reader_owner = volatile_read(reader_xc->owner);
                    }
                }

                if (not rxid._val or not reader_xc or reader_owner != rxid) {
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
                    if (ct3 and tuple_xstamp >= ct3)
                        return {RC_ABORT_SERIAL};
                }
                else if (not reader_end) {
                    ASSERT(rxid == reader_owner);
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
                    ASSERT(ct3);
                    // This reader_end will be the version's xstamp if it committed
                    if (reader_end >= ct3) {
                        auto cr = spin_for_cstamp(rxid, reader_xc);
                        if (cr == TXN_CMMTD)
                            return {RC_ABORT_SERIAL};
                        else if (cr == TXN_INVALID) {
                            // Context change, no clue if the reader committed
                            // or aborted, read tuple's xstamp - the same
                            // reasoning as in SSN
                            if (volatile_read(overwritten_tuple->xstamp) >= ct3)
                                return {RC_ABORT_SERIAL};
                        }
                    }   // otherwise it's worth the spin - won't use it anyway
                }
                else if (reader_end == cstamp) {
                    // Duplicate cstamp by rdtscp - same reasoning as in SSN
                    if (reader_begin == xc->begin.offset())
                        return {RC_ABORT_SERIAL};
                    if (reader_begin < xc->begin.offset()) {
                        auto cr = spin_for_cstamp(rxid, reader_xc);
                        if (cr != TXN_ABRTD)    // CMMTD or INVALID
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
    auto clsn = xc->end;
    for (auto &w : write_set) {
        dbtuple* tuple = w.new_object->tuple();
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
        volatile_write(tuple->get_object()->_clsn, clsn.to_log_ptr());
        INVARIANT(tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);
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
    //
    // This needs to happen after setting overwritten tuples' s2, b/c
    // the reader needs to check this during pre-commit.
    volatile_write(xc->state, TXN_CMMTD);

    // Similar to SSN implementation, xstamp's availability depends solely
    // on when to deregister_reader_tx, not when to transitioning to the
    // "committed" state.
    for (auto &r : read_set) {
        // Update xstamps in read versions, this should happen before
        // deregistering from the bitmap, so when the updater found a
        // context change, it'll get a stable xtamp from the tuple.
        // No need to look into write set and skip: do_tuple_read will
        // skip inserting to read set if it's already in write set; it's
        // possible to see a tuple in both read and write sets, only if
        // the tuple is first read, then updated - updating the xstamp
        // of such a tuple won't hurt, and it eliminates unnecessary
        // cycles spent on hashtable.
        set_tuple_xstamp(r, cstamp);

        // Now wait for the updater that pre-committed before me to go
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
                        spin_for_cstamp(oxid, ox);
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
    // get clsn, abort if failed
    xc->end = log->pre_commit();
    if (xc->end == INVALID_LSN)
        return rc_t{RC_ABORT_INTERNAL};

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
    auto clsn = xc->end;
    for (auto &w : write_set) {
        dbtuple* tuple = w.new_object->tuple();
        if (tuple->is_defunct())
            continue;
        tuple->do_write();
        tuple->get_object()->_clsn = clsn.to_log_ptr();
        INVARIANT(tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
        if (tuple->next()) {
            // construct the (sub)list here so that we have only one CAS per tx
            recycle_oid *r = new recycle_oid(w.oa, w.oid);
            if (not updated_oids_head)
                updated_oids_head = updated_oids_tail = r;
            else {
                updated_oids_tail->next = r;
                updated_oids_tail = r;
            }
        }
#endif
#if CHECK_INVARIANT
        object *obj = tuple->get_object();
        fat_ptr pdest = volatile_read(obj->_pdest);
        ASSERT((pdest == NULL_PTR and not tuple->size) or
               (pdest.asi_type() == fat_ptr::ASI_LOG));
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
    tuple->get_object()->_clsn = xid.to_ptr();

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
    if (unlikely(!btr->insert_if_absent(varkey(key), oid, tuple, &ins_info))) {
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
                    DEFAULT_ALIGNMENT_BITS, &tuple->get_object()->_pdest);

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
    write_set.emplace_back(tuple->get_object(), btr->tuple_vec(), oid);
    return true;
}

rc_t
transaction::do_tuple_read(dbtuple *tuple, value_reader &value_reader)
{
    INVARIANT(tuple);
    ASSERT(xc);
    bool read_my_own = (tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_XID);
    ASSERT(not read_my_own or (read_my_own and XID::from_ptr(volatile_read(tuple->get_object()->_clsn)) == xc->owner));

#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
    if (not read_my_own) {
        rc_t rc = {RC_INVALID};
#ifdef USE_PARALLEL_SSN
        if (flags & TXN_FLAG_READ_ONLY)
            rc = {RC_TRUE};
        else
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
    auto v_clsn = tuple->get_object()->_clsn.offset();
    // \eta - largest predecessor. So if I read this tuple, I should commit
    // after the tuple's creator (trivial, as this is committed version, so
    // this tuple's clsn can only be a predecessor of me): so just update
    // my \eta if needed.
    if (xc->pstamp < v_clsn)
        xc->pstamp = v_clsn;

    auto tuple_sstamp = volatile_read(tuple->sstamp);
    if (tuple_sstamp.asi_type() == fat_ptr::ASI_LOG) {
        // have committed overwrite
        if (xc->sstamp > tuple_sstamp.offset())
            xc->sstamp = tuple_sstamp.offset(); // \pi
    }
    else {
        ASSERT(tuple_sstamp == NULL_PTR or tuple_sstamp.asi_type() == fat_ptr::ASI_XID);
        // Exclude myself
        if (tuple_sstamp != NULL_PTR and XID::from_ptr(tuple_sstamp) == xc->owner)
            return {RC_TRUE};

        // If there's no (committed) overwrite so far, we need to track this read,
        // unless it's an old version.
        ASSERT(tuple_sstamp == NULL_PTR or XID::from_ptr(tuple_sstamp) != xc->owner);
        serial_register_reader_tx(tuple, xc->owner);
        if (tuple->is_old(xc)) {
            if (not tuple->set_persistent_reader())
                return {RC_ABORT_RW_CONFLICT};
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

