#include "macros.h"
#include "amd64.h"
#include "txn.h"
#include "lockguard.h"
#include "dbcore/serial.h"

#include <atomic>
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
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::NodeLockRegionBegin();
#endif
#ifdef PHANTOM_PROT_NODE_SET
    absent_set.set_empty_key(NULL);    // google dense map
#endif
    xid = TXN::xid_alloc();
    xc = xid_get_context(xid);
    xc->begin_epoch = MM::epoch_enter();
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    xc->xct = this;
    // If there's a safesnap, then SSN treats the snapshot as a transaction
    // that has read all the versions, which means every update transaction
    // should have a initial pstamp of the safesnap.
    //
    // Readers under SSI using safesnap are free from SSI checks, but writers
    // will have to see whether they have a ct3 that's before the safesnap lsn
    // (ie the safesnap is T1, updater is T2). So for SSI updaters also needs
    // to take a look at the safesnap lsn.

    // Take a safe snapshot if read-only.
    if (sysconf::enable_safesnap and (flags & TXN_FLAG_READ_ONLY)) {
        ASSERT(MM::safesnap_lsn.offset());
        xc->begin = volatile_read(MM::safesnap_lsn);
        log = NULL;
    }
    else {
        serial_register_tx(xid);
        RCU::rcu_enter();
        log = logmgr->new_tx_log();
        xc->begin = logmgr->cur_lsn();
#ifdef USE_PARALLEL_SSN
        xc->pstamp = volatile_read(MM::safesnap_lsn).offset();
#elif defined(USE_PARALLEL_SSI)
        xc->last_safesnap = volatile_read(MM::safesnap_lsn).offset();
#endif
    }
#else
    RCU::rcu_enter();
    log = logmgr->new_tx_log();
    xc->begin = logmgr->cur_lsn();
#endif
}

transaction::~transaction()
{
    // transaction shouldn't fall out of scope w/o resolution
    // resolution means TXN_EMBRYO, TXN_CMMTD, and TXN_ABRTD
    INVARIANT(state() != TXN_ACTIVE && state() != TXN_COMMITTING);
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    if (not sysconf::enable_safesnap or (not (flags & TXN_FLAG_READ_ONLY)))
        RCU::rcu_exit();
#else
    RCU::rcu_exit();
#endif
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::AssertAllNodeLocksReleased();
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    if (not sysconf::enable_safesnap or (not (flags & TXN_FLAG_READ_ONLY)))
        serial_deregister_tx(xid);
#endif
    if (sysconf::enable_safesnap and flags & TXN_FLAG_READ_ONLY)
        MM::epoch_exit(INVALID_LSN, xc->begin_epoch);
    else
        MM::epoch_exit(xc->end, xc->begin_epoch);
    xid_free(xid);    // must do this after epoch_exit, which uses xc.end
}

void
transaction::abort()
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
        serial_deregister_reader_tx(&r->readers_bitmap);
    }
#endif

    for (auto &w : write_set) {
        dbtuple *tuple = w.get_object()->tuple();
        ASSERT(tuple);
        ASSERT(XID::from_ptr(tuple->get_object()->_clsn) == xid);
        if (tuple->is_defunct()) {   // for repeated overwrites
            continue;  // should have already called deallocate() during the update
        }
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
        if (tuple->next()) {
            volatile_write(tuple->next()->sstamp, NULL_PTR);
            tuple->next()->welcome_readers();
        }
#endif
        oidmgr->oid_unlink(w.oa, w.oid, tuple);
        volatile_write(w.get_object()->_clsn, NULL_PTR);
        ASSERT(w.get_object()->_alloc_epoch == xc->begin_epoch);
        MM::deallocate(w.new_object);
    }

    // Read-only tx on a safesnap won't have log
    if (log)
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

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    // Safe snapshot optimization for read-only transactions:
    // Use the begin ts as cstamp if it's a read-only transaction
    // This is the same for both SSN and SSI.
    if (sysconf::enable_safesnap and (flags & TXN_FLAG_READ_ONLY)) {
        ASSERT(not log);
        ASSERT(write_set.size() == 0);
        xc->end = xc->begin;
        volatile_write(xc->state, TXN_CMMTD);
        return {RC_TRUE};
    }
    else {
        ASSERT(log);
        xc->end = log->pre_commit();
        if (xc->end == INVALID_LSN)
            return rc_t{RC_ABORT_INTERNAL};
#ifdef USE_PARALLEL_SSN
        return parallel_ssn_commit();
#elif defined USE_PARALLEL_SSI
        return parallel_ssi_commit();
#endif
    }
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
    auto cstamp = xc->end.offset();

    // note that sstamp comes from reads, but the read optimization might
    // ignore looking at tuple's sstamp at all, so if tx sstamp is still
    // the initial value so far, we need to initialize it as cstamp. (so
    // that later we can fill tuple's sstamp as cstamp in case sstamp still
    // remained as the initial value.) Consider the extreme case where
    // old_version_threshold = 0: means no read set at all...
    if (is_read_mostly() && sysconf::ssn_read_opt_enabled()) {
        if (xc->sstamp.load(std::memory_order_acquire) > cstamp)
            xc->sstamp.store(cstamp, std::memory_order_release);
    } else {
        if (xc->sstamp.load(std::memory_order_relaxed) > cstamp)
            xc->sstamp.store(cstamp, std::memory_order_relaxed);
    }

    // find out my largest predecessor (\eta) and smallest sucessor (\pi)
    // for reads, see if sb. has written the tuples - look at sucessor lsn
    // for writes, see if sb. has read the tuples - look at access lsn

    // Process reads first for a stable sstamp to be used for the read-optimization
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
                    // Use relaxed for non-read-mostly tx? But maybe not worth the branch miss.
                    xc->set_sstamp(sucessor_xc->sstamp.load(std::memory_order_acquire));
                }
            }
        }
        else {
            // overwriter already fully committed/aborted or no overwriter at all
            ASSERT(sucessor_clsn.asi_type() == fat_ptr::ASI_LOG);
            xc->set_sstamp(sucessor_clsn.offset());
        }
    }

    for (auto &w : write_set) {
        dbtuple *tuple = w.get_object()->tuple();
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
        readers_bitmap_iterator readers_iter(&overwritten_tuple->readers_bitmap);
        while (true) {
            int32_t xid_idx = readers_iter.next(true);
            if (xid_idx == -1)
                break;

            XID rxid = volatile_read(rlist.xids[xid_idx]);
            ASSERT(rxid != xc->owner);
            if (rxid == INVALID_XID)
                continue;

            xid_context *reader_xc = NULL;
            uint64_t reader_end = 0;
            XID reader_owner = INVALID_XID;

            if (rxid._val) {
                reader_xc = xid_get_context(rxid);
                if (reader_xc) {
                    // Copy everything before doing anything:
                    // reader_end for getting pstamp;
                    // reader_owner for further detection of context chagne
                    // Note: must get reader_owner after getting end and begin.
                    reader_end = volatile_read(reader_xc->end).offset();
                    reader_owner = volatile_read(reader_xc->owner);
                }
            }

            // Do this after copying everything we needed
            auto last_read_mostly_cstamp = serial_get_last_read_mostly_cstamp(xid_idx);

            if (not rxid._val or not reader_xc or reader_owner != rxid) {
                if (overwritten_tuple->has_persistent_reader()) {
                    // If the guy thought this was an old version, xstamp won't
                    // be accurate, the writer should then consult the most recent
                    // cstamp that was set by the (reader) transaction on this bit
                    // position. Note that we can't just set pstamp to cstamp-1
                    // because the updater here has no clue what the previous owner
                    // of this bit position did and whether it will/already has a
                    // cstamp < or > my cstamp - could have committed before or
                    // after me, or didn't even read this verions (somebody even
                    // earlier did); or could be still in progress if reader_owner
                    // doesn't match rxid. Prepare for the worst, abort.
                    return {RC_ABORT_RW_CONFLICT};
                }
                else {
                    // Not marked as an old version - follow normal SSN protocol.
                    // Context change - the guy I saw was already gone. Now I need
                    // to read the xstamp (i.e., the reader should make sure it has
                    // set the xstamp for the tuple once it deregisters from the
                    // bitmap). The new guy will spin on me b/c it'll commit after
                    // me, and it will also set xstamp after finishing spinning on
                    // me, so I'll still be reading the xstamp that really belongs
                    // to the older reader - reduces false +ves.)
                    auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
                    xc->set_pstamp(tuple_xstamp);
                }
            }
            else if (not reader_end or reader_end > cstamp) {
                ASSERT(rxid == reader_owner);
                // Not in pre-commit yet or will (attempt to) commit after me,
                // don't care... unless it's considered an old version by some
                // reader. Still there's a chance to set its sstamp so that the
                // reader will (implicitly) know my existence.
                if (overwritten_tuple->has_persistent_reader()) {
                    // Only read-mostly transactions will mark the persistent_reader bit;
                    // if reader_xc isn't read-mostly, then it's definitely not him,
                    // consult last_read_mostly_cstamp.
                    if (reader_xc->xct->is_read_mostly() &&
                        !reader_xc->set_sstamp(xc->sstamp.load(std::memory_order_acquire))) {
                        if (reader_end) {
                            // So reader_end will be the new pstamp
                            last_read_mostly_cstamp = reader_end;
                        } else {
                            // Now we know reader_xc->set_sstamp failed, which implies that the
                            // reader has concluded. Try to find out its clsn here as my pstamp.
                            auto rend = volatile_read(reader_xc->end).offset();
                            auto rowner = volatile_read(reader_xc->owner);
                            if (rxid == rowner) {
                                // It's this guy, we can rely on rend
                                last_read_mostly_cstamp = reader_end = rend;
                            } else {
                                // Another transaction is using this context, refresh last_cstamp
                                last_read_mostly_cstamp = serial_get_last_read_mostly_cstamp(xid_idx);
                            }
                        }
                    }
                    xc->set_pstamp(last_read_mostly_cstamp);
                }
            }
            else {
                ASSERT(reader_end and reader_end < cstamp);
                if (overwritten_tuple->has_persistent_reader()) {
                    // Some reader who thinks this is old version existed, two
                    // options here: (1) spin on it and use reader_end-1 if it
                    // aborted, use reader_end if committed and use last_read_mostly_cstamp
                    // otherwise; (2) just use reader_end for simplicity
                    // (betting it'll successfully commit).
                    spin_for_cstamp(rxid, reader_xc);
                    // Refresh last_read_mostly_cstamp here, it'll be reader_end if the
                    // reader committed; or we just need to use the previous one
                    // if it didn't commit or saw a context change.
                    last_read_mostly_cstamp = serial_get_last_read_mostly_cstamp(xid_idx);
                    xc->set_pstamp(last_read_mostly_cstamp);
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
                        xc->set_pstamp(volatile_read(overwritten_tuple->xstamp));
                    }   // else it's aborted, as if nothing happened
                }
            }
        }
    }

    if (is_read_mostly()) {
        xc->finalize_sstamp();
    }

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
    if (is_read_mostly())
        serial_stamp_last_committed_lsn(xc->end);

    uint64_t my_sstamp = 0;
    if (is_read_mostly()) {
        my_sstamp = xc->sstamp.load(std::memory_order_acquire);
    } else {
        my_sstamp = xc->sstamp.load(std::memory_order_relaxed);
    }
    ASSERT(my_sstamp and my_sstamp != ~uint64_t{0});

    // post-commit: stuff access stamps for reads; init new versions
    auto clsn = xc->end;
    for (auto &w : write_set) {
        dbtuple *tuple = w.get_object()->tuple();
        if (tuple->is_defunct())
            continue;
        tuple->do_write();
        dbtuple *next_tuple = tuple->next();
        ASSERT(not next_tuple or
               ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
               (uint64_t)((char *)next_tuple - sizeof(object)));
        if (next_tuple) {   // update, not insert
            ASSERT(volatile_read(next_tuple->get_object()->_clsn).asi_type());
            ASSERT(XID::from_ptr(next_tuple->sstamp) == xid);
            volatile_write(next_tuple->sstamp, LSN::make(my_sstamp, 0).to_log_ptr());
            ASSERT(next_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
            next_tuple->welcome_readers();
        }
        volatile_write(tuple->xstamp, cstamp);
        volatile_write(tuple->get_object()->_clsn, clsn.to_log_ptr());
        ASSERT(tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);
        if (sysconf::enable_gc and tuple->next()) {
            // construct the (sub)list here so that we have only one CAS per tx
            enqueue_recycle_oids(w);
        }
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
        serial_deregister_reader_tx(&r->readers_bitmap);
    }

    if (updated_oids_head != NULL_PTR) {
        ASSERT(sysconf::enable_gc);
        ASSERT(updated_oids_tail != NULL_PTR);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }

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
            if (w.get_object()->tuple()->is_defunct())
                continue;
            dbtuple *overwritten_tuple = w.get_object()->tuple()->next();
            if (not overwritten_tuple)
                continue;

            // Check xstamp before continue to examine concurrent readers,
            // in case there is no concurrent readers (or they all have left
            // at this point).
            if (volatile_read(overwritten_tuple->xstamp) >= ct3)
                return {RC_ABORT_SERIAL};

            // Note: the bits representing readers that will commit **after**
            // me are stable; those representing readers older than my cstamp
            // could go away any time. But I can look at the version's xstamp
            // in that case. So the reader should make sure when it goes away
            // from the bitmap, xstamp is ready to be read by the updater.

            readers_bitmap_iterator readers_iter(&overwritten_tuple->readers_bitmap);
            while (true) {
                int32_t xid_idx = readers_iter.next(true);
                if (xid_idx == -1)
                    break;

                XID rxid = volatile_read(rlist.xids[xid_idx]);
                ASSERT(rxid != xc->owner);
                if (rxid == INVALID_XID)
                    continue;

                XID reader_owner = INVALID_XID;
                uint64_t reader_end = 0;
                xid_context *reader_xc = NULL;
                if (rxid._val) {
                    reader_xc = xid_get_context(rxid);
                    if (reader_xc) {
                        // copy everything before doing anything
                        reader_end = volatile_read(reader_xc->end).offset();
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
                    if (tuple_xstamp >= ct3)
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

    // stamp overwritten versions, stuff clsn
    auto clsn = xc->end;
    for (auto &w : write_set) {
        dbtuple* tuple = w.get_object()->tuple();
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
        if (sysconf::enable_gc and tuple->next()) {
            // construct the (sub)list here so that we have only one XCHG per tx
            enqueue_recycle_oids(w);
        }
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
        serial_deregister_reader_tx(&r->readers_bitmap);
    }

    // GC stuff, do it out of precommit
    if (updated_oids_head != NULL_PTR) {
        ASSERT(sysconf::enable_gc);
        ASSERT(updated_oids_tail != NULL_PTR);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }

    return rc_t{RC_TRUE};
}
#else
rc_t
transaction::si_commit()
{
    ASSERT(log);
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
    auto clsn = xc->end;
    for (auto &w : write_set) {
        dbtuple* tuple = w.get_object()->tuple();
        if (tuple->is_defunct())
            continue;
        ASSERT(w.oa);
        tuple->do_write();
        tuple->get_object()->_clsn = clsn.to_log_ptr();
        INVARIANT(tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_LOG);
        if (sysconf::enable_gc and tuple->next()) {
            // construct the (sub)list here so that we have only one XCHG per tx
            enqueue_recycle_oids(w);
        }
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

    if (updated_oids_head != NULL_PTR) {
        ASSERT(sysconf::enable_gc);
        ASSERT(updated_oids_tail != NULL_PTR);
        MM::recycle(updated_oids_head, updated_oids_tail);
    }

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
    fat_ptr new_head = object::create_tuple_object(value, false, xc->begin_epoch);
    ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
    dbtuple *tuple = ((object *)new_head.offset())->tuple();
    ASSERT(decode_size_aligned(new_head.size_code()) >= tuple->size);
    tuple->get_object()->_clsn = xid.to_ptr();
    OID oid = oidmgr->alloc_oid(fid);
    oidmgr->oid_put_new(btr->get_oid_array(), oid, new_head);
    typename concurrent_btree::insert_info_t ins_info;
    if (unlikely(!btr->insert_if_absent(varkey(key), oid, tuple, xc, &ins_info))) {
        oidmgr->oid_unlink(btr->get_oid_array(), oid, tuple);
        return false;
    }

#ifdef PHANTOM_PROT_NODE_SET
    // update node #s
    INVARIANT(ins_info.node);
    if (!absent_set.empty()) {
        auto it = absent_set.find(ins_info.node);
        if (it != absent_set.end()) {
            if (unlikely(it->second.version != ins_info.old_version)) {
                // important: unlink the version, otherwise we risk leaving a dead
                // version at chain head -> infinite loop or segfault...
                oidmgr->oid_unlink(btr->get_oid_array(), oid, tuple);
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
    ASSERT(tuple->pvalue->size() == tuple->size);
    add_to_write_set(new_head, btr->get_oid_array(), oid);
    return true;
}

rc_t
transaction::do_tuple_read(dbtuple *tuple, value_reader &value_reader)
{
    INVARIANT(tuple);
    ASSERT(xc);
    bool read_my_own = (tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_XID);
    ASSERT(not read_my_own or (read_my_own and XID::from_ptr(volatile_read(tuple->get_object()->_clsn)) == xc->owner));
    ASSERT(not read_my_own or not(flags & TXN_FLAG_READ_ONLY));

#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
    if (not read_my_own) {
        rc_t rc = {RC_INVALID};
        if (sysconf::enable_safesnap and (flags & TXN_FLAG_READ_ONLY))
            rc = {RC_TRUE};
        else {
#ifdef USE_PARALLEL_SSN
            rc = ssn_read(tuple);
#else
            rc = ssi_read(tuple);
#endif
        }
        if (rc_is_abort(rc))
            return rc;
    } // otherwise it's my own update/insert, just read it
#endif

    // do the actual tuple read
    dbtuple::ReadStatus stat;
    {
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
        serial_register_reader_tx(xc->owner, &tuple->readers_bitmap);
        if (tuple->is_old(xc)) {
            ASSERT(is_read_mostly());
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
        // Read-only optimization: s2 is not a problem if we're read-only and
        // my begin ts is earlier than s2.
        if (not ((sysconf::enable_ssi_read_only_opt and write_set.size() == 0) and
                 xc->begin.offset() < tuple->s2)) {
            // sstamp will be valid too if s2 is valid
            ASSERT(tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
            return rc_t{RC_ABORT_SERIAL};
        }
    }

    fat_ptr tuple_s1 = volatile_read(tuple->sstamp);
    // see if there was a guy with cstamp=tuple_s1 who overwrote this version
    if (tuple_s1.asi_type() == fat_ptr::ASI_LOG) { // I'm T2
        // remember the smallest sstamp and use it during precommit
        if (not xc->ct3 or xc->ct3 > tuple_s1.offset())
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
        serial_register_reader_tx(xc->owner, &tuple->readers_bitmap);
        read_set.emplace_back(tuple);
    }
    return {RC_TRUE};
}
#endif

