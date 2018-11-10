#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/sm-rep.h"
#include "dbcore/serial.h"
#include "ermia.h"

namespace ermia {

#if defined(SSN) || defined(SSI) || defined(MVOCC)
static __thread transaction::read_set_t *tls_read_set;
#endif

write_set_t tls_write_set[config::MAX_THREADS];

transaction::transaction(uint64_t flags, str_arena &sa)
    : flags(flags), sa(&sa), write_set(tls_write_set[thread::MyId()]) {
  if (!(flags & TXN_FLAG_CMD_REDO) && config::is_backup_srv()) {
    // Read-only transaction on backup - grab a begin timestamp and go.
    // A read-only 'transaction' on a backup basically is reading a
    // consistent snapshot back in time. No CC involved.
    thread_local TXN::xid_context *ctx = nullptr;
    if (!ctx) {
      ctx = TXN::xid_get_context(TXN::xid_alloc());
    }
    xc = ctx;
    RCU::rcu_enter();
    xc->begin = rep::GetReadView();
    ASSERT(xc->begin);
    xc->xct = this;
  } else {
    initialize_read_write();
  }
}

void transaction::initialize_read_write() {
  if (config::phantom_prot) {
    masstree_absent_set.set_empty_key(NULL);  // google dense map
    masstree_absent_set.clear();
  }
  write_set.clear();
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  if (unlikely(not tls_read_set)) {
    tls_read_set = new read_set_t;
    tls_read_set->reserve(2000);
  }
  read_set = tls_read_set;
  read_set->clear();
#endif
  xid = TXN::xid_alloc();
  xc = TXN::xid_get_context(xid);
  xc->begin_epoch = MM::epoch_enter();
  xc->xct = this;
#if defined(SSN) || defined(SSI)
  // If there's a safesnap, then SSN treats the snapshot as a transaction
  // that has read all the versions, which means every update transaction
  // should have a initial pstamp of the safesnap.
  //
  // Readers under SSI using safesnap are free from SSI checks, but writers
  // will have to see whether they have a ct3 that's before the safesnap lsn
  // (ie the safesnap is T1, updater is T2). So for SSI updaters also needs
  // to take a look at the safesnap lsn.

  // Take a safe snapshot if read-only.
  if (config::enable_safesnap and (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(MM::safesnap_lsn);
    xc->begin = volatile_read(MM::safesnap_lsn);
    log = NULL;
  } else {
    TXN::serial_register_tx(xid);
    RCU::rcu_enter();
    log = logmgr->new_tx_log();
    // Must +1: a tx T can only update a tuple if its latest version was
    // created before T's begin timestamp (i.e., version.clsn < T.begin,
    // note the range is exclusive; see first updater wins rule in
    // oid_put_update() in sm-oid.cpp). Otherwise we risk making no
    // progress when retrying an aborted transaction: everyone is trying
    // to update the same tuple with latest version stamped at cur_lsn()
    // but no one can succeed (because version.clsn == cur_lsn == t.begin).
    xc->begin = logmgr->cur_lsn().offset() + 1;
#ifdef SSN
    xc->pstamp = volatile_read(MM::safesnap_lsn);
#elif defined(SSI)
    xc->last_safesnap = volatile_read(MM::safesnap_lsn);
#endif
  }
#else
  RCU::rcu_enter();
  log = logmgr->new_tx_log();
  xc->begin = logmgr->cur_lsn().offset() + 1;
#endif
}

transaction::~transaction() {
  if (config::is_backup_srv() && !(flags & TXN_FLAG_CMD_REDO)) {
    RCU::rcu_exit();
    return;
  }

  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_CMMTD, and TXN_ABRTD
  ASSERT(state() != TXN::TXN_ACTIVE && state() != TXN::TXN_COMMITTING);
#if defined(SSN) || defined(SSI)
  if (not config::enable_safesnap or (not(flags & TXN_FLAG_READ_ONLY)))
    RCU::rcu_exit();
#else
  RCU::rcu_exit();
#endif
#if defined(SSN) || defined(SSI)
  if (not config::enable_safesnap or (not(flags & TXN_FLAG_READ_ONLY)))
    TXN::serial_deregister_tx(xid);
#endif
  if (config::enable_safesnap and flags & TXN_FLAG_READ_ONLY)
    MM::epoch_exit(0, xc->begin_epoch);
  else
    MM::epoch_exit(xc->end, xc->begin_epoch);
  TXN::xid_free(xid);  // must do this after epoch_exit, which uses xc.end
}

void transaction::Abort() {
  // Mark the dirty tuple as invalid, for oid_get_version to
  // move on more quickly.
  volatile_write(xc->state, TXN::TXN_ABRTD);

#if defined(SSN) || defined(SSI)
  // Go over the read set first, to deregister from the tuple
  // asap so the updater won't wait for too long.
  for (uint32_t i = 0; i < read_set->size(); ++i) {
    auto &r = (*read_set)[i];
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
    // remove myself from reader list
    serial_deregister_reader_tx(&r->readers_bitmap);
  }
#endif

  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    dbtuple *tuple = (dbtuple *)w.get_object()->GetPayload();
    ASSERT(tuple);
    ASSERT(XID::from_ptr(tuple->GetObject()->GetClsn()) == xid);
#if defined(SSI) || defined(SSN) || defined(MVOCC)
    if (tuple->NextVolatile()) {
      volatile_write(tuple->NextVolatile()->sstamp, NULL_PTR);
#ifdef SSN
      tuple->NextVolatile()->welcome_read_mostly_tx();
#endif
    }
#endif
    Object *obj = w.get_object();
    fat_ptr entry = *w.entry;
    oidmgr->PrimaryTupleUnlink(w.entry);
    obj->SetClsn(NULL_PTR);
    ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
    MM::deallocate(entry);
  }

  // Read-only tx on a safesnap won't have log
  if (log) {
    log->discard();
  }
}

rc_t transaction::commit() {
  ALWAYS_ASSERT(state() == TXN::TXN_ACTIVE);
  volatile_write(xc->state, TXN::TXN_COMMITTING);
#if defined(SSN) || defined(SSI)
  // Safe snapshot optimization for read-only transactions:
  // Use the begin ts as cstamp if it's a read-only transaction
  // This is the same for both SSN and SSI.
  if (config::enable_safesnap and (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(not log);
    ASSERT(write_set.size() == 0);
    xc->end = xc->begin;
    volatile_write(xc->state, TXN::TXN_CMMTD);
    return {RC_TRUE};
  } else {
    ASSERT(log);
    xc->end = log->pre_commit().offset();
    if (xc->end == 0) {
      return rc_t{RC_ABORT_INTERNAL};
    }
#ifdef SSN
    return parallel_ssn_commit();
#elif defined SSI
    return parallel_ssi_commit();
#endif
  }
#elif defined(MVOCC)
  return mvocc_commit();
#else
  return si_commit();
#endif
}

#if defined(SSN) || defined(SSI)
#define set_tuple_xstamp(tuple, s)                                    \
  {                                                                   \
    uint64_t x;                                                       \
    do {                                                              \
      x = volatile_read(tuple->xstamp);                               \
    } while (x < s and                                                \
             not __sync_bool_compare_and_swap(&tuple->xstamp, x, s)); \
  }
#endif

#ifdef SSN
rc_t transaction::parallel_ssn_commit() {
  auto cstamp = xc->end;

  // note that sstamp comes from reads, but the read optimization might
  // ignore looking at tuple's sstamp at all, so if tx sstamp is still
  // the initial value so far, we need to initialize it as cstamp. (so
  // that later we can fill tuple's sstamp as cstamp in case sstamp still
  // remained as the initial value.) Consider the extreme case where
  // old_version_threshold = 0: means no read set at all...
  if (is_read_mostly() && config::ssn_read_opt_enabled()) {
    if (xc->sstamp.load(std::memory_order_acquire) == 0)
      xc->sstamp.store(cstamp, std::memory_order_release);
  } else {
    if (xc->sstamp.load(std::memory_order_relaxed) == 0)
      xc->sstamp.store(cstamp, std::memory_order_relaxed);
  }

  // find out my largest predecessor (\eta) and smallest sucessor (\pi)
  // for reads, see if sb. has written the tuples - look at sucessor lsn
  // for writes, see if sb. has read the tuples - look at access lsn

  // Process reads first for a stable sstamp to be used for the
  // read-optimization
  for (uint32_t i = 0; i < read_set->size(); ++i) {
    auto &r = (*read_set)[i];
  try_get_successor:
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
    // read tuple->slsn to a local variable before doing anything relying on it,
    // it might be changed any time...
    fat_ptr successor_clsn = volatile_read(r->sstamp);
    if (successor_clsn == NULL_PTR) continue;

    if (successor_clsn.asi_type() == fat_ptr::ASI_LOG) {
      // overwriter already fully committed/aborted or no overwriter at all
      xc->set_sstamp(successor_clsn.offset());
      if (not ssn_check_exclusion(xc)) {
        return rc_t{RC_ABORT_SERIAL};
      }
    } else {
      // overwriter in progress
      ALWAYS_ASSERT(successor_clsn.asi_type() == fat_ptr::ASI_XID);
      XID successor_xid = XID::from_ptr(successor_clsn);
      TXN::xid_context *successor_xc = TXN::xid_get_context(successor_xid);
      if (not successor_xc) {
        goto try_get_successor;
      }

      if (volatile_read(successor_xc->owner) == xc->owner)  // myself
        continue;

      // Must obtain the successor's status first then check ownership
      auto successor_state = volatile_read(successor_xc->state);
      if (not successor_xc->verify_owner(successor_xid)) {
        goto try_get_successor;
      }

      // Note the race between reading the successor's cstamp and the successor
      // setting its cstamp after got one from the log: the successor could
      // have got a cstamp but hasn't stored it in its cstamp field, so here
      // must rely on the successor's state (set before obtaining cstamp) and
      // then spin on successor's cstamp if necessary (state is not
      // committing). Directly reading successor's cstamp might miss successors
      // that have already got cstamp but hasn't stored it in successor_xc->end
      // (esp. dangerous if its cstamp is smaller than mine - could miss
      // successor).
      if (successor_state == TXN::TXN_ACTIVE) {
        // Not yet in pre-commit, skip
        continue;
      }
      // Already in pre-commit or committed, definitely has (or will have)
      // cstamp
      uint64_t successor_end = 0;
      bool should_continue = false;
      while (not successor_end) {
        // Must in the order of 1. read cstamp, 2. read state, 3. verify owner
        auto s = volatile_read(successor_xc->end);
        successor_state = volatile_read(successor_xc->state);
        if (not successor_xc->verify_owner(successor_xid)) {
          goto try_get_successor;
        }
        if (successor_state == TXN::TXN_ABRTD) {
          // If there's a new overwriter, it must have a cstamp larger than mine
          should_continue = true;
          break;
        }
        ALWAYS_ASSERT(successor_state == TXN::TXN_CMMTD or
                      successor_state == TXN::TXN_COMMITTING);
        successor_end = s;
      }
      if (should_continue) {
        continue;
      }
      // overwriter might haven't committed, be commited after me, or before me
      // we only care if the successor is committed *before* me.
      ALWAYS_ASSERT(successor_end);
      ALWAYS_ASSERT(successor_end != cstamp);
      if (successor_end > cstamp) {
        continue;
      }
      if (successor_state == TXN::TXN_COMMITTING) {
        // When we got successor_end, the successor was committing, use
        // successor_end
        // if it indeed committed
        successor_state = TXN::spin_for_cstamp(successor_xid, successor_xc);
      }
      // Context change, previous overwriter was gone, retry (should see ASI_LOG
      // this time).
      if (successor_state == TXN::TXN_INVALID)
        goto try_get_successor;
      else if (successor_state == TXN::TXN_CMMTD) {
        // Again, successor_xc->sstamp might change any time (i.e., successor_xc
        // might get reused because successor concludes), so must
        // read-then-verify.
        auto s = successor_xc->sstamp.load(
            (config::ssn_read_opt_enabled() && is_read_mostly())
                ? std::memory_order_acquire
                : std::memory_order_relaxed);
        if (not successor_xc->verify_owner(successor_xid)) {
          goto try_get_successor;
        }
        xc->set_sstamp(s);
        if (not ssn_check_exclusion(xc)) {
          return rc_t{RC_ABORT_SERIAL};
        }
      }
    }
  }

  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    dbtuple *tuple = (dbtuple *)w.get_object()->GetPayload();

    // go to the precommitted or committed version I (am about to)
    // overwrite for the reader list
    dbtuple *overwritten_tuple = tuple->NextVolatile();
    ASSERT(not overwritten_tuple or
           (tuple->GetObject())->GetNextVolatile().offset() ==
               (uint64_t)(overwritten_tuple->GetObject()));
    if (not overwritten_tuple)  // insert
      continue;

    ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);

    // Do this before examining the preader field and reading the readers bitmap
    overwritten_tuple->lockout_read_mostly_tx();

    // Now readers who think this is an old version won't be able to read it
    // Then read the readers bitmap - it's guaranteed to cover all possible
    // readers (those who think it's an old version) as we
    // lockout_read_mostly_tx()
    // first. Readers who think this is a young version can still come at any
    // time - they will be handled by the orignal SSN machinery.
    TXN::readers_bitmap_iterator readers_iter(&overwritten_tuple->readers_bitmap);
    while (true) {
      int32_t xid_idx = readers_iter.next(true);
      if (xid_idx == -1) break;

      XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
      ASSERT(rxid != xc->owner);

      TXN::xid_context *reader_xc = NULL;
      uint64_t reader_end = 0;
      auto reader_state = TXN::TXN_ACTIVE;

      if (rxid != INVALID_XID) {
        reader_xc = TXN::xid_get_context(rxid);
        if (reader_xc) {
          // Copy everything before doing anything:
          // reader_end for getting pstamp;
          reader_state = volatile_read(reader_xc->state);
          reader_end = volatile_read(reader_xc->end);
        }
      }

      if (rxid == INVALID_XID or not reader_xc or
          not reader_xc->verify_owner(rxid)) {
      context_change:
        // Context change - the guy I saw was already gone, should read tuple
        // xstamp
        // (i.e., the reader should make sure it has set the xstamp for the
        // tuple once
        // it deregisters from the bitmap). The new guy that inherits this bit
        // position
        // will spin on me b/c it'll commit after me, and it will also set
        // xstamp after
        // spinning on me, so I'll still be reading the xstamp that really
        // belongs to
        // the older reader - reduces false +ves.)
        if (config::ssn_read_opt_enabled() and
            overwritten_tuple->has_persistent_reader()) {
          // If somebody thought this was an old version, xstamp alone won't be
          // accurate,
          // should consult both tls read_mostly cstamp and xstamp (we consult
          // xstamp in the
          // end of the readers loop for each version later in one go), e.g., a
          // read-mostly
          // tx read it as an old version after a normal tx. Note that we can't
          // just set
          // pstamp to cstamp-1 because the updater here has no clue what the
          // previous
          // owner of this bit position did and how its cstamp compares to mine.
          uint64_t last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
          if (last_cstamp > cstamp) {
            // Reader committed without knowing my existence with a larger
            // cstamp,
            // ie it didn't account me as successor, nothing else to do than
            // abort.
            return {RC_ABORT_RW_CONFLICT};
          }
          xc->set_pstamp(last_cstamp);
          if (not ssn_check_exclusion(xc)) {
            return rc_t{RC_ABORT_SERIAL};
          }
        }  // otherwise we will catch the tuple's xstamp outside the loop
      } else {
        // We have a valid context, now see if we should get reader's commit ts.
        if (reader_state != TXN::TXN_ACTIVE and not reader_end) {
          while (not reader_end) {
            auto r = volatile_read(reader_xc->end);
            reader_state = volatile_read(reader_xc->state);
            if (not reader_xc->verify_owner(rxid)) {
              goto context_change;
            }
            if (reader_state == TXN::TXN_ABRTD) {
              reader_end = 0;
              break;
            }
            reader_end = r;
          }
        }
        if (reader_state == TXN::TXN_ACTIVE or not reader_end or
            reader_end > cstamp) {
          // Not in pre-commit yet or will (attempt to) commit after me or
          // aborted,
          // don't care... unless it's considered an old version by some reader.
          // Still there's a chance to set its sstamp so that the reader will
          // (implicitly) know my existence.
          if (config::ssn_read_opt_enabled() and
              overwritten_tuple->has_persistent_reader()) {
            // Only read-mostly transactions will mark the persistent_reader
            // bit;
            // if reader_xc isn't read-mostly, then it's definitely not him,
            // consult last_read_mostly_cstamp.
            // Need to account for previously committed read-mostly txs anyway
            uint64_t last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
            if (reader_xc->xct->is_read_mostly() and
                not reader_xc->set_sstamp(
                    (~TXN::xid_context::sstamp_final_mark) &
                    xc->sstamp.load(std::memory_order_acquire))) {
              // Failed setting the tx's sstamp - it must have finalized sstamp,
              // i.e., entered precommit, so it must have a valid cstamp.
              if (reader_end == 0) {
                reader_end = volatile_read(reader_xc->end);
              }
              if (reader_xc->verify_owner(rxid)) {
                ALWAYS_ASSERT(reader_end);
                while (last_cstamp < reader_end) {
                  // Wait until the tx sets last_cstamp or aborts
                  last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
                  if (volatile_read(reader_xc->state) == TXN::TXN_ABRTD or
                      !reader_xc->verify_owner(rxid)) {
                    last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
                    break;
                  }
                }
              } else {
                // context change - the tx must have already updated its
                // last_cstamp if committed.
                last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
              }
              if (last_cstamp > cstamp) {
                // committed without knowing me
                return {RC_ABORT_RW_CONFLICT};
              }
            }  // else it must be another transaction is using this context or
            // we succeeded setting the read-mostly tx's sstamp
            xc->set_pstamp(last_cstamp);
            if (not ssn_check_exclusion(xc)) {
              return rc_t{RC_ABORT_SERIAL};
            }
          }
        } else {
          ALWAYS_ASSERT(reader_end and reader_end < cstamp);
          if (config::ssn_read_opt_enabled() and
              overwritten_tuple->has_persistent_reader()) {
            // Some reader who thinks this tuple is old existed, in case
            // reader_xc
            // is read-mostly, spin on it and consult the read_mostly_cstamp.
            // Note
            // still we need to refresh and read xstamp outside the loop in case
            // this
            // isn't the real reader that I should care.
            if (reader_xc->xct->is_read_mostly()) {
              TXN::spin_for_cstamp(rxid, reader_xc);
            }
            xc->set_pstamp(TXN::serial_get_last_read_mostly_cstamp(xid_idx));
            if (not ssn_check_exclusion(xc)) {
              return rc_t{RC_ABORT_SERIAL};
            }
          } else {
            // (Pre-) committed before me, need to wait for its xstamp to
            // finalize.
            if (TXN::spin_for_cstamp(rxid, reader_xc) == TXN::TXN_CMMTD) {
              xc->set_pstamp(reader_end);
              if (not ssn_check_exclusion(xc)) {
                return rc_t{RC_ABORT_SERIAL};
              }
            }
            // else aborted or context change during the spin, no clue if the
            // reader
            // committed - read the xstamp in case it did committ (xstamp is
            // stable
            // now, b/c it'll only deregister from the bitmap after setting
            // xstamp,
            // and a context change means the reader must've concluded - either
            // aborted or committed - and so deregistered from the bitmap). We
            // do this
            // outside the loop in one go.
          }
        }
      }
    }
    // Still need to re-read xstamp in case we missed any reader
    xc->set_pstamp(volatile_read(overwritten_tuple->xstamp));
    if (not ssn_check_exclusion(xc)) {
      return rc_t{RC_ABORT_SERIAL};
    }
  }

  if (config::ssn_read_opt_enabled() and is_read_mostly()) {
    xc->finalize_sstamp();
  }

  if (not ssn_check_exclusion(xc)) return rc_t{RC_ABORT_SERIAL};

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  // ok, can really commit if we reach here
  log->commit(NULL);

  // Do this before setting TXN_CMMTD state so that it'll be stable
  // no matter the guy spinning on me noticed a context change or
  // my real state (CMMTD or ABRTD)
  // XXX: one optimization might be setting this only when read some
  // old versions.
  if (config::ssn_read_opt_enabled() and is_read_mostly())
    TXN::serial_stamp_last_committed_lsn(xc->end);

  uint64_t my_sstamp = 0;
  if (config::ssn_read_opt_enabled() and is_read_mostly()) {
    my_sstamp = xc->sstamp.load(std::memory_order_acquire) &
                (~TXN::xid_context::sstamp_final_mark);
  } else {
    my_sstamp = xc->sstamp.load(std::memory_order_relaxed);
  }
  ALWAYS_ASSERT(my_sstamp and
                (my_sstamp & TXN::xid_context::sstamp_final_mark) == 0);

  // post-commit: stuff access stamps for reads; init new versions
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    tuple->DoWrite();
    dbtuple *next_tuple = tuple->NextVolatile();
    ASSERT(not next_tuple or (object->GetNextVolatile().offset() ==
                              (uint64_t)next_tuple->GetObject()));
    if (next_tuple) {  // update, not insert
      ASSERT(next_tuple->GetObject()->GetClsn().asi_type());
      ASSERT(XID::from_ptr(next_tuple->sstamp) == xid);
      volatile_write(next_tuple->sstamp, LSN::make(my_sstamp, 0).to_log_ptr());
      ASSERT(next_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
      next_tuple->welcome_read_mostly_tx();
    }
    volatile_write(tuple->xstamp, cstamp);
    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
  }

  // This state change means:
  // 1. New data generated by me are available to be read
  //    (need to this after finished post-commit for write set)
  // 2. My cstamp is valid and stable, can be used by
  //    conflicting readers as their sstamp or by conflicting
  //    writers as their pstamp.
  COMPILER_MEMORY_FENCE;
  volatile_write(xc->state, TXN::TXN_CMMTD);

  // The availability of xtamp, solely depends on when
  // serial_deregister_reader_tx is called. So the point
  // here is to do the following strictly one by one in order:
  // 1. Spin on older successor
  // 2. Set xstamp
  // 3. Deregister from bitmap
  // Without 1, the updater might see a larger-than-it-should
  // xstamp and use it as its pstamp -> more unnecessary aborts
  for (uint32_t i = 0; i < read_set->size(); ++i) {
    auto &r = (*read_set)[i];
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);

    // Spin to hold this position until the older successor is gone,
    // so the updater can get a resonable xstamp (not too high)
    auto sstamp = volatile_read(r->sstamp);
    if (sstamp.asi_type() == fat_ptr::ASI_XID) {
      XID oxid = XID::from_ptr(sstamp);
      if (oxid != this->xid) {  // exclude myself
        TXN::xid_context *ox = TXN::xid_get_context(oxid);
        if (ox) {
          auto ox_end = volatile_read(ox->end);
          auto ox_owner = volatile_read(ox->owner);
          if (ox_owner == oxid and ox_end and ox_end < cstamp)
            TXN::spin_for_cstamp(oxid, ox);
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
  return rc_t{RC_TRUE};
}
#elif defined(SSI)
rc_t transaction::parallel_ssi_commit() {
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

  auto cstamp = xc->end;

  // get the smallest s1 in each tuple we have read (ie, the smallest cstamp
  // of T3 in the dangerous structure that clobbered our read)
  uint64_t ct3 = xc->ct3;  // this will be the s2 of versions I clobbered

  for (uint32_t i = 0; i < read_set->size(); ++i) {
    auto &r = (*read_set)[i];
  get_overwriter:
    fat_ptr overwriter_clsn = volatile_read(r->sstamp);
    if (overwriter_clsn == NULL_PTR) continue;

    uint64_t tuple_s1 = 0;
    if (overwriter_clsn.asi_type() == fat_ptr::ASI_LOG) {
      // already committed, read tuple's sstamp
      ALWAYS_ASSERT(overwriter_clsn.asi_type() == fat_ptr::ASI_LOG);
      tuple_s1 = volatile_read(overwriter_clsn).offset();
    } else {
      ALWAYS_ASSERT(overwriter_clsn.asi_type() == fat_ptr::ASI_XID);
      XID ox = XID::from_ptr(overwriter_clsn);
      if (ox == xc->owner)  // myself
        continue;
      ASSERT(ox != xc->owner);
      TXN::xid_context *overwriter_xc = TXN::xid_get_context(ox);
      if (not overwriter_xc) goto get_overwriter;

      // A race exists between me and the overwriter (similar to that in SSN):
      // a transaction must transition to COMMITTING state before setting its
      // cstamp, and I must read overwriter's state first, before looking at
      // its cstamp; otherwise if I look at overwriter's cstamp directly, I
      // might miss overwriterwho actually have a smaller cstamp - obtaining
      // a cstamp and storing it in xc->end are not done atomically in a single
      // instruction. So here start with reading overwriter's state.

      // Must obtain the overwriter's status first then check ownership
      auto overwriter_state = volatile_read(overwriter_xc->state);
      if (not overwriter_xc->verify_owner(ox)) {
        goto get_overwriter;
      }

      if (overwriter_state == TXN::TXN_ACTIVE) {
        // successor really still hasn't entered pre-commit, skip
        continue;
      }
      uint64_t overwriter_end = 0;
      bool should_continue = false;
      while (overwriter_end == 0) {
        auto s = volatile_read(overwriter_xc->end);
        overwriter_state = volatile_read(overwriter_xc->state);
        if (not overwriter_xc->verify_owner(ox)) {
          goto get_overwriter;
        }
        if (overwriter_state == TXN::TXN_ABRTD) {
          should_continue = true;
          break;
        }
        overwriter_end = s;
      }
      if (should_continue) {
        continue;
      }
      ALWAYS_ASSERT(overwriter_end);
      ALWAYS_ASSERT(overwriter_end != cstamp);
      if (overwriter_end > cstamp) {
        continue;
      }
      // Spin if the overwriter entered precommit before me: need to
      // find out the final sstamp value.
      // => the updater maybe doesn't know my existence and commit.
      if (overwriter_state == TXN::TXN_COMMITTING) {
        overwriter_state = TXN::spin_for_cstamp(ox, overwriter_xc);
      }
      if (overwriter_state == TXN::TXN_INVALID)  // context change, retry
        goto get_overwriter;
      else if (overwriter_state == TXN::TXN_CMMTD)
        tuple_s1 = overwriter_end;
    }

    if (tuple_s1 and (not ct3 or ct3 > tuple_s1)) ct3 = tuple_s1;

    // Now the updater (if exists) should've already concluded and stamped
    // s2 - requires updater to change state to CMMTD only after setting
    // all s2 values.
    COMPILER_MEMORY_FENCE;
    if (volatile_read(r->s2)) return {RC_ABORT_SERIAL};
    // Release read lock (readers bitmap) after setting xstamp in post-commit
  }

  if (ct3) {
    // now see if I'm the unlucky T2
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set[i];
      dbtuple *overwritten_tuple =
          w.get_object()->GetPinnedTuple()->NextVolatile();
      if (not overwritten_tuple) continue;

      // Note: the bits representing readers that will commit **after**
      // me are stable; those representing readers older than my cstamp
      // could go away any time. But I can look at the version's xstamp
      // in that case. So the reader should make sure when it goes away
      // from the bitmap, xstamp is ready to be read by the updater.

      TXN::readers_bitmap_iterator readers_iter(&overwritten_tuple->readers_bitmap);
      while (true) {
        int32_t xid_idx = readers_iter.next(true);
        if (xid_idx == -1) break;

        XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
        ASSERT(rxid != xc->owner);
        if (rxid == INVALID_XID) continue;

        uint64_t reader_end = 0;
        auto reader_state = TXN::TXN_ACTIVE;
        TXN::xid_context *reader_xc = NULL;
        if (rxid._val) {
          reader_xc = TXN::xid_get_context(rxid);
          if (reader_xc) {
            // copy everything before doing anything
            reader_state = volatile_read(reader_xc->state);
            reader_end = volatile_read(reader_xc->end);
          }
        }

        if (not rxid._val or not reader_xc or
            not reader_xc->verify_owner(rxid)) {
        context_change:
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
          if (tuple_xstamp >= ct3) return {RC_ABORT_SERIAL};
        } else {
          bool should_continue = false;
          if (reader_state != TXN::TXN_ACTIVE and not reader_end) {
            while (not reader_end) {
              auto r = volatile_read(reader_xc->end);
              reader_state = volatile_read(reader_xc->state);
              if (not reader_xc->verify_owner(rxid)) {
                goto context_change;
              }
              if (reader_state == TXN::TXN_ABRTD) {
                should_continue = true;
                break;
              }
              reader_end = r;
            }
          }
          if (should_continue) {  // reader aborted
            continue;
          }
          if (reader_state == TXN::TXN_ACTIVE or not reader_end) {
            // Reader not in precommit yet, and not sure if it is likely
            // to commit. But aborting the pivot is easier here, so we
            // just abort (betting the reader is likely to succeed).
            //
            // Another way is don't do anything here, reader will notice
            // my presence or my legacy (sstamp on the version) once
            // entered precommit; this might cause deadlock tho.
            return {RC_ABORT_SERIAL};
          } else if (reader_end < cstamp) {
            ASSERT(ct3);
            // This reader_end will be the version's xstamp if it committed
            if (reader_end >= ct3) {
              auto cr = TXN::spin_for_cstamp(rxid, reader_xc);
              if (cr == TXN::TXN_CMMTD)
                return {RC_ABORT_SERIAL};
              else if (cr == TXN::TXN_INVALID) {
                // Context change, no clue if the reader committed
                // or aborted, read tuple's xstamp - the same
                // reasoning as in SSN
                if (volatile_read(overwritten_tuple->xstamp) >= ct3)
                  return {RC_ABORT_SERIAL};
              }
            }  // otherwise it's worth the spin - won't use it anyway
          } else {
            // Reader will commit after me, ie its cstamp will be > ct3
            // (b/c ct3 is the first committed in the dangerous structure)
            // and will not release its seat in the bitmap until I'm gone
            return {RC_ABORT_SERIAL};
          }
        }
      }
      // Check xstamp in case in-flight readers left or there was no reader
      if (volatile_read(overwritten_tuple->xstamp) >= ct3)
        return {RC_ABORT_SERIAL};
    }
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  // survived!
  log->commit(NULL);

  // stamp overwritten versions, stuff clsn
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    tuple->DoWrite();
    dbtuple *overwritten_tuple = tuple->NextVolatile();

    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    if (overwritten_tuple) {  // update
      ASSERT(not overwritten_tuple->s2);
      // Must set sstamp first before setting s2 (ssi_read assumes sstamp is
      // available once s2 is available)
      ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);
      volatile_write(overwritten_tuple->sstamp, clsn_ptr);

      // Must set s2 first, before setting clsn
      volatile_write(overwritten_tuple->s2, ct3);
      COMPILER_MEMORY_FENCE;
    }
    volatile_write(tuple->xstamp, cstamp);
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
  }

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is where (committed) tuple data are made visible to readers
  //
  // This needs to happen after setting overwritten tuples' s2, b/c
  // the reader needs to check this during pre-commit.
  COMPILER_MEMORY_FENCE;
  volatile_write(xc->state, TXN::TXN_CMMTD);

  // Similar to SSN implementation, xstamp's availability depends solely
  // on when to deregister_reader_tx, not when to transitioning to the
  // "committed" state.
  for (uint32_t i = 0; i < read_set->size(); ++i) {
    auto &r = (*read_set)[i];
    // Update xstamps in read versions, this should happen before
    // deregistering from the bitmap, so when the updater found a
    // context change, it'll get a stable xtamp from the tuple.
    // No need to look into write set and skip: DoTupleRead will
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
      if (oxid != this->xid) {  // exclude myself
        TXN::xid_context *ox = TXN::xid_get_context(oxid);
        if (ox) {
          auto ox_end = volatile_read(ox->end);
          if (ox->verify_owner(oxid) and ox_end and ox_end < cstamp)
            TXN::spin_for_cstamp(oxid, ox);
        }
        // if !ox or ox_owner != oxid then the guy is
        // already gone, don't bother
      }
    }
    COMPILER_MEMORY_FENCE;
    // now it's safe to release my seat!
    // Need to do this after setting xstamp, so that the updater can
    // see the xstamp if it didn't find the bit in the bitmap is set.
    serial_deregister_reader_tx(&r->readers_bitmap);
  }
  return rc_t{RC_TRUE};
}
#elif defined(MVOCC)
rc_t transaction::mvocc_commit() {
  if (!(flags & TXN_FLAG_CMD_REDO) && config::is_backup_srv()) {
    return rc_t{RC_TRUE};
  }

  ASSERT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit().offset();
  if (xc->end == 0) {
    return rc_t{RC_ABORT_INTERNAL};
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  // Just need to check read-set
  for (uint32_t i = 0; i < read_set->size(); ++i) {
    auto &r = (*read_set)[i];
  check_backedge:
    fat_ptr successor_clsn = volatile_read(r->sstamp);
    if (!successor_clsn.offset()) {
      continue;
    }

    // Already overwritten, see if this is a back-edge, i.e., committed before
    // me
    if (successor_clsn.asi_type() == fat_ptr::ASI_LOG) {
      if (successor_clsn.offset() < xc->end) {
        return rc_t{RC_ABORT_SERIAL};
      }
    } else {
      XID successor_xid = XID::from_ptr(successor_clsn);
      TXN::xid_context *successor_xc = TXN::xid_get_context(successor_xid);
      if (!successor_xc) {
        goto check_backedge;
      }
      if (volatile_read(successor_xc->owner) == xc->owner) {  // myself
        continue;
      }
      auto successor_state = volatile_read(successor_xc->state);
      if (!successor_xc->verify_owner(successor_xid)) {
        goto check_backedge;
      }
      if (successor_state == TXN::TXN_ACTIVE) {
        // Not yet in pre-commit, skip
        continue;
      }
      // Already in pre-commit or committed, definitely has (or will have)
      // cstamp
      uint64_t successor_end = 0;
      bool should_continue = false;
      while (!successor_end) {
        auto s = volatile_read(successor_xc->end);
        successor_state = volatile_read(successor_xc->state);
        if (not successor_xc->verify_owner(successor_xid)) {
          goto check_backedge;
        }
        if (successor_state == TXN::TXN_ABRTD) {
          // If there's a new overwriter, it must have a cstamp larger than mine
          should_continue = true;
          break;
        }
        ALWAYS_ASSERT(successor_state == TXN::TXN_CMMTD ||
                      successor_state == TXN::TXN_COMMITTING);
        successor_end = s;
      }
      if (should_continue) {
        continue;
      }
      if (successor_end < xc->end) {
        return rc_t{RC_ABORT_SERIAL};
      }
    }
  }

  log->commit(NULL);  // will populate log block

  // post-commit cleanup: install clsn to tuples
  // (traverse write-tuple)
  // stuff clsn in tuples in write-set
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    ASSERT(w.entry);
    tuple->DoWrite();
    dbtuple *overwritten_tuple = tuple->NextVolatile();
    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    if (overwritten_tuple) {
      ASSERT(overwritten_tuple->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(overwritten_tuple->sstamp) == xid);
      volatile_write(overwritten_tuple->sstamp, clsn_ptr);
      ASSERT(overwritten_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
      ASSERT(overwritten_tuple->sstamp.offset() == clsn_ptr.offset());
    }
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
#ifndef NDEBUG
    Object *obj = tuple->GetObject();
    fat_ptr pdest = obj->GetPersistentAddress();
    ASSERT((pdest == NULL_PTR and not tuple->size) or
           (pdest.asi_type() == fat_ptr::ASI_LOG));
#endif
  }

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is where (committed) tuple data are made visible to readers
  volatile_write(xc->state, TXN::TXN_CMMTD);
  return rc_t{RC_TRUE};
}
#else
rc_t transaction::si_commit() {
  if (!(flags & TXN_FLAG_CMD_REDO) && config::is_backup_srv()) {
    return rc_t{RC_TRUE};
  }

  ASSERT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit().offset();
  if (xc->end == 0) return rc_t{RC_ABORT_INTERNAL};

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  log->commit(NULL);  // will populate log block

  // post-commit cleanup: install clsn to tuples
  // (traverse write-tuple)
  // stuff clsn in tuples in write-set
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    ASSERT(object);
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    ASSERT(w.entry);
    tuple->DoWrite();

    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
#ifndef NDEBUG
    Object *obj = tuple->GetObject();
    fat_ptr pdest = obj->GetPersistentAddress();
    ASSERT((pdest == NULL_PTR and not tuple->size) or
           (pdest.asi_type() == fat_ptr::ASI_LOG));
#endif
  }

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is where (committed) tuple data are made visible to readers
  volatile_write(xc->state, TXN::TXN_CMMTD);
  return rc_t{RC_TRUE};
}
#endif

// returns true if btree versions have changed, ie there's phantom
bool transaction::MasstreeCheckPhantom() {
  for (auto &r : masstree_absent_set) {
    const uint64_t v = ConcurrentMasstree::ExtractVersionNumber(r.first);
    if (unlikely(v != r.second)) return false;
  }
  return true;
}

rc_t transaction::Update(IndexDescriptor *index_desc, OID oid, const varstr *k, varstr *v) {
  oid_array *tuple_array = index_desc->GetTupleArray();
  FID tuple_fid = index_desc->GetTupleFid();

  // first *updater* wins
  fat_ptr new_obj_ptr = NULL_PTR;
  fat_ptr prev_obj_ptr =
      oidmgr->PrimaryTupleUpdate(tuple_array, oid, v, xc, &new_obj_ptr);
  Object *prev_obj = (Object *)prev_obj_ptr.offset();

  if (prev_obj) {  // succeeded
    dbtuple *tuple = ((Object *)new_obj_ptr.offset())->GetPinnedTuple();
    ASSERT(tuple);
    dbtuple *prev = prev_obj->GetPinnedTuple();
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    ASSERT(xc);
#ifdef SSI
    ASSERT(prev->sstamp == NULL_PTR);
    if (xc->ct3) {
      // Check if we are the T2 with a committed T3 earlier than a safesnap
      // (being T1)
      if (xc->ct3 <= xc->last_safesnap) return {RC_ABORT_SERIAL};

      if (volatile_read(prev->xstamp) >= xc->ct3 or
          not prev->readers_bitmap.is_empty(true)) {
        // Read-only optimization: safe if T1 is read-only (so far) and T1's
        // begin ts
        // is before ct3.
        if (config::enable_ssi_read_only_opt) {
          TXN::readers_bitmap_iterator readers_iter(&prev->readers_bitmap);
          while (true) {
            int32_t xid_idx = readers_iter.next(true);
            if (xid_idx == -1) break;

            XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
            ASSERT(rxid != xc->owner);
            if (rxid == INVALID_XID)  // reader is gone, check xstamp in the end
              continue;

            XID reader_owner = INVALID_XID;
            uint64_t reader_begin = 0;
            TXN::xid_context *reader_xc = NULL;
            reader_xc = TXN::xid_get_context(rxid);
            if (not reader_xc)  // context change, consult xstamp later
              continue;

            // copy everything before doing anything
            reader_begin = volatile_read(reader_xc->begin);
            reader_owner = volatile_read(reader_xc->owner);
            if (reader_owner != rxid)  // consult xstamp later
              continue;

            // we're safe if the reader is read-only (so far) and started after
            // ct3
            if (reader_xc->xct->write_set.size() > 0 and
                reader_begin <= xc->ct3) {
              oidmgr->PrimaryTupleUnlink(tuple_array, oid);
              return {RC_ABORT_SERIAL};
            }
          }
        } else {
          oidmgr->PrimaryTupleUnlink(tuple_array, oid);
          return {RC_ABORT_SERIAL};
        }
      }
    }
#endif
#ifdef SSN
    // update hi watermark
    // Overwriting a version could trigger outbound anti-dep,
    // i.e., I'll depend on some tx who has read the version that's
    // being overwritten by me. So I'll need to see the version's
    // access stamp to tell if the read happened.
    ASSERT(prev->sstamp == NULL_PTR);
    auto prev_xstamp = volatile_read(prev->xstamp);
    if (xc->pstamp < prev_xstamp) xc->pstamp = prev_xstamp;

#ifdef EARLY_SSN_CHECK
    if (not ssn_check_exclusion(xc)) {
      // unlink the version here (note abort_impl won't be able to catch
      // it because it's not yet in the write set)
      oidmgr->PrimaryTupleUnlink(tuple_array, oid);
      return rc_t{RC_ABORT_SERIAL};
    }
#endif

    // copy access stamp to new tuple from overwritten version
    // (no need to copy sucessor lsn (slsn))
    volatile_write(tuple->xstamp, prev->xstamp);
#endif

    // read prev's clsn first, in case it's a committing XID, the clsn's state
    // might change to ASI_LOG anytime
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    fat_ptr prev_clsn = prev->GetObject()->GetClsn();
    fat_ptr prev_persistent_ptr = NULL_PTR;
    if (prev_clsn.asi_type() == fat_ptr::ASI_XID and
        XID::from_ptr(prev_clsn) == xid) {
      // updating my own updates!
      // prev's prev: previous *committed* version
      ASSERT(((Object *)prev_obj_ptr.offset())->GetAllocateEpoch() ==
             xc->begin_epoch);
      prev_persistent_ptr = prev_obj->GetNextPersistent();
      MM::deallocate(prev_obj_ptr);
    } else {  // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      volatile_write(prev->sstamp, xc->owner.to_ptr());
      ASSERT(prev->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(prev->sstamp) == xc->owner);
      ASSERT(tuple->NextVolatile() == prev);
#endif
      add_to_write_set(tuple_array->get(oid));
      prev_persistent_ptr = prev_obj->GetPersistentAddress();
    }

    ASSERT(not tuple->pvalue or tuple->pvalue->size() == tuple->size);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_XID);
    ASSERT(oidmgr->oid_get_version(tuple_fid, oid, xc) == tuple);
    ASSERT(log);

    // FIXME(tzwang): mark deleted in all 2nd indexes as well?

    // The varstr also encodes the pdest of the overwritten version.
    // FIXME(tzwang): the pdest of the overwritten version doesn't belong to
    // varstr. Embedding it in varstr makes it part of the payload and is
    // helpful for digging out versions on backups. Not used by the primary.
    bool is_delete = !v;
    if (!v) {
      // Get an empty varstr just to store the overwritten tuple's
      // persistent address
      v = string_allocator().next(0);
      v->p = nullptr;
      v->l = 0;
    }
    ASSERT(v);
    v->ptr = prev_persistent_ptr;
    ASSERT(v->ptr.offset() && v->ptr.asi_type() == fat_ptr::ASI_LOG);

    // log the whole varstr so that recovery can figure out the real size
    // of the tuple, instead of using the decoded (larger-than-real) size.
    size_t data_size = v->size() + sizeof(varstr);
    auto size_code = encode_size_aligned(data_size);
    if (is_delete) {
      log->log_enhanced_delete(tuple_fid, oid,
                                 fat_ptr::make((void *)v, size_code),
                                 DEFAULT_ALIGNMENT_BITS);
    } else {
      log->log_update(tuple_fid, oid, fat_ptr::make((void *)v, size_code),
                        DEFAULT_ALIGNMENT_BITS,
                        tuple->GetObject()->GetPersistentAddressPtr());

      if (config::log_key_for_update) {
        auto key_size = align_up(k->size() + sizeof(varstr));
        auto key_size_code = encode_size_aligned(key_size);
        log->log_update_key(tuple_fid, oid,
                              fat_ptr::make((void *)k, key_size_code),
                              DEFAULT_ALIGNMENT_BITS);
      }
    }
    return rc_t{RC_TRUE};
  } else {  // somebody else acted faster than we did
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
}

OID transaction::PrepareInsert(OrderedIndex *index, varstr *value, dbtuple **out_tuple) {
  IndexDescriptor *id = index->GetDescriptor();
  bool is_primary_idx = id->IsPrimary();
  auto *tuple_array = id->GetTupleArray();
  FID tuple_fid = id->GetTupleFid();
  *out_tuple = nullptr;
  OID oid = 0;
  if (likely(is_primary_idx)) {
    fat_ptr new_head = Object::Create(value, false, xc->begin_epoch);
    ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
    ASSERT(new_head.asi_type() == 0);
    *out_tuple = (dbtuple *)((Object *)new_head.offset())->GetPayload();
    ASSERT(decode_size_aligned(new_head.size_code()) >= (*out_tuple)->size);
    (*out_tuple)->GetObject()->SetClsn(xid.to_ptr());
    oid = oidmgr->alloc_oid(tuple_fid);
    oidmgr->oid_put_new(tuple_array, oid, new_head);
  } else {
    // Inserting into a secondary index - just key-OID mapping is enough
    oid = *(OID *)value;  // It's actually a pointer to an OID in user space
  }
  return oid;
}

bool transaction::TryInsertNewTuple(OrderedIndex *index, const varstr *key,
                                    varstr *value, OID *inserted_oid) {
  dbtuple *tuple = nullptr;
  OID oid = PrepareInsert(index, value, &tuple);
  if (inserted_oid) {
    *inserted_oid = oid;
  }
  IndexDescriptor *id = index->GetDescriptor();
  auto *tuple_array = id->GetTupleArray();
  auto *key_array = id->GetKeyArray();

  ASSERT(key);
  bool is_primary_idx = id->IsPrimary();
  if (!index->InsertIfAbsent(this, *key, oid)) {
    if (is_primary_idx) {
      oidmgr->PrimaryTupleUnlink(tuple_array, oid);
    }
    if (config::enable_chkpt) {
      volatile_write(key_array->get(oid)->_ptr, 0);
    }
    return false;
  }
  FinishInsert(index, oid, key, value, tuple);
  return true;
}

void transaction::FinishInsert(OrderedIndex *index, OID oid, const varstr *key, varstr *value, dbtuple *tuple) {
  IndexDescriptor *id = index->GetDescriptor();
  auto *tuple_array = id->GetTupleArray();
  auto *key_array = id->GetKeyArray();

  // Succeeded, now put the key there if we need it
  if (config::enable_chkpt) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key =
        (varstr *)MM::allocate(sizeof(varstr) + key->size(), xc->begin_epoch);
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(key);
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  // insert to log
  bool is_primary_idx = id->IsPrimary();
  ASSERT(log);
  if (likely(tuple)) {  // Primary index, "real tuple"
    ASSERT(is_primary_idx);
    ASSERT(tuple->size == value->size());
    value->ptr = NULL_PTR;
    auto record_size = align_up((size_t)tuple->size) + sizeof(varstr);
    auto size_code = encode_size_aligned(record_size);
    ASSERT(not((uint64_t)value & ((uint64_t)0xf)));
    ASSERT(tuple->size);
    // log the whole varstr so that recovery can figure out the real size
    // of the tuple, instead of using the decoded (larger-than-real) size.
    FID tuple_fid = id->GetTupleFid();
    log->log_insert(tuple_fid, oid, fat_ptr::make((void *)value, size_code),
                    DEFAULT_ALIGNMENT_BITS,
                    tuple->GetObject()->GetPersistentAddressPtr());
  }

  // Note: here we log the whole key varstr so that recovery
  // can figure out the real key length with key->size(), otherwise
  // it'll have to use the decoded (inaccurate) size (and so will
  // build a different index...).
  auto record_size = align_up(sizeof(varstr) + key->size());
  ASSERT((char *)key->data() == (char *)key + sizeof(varstr));
  auto size_code = encode_size_aligned(record_size);
  log->log_insert_index(id->GetKeyFid(), oid,
                        fat_ptr::make((void *)key, size_code),
                        DEFAULT_ALIGNMENT_BITS, NULL);

  // FIXME(tzwang): correct?
  if (likely(is_primary_idx)) {
    // update write_set
    ASSERT(tuple->pvalue->size() == tuple->size);
    add_to_write_set(tuple_array->get(oid));
  }
}

rc_t transaction::DoTupleRead(dbtuple *tuple, varstr *out_v) {
  ASSERT(tuple);
  ASSERT(xc);
  bool read_my_own =
      (tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_XID);
  ASSERT(not read_my_own or
         (read_my_own and
          XID::from_ptr(tuple->GetObject()->GetClsn()) == xc->owner));
  ASSERT(not read_my_own or not(flags & TXN_FLAG_READ_ONLY));

#if defined(SSI) || defined(SSN) || defined(MVOCC)
  if (not read_my_own) {
    rc_t rc = {RC_INVALID};
    if (flags & TXN_FLAG_READ_ONLY) {
#if defined(SSI) || defined(SSN)
      if (config::enable_safesnap) {
        return rc_t{RC_TRUE};
      }
#endif
    } else {
#ifdef SSN
      rc = ssn_read(tuple);
#elif defined(SSI)
      rc = ssi_read(tuple);
#else
      rc = mvocc_read(tuple);
#endif
    }
    if (rc_is_abort(rc)) return rc;
  }  // otherwise it's my own update/insert, just read it
#endif

  // do the actual tuple read
  return tuple->DoRead(out_v, !read_my_own);
}

#ifdef SSN
rc_t transaction::ssn_read(dbtuple *tuple) {
  auto v_clsn = tuple->GetObject()->GetClsn().offset();
  // \eta - largest predecessor. So if I read this tuple, I should commit
  // after the tuple's creator (trivial, as this is committed version, so
  // this tuple's clsn can only be a predecessor of me): so just update
  // my \eta if needed.
  if (xc->pstamp < v_clsn) xc->pstamp = v_clsn;

  auto tuple_sstamp = volatile_read(tuple->sstamp);
  if (tuple_sstamp.asi_type() == fat_ptr::ASI_LOG) {
    // have committed overwrite
    if (xc->sstamp > tuple_sstamp.offset() or xc->sstamp == 0)
      xc->sstamp = tuple_sstamp.offset();  // \pi
  } else {
    ASSERT(tuple_sstamp == NULL_PTR or
           tuple_sstamp.asi_type() == fat_ptr::ASI_XID);
    // Exclude myself
    if (tuple_sstamp != NULL_PTR and XID::from_ptr(tuple_sstamp) == xc->owner)
      return {RC_TRUE};

    // If there's no (committed) overwrite so far, we need to track this read,
    // unless it's an old version.
    ASSERT(tuple_sstamp == NULL_PTR or
           XID::from_ptr(tuple_sstamp) != xc->owner);
    if (tuple->is_old(xc)) {
      ASSERT(is_read_mostly());
      // Aborting long read-mostly transactions is expensive, spin first
      static const uint32_t kSpins = 100000;
      uint32_t spins = 0;
      while (not tuple->set_persistent_reader()) {
        if (++spins >= kSpins) {
          return {RC_ABORT_RW_CONFLICT};
        }
      }
    } else {
      // Now if this tuple was overwritten by somebody, this means if I read
      // it, that overwriter will have anti-dependency on me (I must be
      // serialized before the overwriter), and it already committed (as a
      // successor of mine), so I need to update my \pi for the SSN check.
      // This is the easier case of anti-dependency (the other case is T1
      // already read a (then latest) version, then T2 comes to overwrite it).
      read_set->emplace_back(tuple);
    }
    serial_register_reader_tx(xc->owner, &tuple->readers_bitmap);
  }

#ifdef EARLY_SSN_CHECK
  if (not ssn_check_exclusion(xc)) return {RC_ABORT_SERIAL};
#endif
  return {RC_TRUE};
}
#endif

#ifdef SSI
rc_t transaction::ssi_read(dbtuple *tuple) {
  // Consider the dangerous structure that could lead to non-serializable
  // execution: T1 r:w T2 r:w T3 where T3 committed first. Read() needs
  // to check if I'm the T1 and do bookeeping if I'm the T2 (pivot).
  // See tuple.h for explanation on what s2 means.
  if (volatile_read(tuple->s2)) {
    // Read-only optimization: s2 is not a problem if we're read-only and
    // my begin ts is earlier than s2.
    if (not config::enable_ssi_read_only_opt or write_set.size() > 0 or
        xc->begin >= tuple->s2) {
      // sstamp will be valid too if s2 is valid
      ASSERT(tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
      return rc_t{RC_ABORT_SERIAL};
    }
  }

  fat_ptr tuple_s1 = volatile_read(tuple->sstamp);
  // see if there was a guy with cstamp=tuple_s1 who overwrote this version
  if (tuple_s1.asi_type() == fat_ptr::ASI_LOG) {  // I'm T2
    // remember the smallest sstamp and use it during precommit
    if (not xc->ct3 or xc->ct3 > tuple_s1.offset()) xc->ct3 = tuple_s1.offset();
    // The purpose of adding a version to read-set is to re-check its
    // sstamp status at precommit and set its xstamp for updaters' (if
    // exist) reference during pre-commit thru the readers bitmap.
    // The xstamp is only useful for versions that haven't been updated,
    // or whose updates haven't been finalized. It's only used by the
    // updater at precommit. Once updated (ie v.sstamp is ASI_LOG), future
    // readers of this version (ie it started realy early and is perhaps a
    // long tx) will read the version's sstamp and s2 values. So read won't
    // need to add the version in read-set if it's overwritten.
  } else {
    // survived, register as a reader
    // After this point, I'll be visible to the updater (if any)
    serial_register_reader_tx(xc->owner, &tuple->readers_bitmap);
    read_set->emplace_back(tuple);
  }
  return {RC_TRUE};
}
#endif

#ifdef MVOCC
rc_t transaction::mvocc_read(dbtuple *tuple) {
  read_set->emplace_back(tuple);
  return rc_t{RC_TRUE};
}
#endif
}  // namespace ermia
