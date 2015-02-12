#ifndef _NDB_TXN_IMPL_H_
#define _NDB_TXN_IMPL_H_

#include "txn.h"
#include "lockguard.h"

#include "object.h"
#include "dbcore/sm-common.h"

using namespace TXN;

// base definitions

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::transaction(uint64_t flags, string_allocator_type &sa)
  : transaction_base(flags), xid(TXN::xid_alloc()), xc(xid_get_context(xid)), sa(&sa)
{
  RA::epoch_enter();
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::NodeLockRegionBegin();
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  serial_register_tx(xid);
#endif
  write_set.set_empty_key(NULL);    // google dense map
#ifdef PHANTOM_PROT_NODE_SET
  absent_set.set_empty_key(NULL);    // google dense map
#endif
  RCU::rcu_enter();
  log = logger->new_tx_log();
  xc->begin = logger->cur_lsn();
  xc->end = INVALID_LSN;
  xc->state = TXN_EMBRYO;
}

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::~transaction()
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
  xid_free(xid);
  //write_set.clear();
  //read_set.clear();
  RA::epoch_exit();
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::abort_impl()
{
  if (likely(state() != TXN_COMMITTING))
    volatile_write(xc->state, TXN_ABRTD);

  for (auto &w : write_set) {
    if (not w.second.btr)   // for repeated overwrites
        continue;
    dbtuple *tuple = w.second.new_tuple;
    ASSERT(tuple);
    ASSERT(XID::from_ptr(tuple->clsn) == xid);
    w.second.btr->unlink_tuple(w.second.oid, tuple);
  }

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  for (auto &r : read_set) {
    ASSERT(r.tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
    ASSERT(r.tuple == r.btr->fetch_committed_version_at(r.oid, xid, LSN::from_ptr(r.tuple->clsn)));
    // remove myself from reader list
    serial_deregister_reader_tx(r.tuple);
  }
#endif

  if (likely(state() != TXN_COMMITTING))
    log->pre_commit();
  log->discard();
  if (unlikely(state() == TXN_COMMITTING))
    volatile_write(xc->state, TXN_ABRTD);
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
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  inline std::string
  transaction_flags_to_str(uint64_t flags)
  {
    bool first = true;
    std::ostringstream oss;
    if (flags & transaction_base::TXN_FLAG_LOW_LEVEL_SCAN) {
      oss << "TXN_FLAG_LOW_LEVEL_SCAN";
      first = false;
    }
    if (flags & transaction_base::TXN_FLAG_READ_ONLY) {
      if (first)
        oss << "TXN_FLAG_READ_ONLY";
      else
        oss << " | TXN_FLAG_READ_ONLY";
      first = false;
    }
    return oss.str();
  }
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::dump_debug_info() const
{
  std::cerr << "Transaction (obj=" << util::hexify(this) << ") -- state "
       << transaction_state_to_cstr(state()) << std::endl;
  //std::cerr << "  Abort Reason: " << AbortReasonStr(reason) << std::endl;
  std::cerr << "  Flags: " << transaction_flags_to_str(flags) << std::endl;
}

template <template <typename> class Protocol, typename Traits>
rc_t
transaction<Protocol, Traits>::commit()
{
#ifdef PHANTOM_PROT_NODE_SET
  if (not check_phantom())
    return rc_t{RC_ABORT};
#endif
#ifdef USE_PARALLEL_SSN
  return parallel_ssn_commit();
#elif defined USE_PARALLEL_SSI
  return parallel_ssi_commit();
#else
  return si_commit();
#endif
}

#ifdef USE_PARALLEL_SSN
template <template <typename> class Protocol, typename Traits>
rc_t
transaction<Protocol, Traits>::parallel_ssn_commit()
{
  PERF_DECL(
      static std::string probe0_name(
        std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe0_name.c_str(), &transaction_base::g_txn_commit_probe0_cg);

  switch (state()) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    volatile_write(xc->state, TXN_COMMITTING);
    break;
  case TXN_CMMTD:
  case TXN_COMMITTING:
  case TXN_ABRTD:
    ALWAYS_ASSERT(false);
  }

  INVARIANT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit();
  LSN clsn = xc->end;
  auto cstamp = clsn.offset();
  if (xc->end == INVALID_LSN)
    return rc_t{RC_ABORT};

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
    if (not w.second.btr)   // for repeated overwrites
        continue;
    dbtuple *tuple = w.second.new_tuple;

    // go to the precommitted or committed version I (am about to)
    // overwrite for the reader list
    dbtuple *overwritten_tuple = w.first;   // the key
    ASSERT(tuple == overwritten_tuple or
           ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
           (uint64_t)((char *)overwritten_tuple - sizeof(object)));
    if (overwritten_tuple == tuple) // insert, see do_tree_put for the rules
      continue;

    int64_t age = 0;
    // note fetch_overwriter might return a tuple with clsn being an XID
    // (of a precommitted but still in post-commit transaction).
  try_get_age:
    fat_ptr overwritten_tuple_clsn = volatile_read(overwritten_tuple->clsn);
    if (overwritten_tuple_clsn.asi_type() == fat_ptr::ASI_XID) {
      // then that tx must have pre-committed, ie it has a valid xc->end
      // and need to go to the tx's context to find out its cstamp to
      // calculate age
      XID overwritten_xid = XID::from_ptr(overwritten_tuple_clsn);
      xid_context *overwritten_xc = xid_get_context(overwritten_xid);
      auto overwritten_end = volatile_read(overwritten_xc->end).offset();
      ASSERT(overwritten_end);
      XID overwritten_owner = volatile_read(overwritten_xc->owner);
      if (overwritten_owner != overwritten_xid)
        goto try_get_age;
      age = xc->begin.offset() - overwritten_end;
    }
    else {
      ASSERT(overwritten_tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
      age = xc->begin.offset() - overwritten_tuple->clsn.offset();
    }
    ASSERT(volatile_read(overwritten_tuple->sstamp) == 0);

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
      // copy everything before doing anything
      auto reader_owner = volatile_read(reader_xc->owner);
      auto reader_end = volatile_read(reader_xc->end).offset();
      auto reader_begin = volatile_read(reader_xc->begin).offset();
      if (reader_owner != rxid)
          goto get_reader;
      if (age < OLD_VERSION_THRESHOLD) {
        if (reader_end and reader_end < cstamp and wait_for_commit_result(reader_xc)) {
          if (xc->pstamp < reader_end)
            xc->pstamp = reader_end;
        }
      }
      else {  // old version
        auto tuple_bs = volatile_read(overwritten_tuple->bstamp);
        // I (as the writer) need to backoff if the reader has the
        // possibility of having read the version, and it is or will
        // be serialized after me.
        if (reader_begin < tuple_bs and reader_end >= cstamp) {
          tls_rw_conflict_abort_count++;
          return rc_t{RC_ABORT_RW_CONFLICT};
        }
        xc->pstamp = cstamp - 1;
      }
    }
  }
  ASSERT(xc->pstamp <= cstamp - 1);

  for (auto &r : read_set) {
    // skip writes (note we didn't remove the one in read set)
    if (write_set[r.tuple].btr)
      continue;
    // so tuple should be the committed version I read
    ASSERT(r.tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
    dbtuple *overwriter_tuple = (dbtuple *)r.btr->fetch_overwriter(r.oid,
                                                  LSN::from_ptr(r.tuple->clsn));
    if (not overwriter_tuple)
      continue;

  try_get_sucessor:
    // read tuple->slsn to a local variable before doing anything relying on it,
    // it might be changed any time...
    fat_ptr sucessor_clsn = volatile_read(overwriter_tuple->clsn);

    // overwriter in progress?
    if (sucessor_clsn.asi_type() == fat_ptr::ASI_XID) {
      XID successor_xid = XID::from_ptr(sucessor_clsn);
      xid_context *sucessor_xc = xid_get_context(successor_xid);
      auto successor_owner = volatile_read(sucessor_xc->owner);
      if (successor_owner == xc->owner)  // myself
          continue;

      // read everything before doing anything
      auto sucessor_end = volatile_read(sucessor_xc->end).offset();
      if (successor_owner != successor_xid)
        goto try_get_sucessor;

      // overwriter might haven't committed, be serialized after me, or before me
      if (not sucessor_end) // not even in precommit, don't bother
          ;
      else if (sucessor_end > cstamp)    // serialzed after me, (dependency trivially satisfied as I as the reader will (hopefully) commit first)
          ;
      else {
        if (wait_for_commit_result(sucessor_xc)) {    // either wait or give conservative estimation
          // now read the sucessor stamp (sucessor's clsn, as sucessor needs to fill its clsn to the overwritten tuple's slsn at post-commit)
          if (sucessor_end < xc->sstamp)
            xc->sstamp = sucessor_end;
        } // otherwise aborted, ignore
      }
    }
    else {
      // overwriter already fully committed/aborted or no overwriter at all
      ASSERT(sucessor_clsn.asi_type() == fat_ptr::ASI_LOG);
      uint64_t tuple_sstamp = volatile_read(r.tuple->sstamp);
      // tuple_sstamp = 0 means no one has overwritten this version so far
      if (tuple_sstamp and tuple_sstamp < xc->sstamp)
        xc->sstamp = tuple_sstamp;
    }
  }

  if (not ssn_check_exclusion(xc))
    return rc_t{RC_ABORT_SSN_EXCLUSION};

  // ok, can really commit if we reach here
  log->commit(NULL);

  // change state
  volatile_write(xc->state, TXN_CMMTD);

  // post-commit: stuff access stamps for reads; init new versions
  for (auto &w : write_set) {
    if (not w.second.btr)   // for repeated overwrites
        continue;
    dbtuple *tuple = w.second.new_tuple;
    dbtuple *next_tuple = w.first;
    ASSERT(tuple == next_tuple or
           ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
           (uint64_t)((char *)next_tuple - sizeof(object)));
    if (tuple != next_tuple) {   // update, not insert
      ASSERT(volatile_read(next_tuple->clsn).asi_type());
      // FIXME: tuple's sstamp should never decrease?
      ASSERT(xc->sstamp and xc->sstamp != ~uint64_t{0});
      volatile_write(next_tuple->sstamp, xc->sstamp);
    }
    volatile_write(tuple->xstamp, cstamp);
    volatile_write(tuple->clsn, clsn.to_log_ptr());
    ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
  }

  for (auto &r : read_set) {
    if (write_set[r.tuple].btr)
      continue;
    ASSERT(r.tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
    uint64_t xlsn;
    do {
      xlsn = volatile_read(r.tuple->xstamp);
    }
    while (xlsn < cstamp and not __sync_bool_compare_and_swap(&r.tuple->xstamp, xlsn, cstamp));
    // remove myself from readers set, so others won't see "invalid XID" while enumerating readers
    serial_deregister_reader_tx(r.tuple);
  }
  return rc_t{RC_TRUE};
}
#elif defined(USE_PARALLEL_SSI)
template <template <typename> class Protocol, typename Traits>
rc_t
transaction<Protocol, Traits>::parallel_ssi_commit()
{
  PERF_DECL(
      static std::string probe0_name(
        std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe0_name.c_str(), &transaction_base::g_txn_commit_probe0_cg);

  switch (state()) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    xc->state = TXN_COMMITTING;
    break;
  case TXN_CMMTD:
  case TXN_COMMITTING:
  case TXN_ABRTD:
    ALWAYS_ASSERT(false);
  }

  INVARIANT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit();
  if (xc->end == INVALID_LSN)
    return rc_t{RC_ABORT};
  auto cstamp = xc->end.offset();

  // get the smallest s1 in each tuple we have read (ie, the smallest cstamp
  // of T3 in the dangerous structure that clobbered our read)
  uint64_t min_read_s1 = 0;    // this will be the s2 of versions I clobbered
  for (auto &r : read_set) {
    if (write_set[r.tuple].btr)
      continue;
    auto tuple_s1 = volatile_read(r.tuple->sstamp);
    if (not tuple_s1) {
      // need to see if there's any overwritter (if so also its state)
      dbtuple *overwriter_tuple = (dbtuple *)r.btr->fetch_overwriter(r.oid,
                                                    LSN::from_ptr(r.tuple->clsn));
      if (not overwriter_tuple)
        continue;
    get_overwriter:
      fat_ptr overwriter_xid = volatile_read(overwriter_tuple->clsn);
      if (overwriter_xid.asi_type() == fat_ptr::ASI_XID) {
        XID ox = XID::from_ptr(overwriter_xid);
        ASSERT(ox != xc->owner);
        xid_context *overwriter_xc = xid_get_context(ox);
        // read what i need before verifying ownership
        uint64_t overwriter_end = volatile_read(overwriter_xc->end).offset();
        if (volatile_read(overwriter_xc->owner) != ox)
          goto get_overwriter;
        // if the overwriter is serialized **before** me, I need to spin to find
        // out its final commit result
        // don't bother if it's serialized after me or not even in precommit
        if (overwriter_end and overwriter_end < cstamp and wait_for_commit_result(overwriter_xc)) {
          tuple_s1 = overwriter_end;
        }
      }
      else {    // already committed, re-read tuple's sstamp
          tuple_s1 = volatile_read(r.tuple->sstamp);
      }
    }
    if (tuple_s1 and (not min_read_s1 or min_read_s1 > tuple_s1))
      min_read_s1 = tuple_s1;
    // will release read lock (readers bitmap) in post-commit
  }

  // now see if I'm the unlucky T2
  if (min_read_s1) {
    uint64_t max_xstamp = 0;
    for (auto &w : write_set) {
      dbtuple *overwritten_tuple = w.first;
      if (not w.second.btr)
        continue;
      // abort if there's any in-flight reader of this tuple or
      // if someone has read the tuple after someone else clobbered it...
      if (serial_get_tuple_readers(overwritten_tuple, true)) {
        tls_serial_abort_count++;
        return rc_t{RC_ABORT_SSI};
      }
      else {
        // find the latest reader (ie, largest xstamp of tuples I clobber)
        // FIXME: tuple_xstamp > xc->begin, correct? ie the read should
        // have happened after i started
        auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
        if (max_xstamp < tuple_xstamp)
          max_xstamp = tuple_xstamp;
        if (max_xstamp >= min_read_s1 and has_committed_t3(xc)) {
          tls_serial_abort_count++;
          return rc_t{RC_ABORT_SSI};
        }
      }
    }
  }

  // survived!
  log->commit(NULL);

  // change state
  volatile_write(xc->state, TXN_CMMTD);

  // stamp overwritten versions, stuff clsn
  for (auto &w : write_set) {
    dbtuple *overwritten_tuple = w.first;
    if (not w.second.btr)
      continue;
    ASSERT(not volatile_read(overwritten_tuple->sstamp));
    dbtuple* tuple = w.second.new_tuple;
    if (overwritten_tuple != tuple) {    // update
      volatile_write(overwritten_tuple->sstamp, cstamp);
      if (min_read_s1 > overwritten_tuple->s2)   // correct?
        volatile_write(overwritten_tuple->s2, min_read_s1);
    }
    tuple->clsn = xc->end.to_log_ptr();
    INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
  }

  // update xstamps in read versions
  for (auto &r : read_set) {
    if (write_set[r.tuple].btr) // correct?
      continue;
    uint64_t xstamp;
    do {
      xstamp = volatile_read(r.tuple->xstamp);
    }
    while (xstamp < cstamp and not __sync_bool_compare_and_swap(&r.tuple->xstamp, xstamp, cstamp));
    serial_deregister_reader_tx(r.tuple);
  }
  return rc_t{RC_TRUE};
}
#else
template <template <typename> class Protocol, typename Traits>
rc_t
transaction<Protocol, Traits>::si_commit()
{
  PERF_DECL(
      static std::string probe0_name(
        std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe0_name.c_str(), &transaction_base::g_txn_commit_probe0_cg);

  switch (state()) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    xc->state = TXN_COMMITTING;
    break;
  case TXN_CMMTD:
  case TXN_COMMITTING: 
  case TXN_ABRTD:
    ALWAYS_ASSERT(false);
  }
  
  INVARIANT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit();
  if (xc->end == INVALID_LSN)
    return rc_t{RC_ABORT};
  log->commit(NULL);

  // change state
  volatile_write(xc->state, TXN_CMMTD);

  // post-commit cleanup: install clsn to tuples
  // (traverse write-tuple)
  // stuff clsn in tuples in write-set
  for (auto &w : write_set) {
    if (not w.second.btr)
      continue;
    dbtuple* tuple = w.second.new_tuple;
    tuple->clsn = xc->end.to_log_ptr();
    INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
  }
  return rc_t{RC_TRUE};
}
#endif

#ifdef PHANTOM_PROT_NODE_SET
// returns true if btree versions have changed, ie there's phantom
template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::check_phantom()
{
  for (auto &r : absent_set) {
    const uint64_t v = concurrent_btree::ExtractVersionNumber(r.first);
    if (unlikely(v != r.second.version))
      return false;
  }
  return true;
}
#endif

typedef object_vector tuple_vector_type;
// FIXME: tzwang: note: we only try once in this function. If it
// failed (returns false) then the caller (supposedly do_tree_put)
// should fallback to normal update procedure.
template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::try_insert_new_tuple(
    concurrent_btree *btr,
    const varstr *key,
    const varstr *value,
    object* object,
    dbtuple::tuple_writer_t writer)
{
  INVARIANT(key);
  char*p = (char*)object;
  dbtuple* tuple = (dbtuple *)object->payload();
  tuple_vector_type* tuple_vector = btr->get_tuple_vector();

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

  oid_type oid = tuple_vector->alloc();
  fat_ptr new_head = fat_ptr::make(object, INVALID_SIZE_CODE, 0);
  if (not tuple_vector->put(oid, new_head)) {
#ifdef PHANTOM_PROT_TABLE_LOCK
    if (instant_xlock)
      object_vector::unlock(l);
#endif
    return false;
  }

  typename concurrent_btree::insert_info_t insert_info;
  if (unlikely(!btr->insert_if_absent(
          varkey(key), oid, tuple, &insert_info))) {
    ++transaction_base::g_evt_dbtuple_write_insert_failed;
    tuple_vector->unlink(oid, tuple);
#ifdef PHANTOM_PROT_TABLE_LOCK
    if (instant_xlock)
      object_vector::unlock(l);
#endif
    return false;
  }

#ifdef PHANTOM_PROT_TABLE_LOCK
    if (instant_xlock)
      object_vector::unlock(l);
#endif

  // insert to log
  // FIXME: tzwang: leave pdest as null and FID is always 1 now.
  INVARIANT(log);
  ASSERT(tuple->size == value->size());
  auto record_size = align_up((size_t)tuple->size + sizeof(varstr));
  auto size_code = encode_size_aligned(record_size);
  ASSERT(not ((uint64_t)p & ((uint64_t)0xf)));
  ASSERT(not ((uint64_t)tuple & ((uint64_t)0xf)));
  ASSERT(not ((uint64_t)value & ((uint64_t)0xf)));
  ASSERT(tuple->size);
  log->log_insert(1,
                  oid,
                  fat_ptr::make((void *)value, size_code),
                  DEFAULT_ALIGNMENT_BITS, NULL);
  // update write_set
  write_set[tuple] = write_record_t(tuple, btr, oid);

#ifdef PHANTOM_PROT_NODE_SET
  // update node #s
  INVARIANT(insert_info.node);
  if (!absent_set.empty()) {
    auto it = absent_set.find(insert_info.node);
    if (it != absent_set.end()) {
      if (unlikely(it->second.version != insert_info.old_version)) {
        return false;
      }
      // otherwise, bump the version
      it->second.version = insert_info.new_version;
      SINGLE_THREADED_INVARIANT(concurrent_btree::ExtractVersionNumber(it->first) == it->second);
    }
  }
#endif

  return true;
}

template <template <typename> class Protocol, typename Traits>
template <typename ValueReader>
rc_t
transaction<Protocol, Traits>::do_tuple_read(
    concurrent_btree *btr_ptr, oid_type oid, dbtuple *tuple, ValueReader &value_reader)
{
  INVARIANT(tuple);
  ++evt_local_search_lookups;
  ASSERT(xc);

#ifdef USE_PARALLEL_SSN
  // SSN stamps and check
  if (tuple->clsn.asi_type() == fat_ptr::ASI_LOG) {
    auto v_clsn = tuple->clsn.offset();
    int64_t age = xc->begin.offset() - v_clsn;
    auto tuple_sstamp = volatile_read(tuple->sstamp);
    if (age < OLD_VERSION_THRESHOLD) {
      // \eta - largest predecessor. So if I read this tuple, I should commit
      // after the tuple's creator (trivial, as this is committed version, so
      // this tuple's clsn can only be a predecessor of me): so just update
      // my \eta if needed.
      if (xc->pstamp < v_clsn)
          xc->pstamp = v_clsn;

      // Now if this tuple was overwritten by somebody, this means if I read
      // it, that overwriter will have anti-dependency on me (I must be
      // serialized before the overwriter), and it already committed (as a
      // successor of mine), so I need to update my \pi for the SSN check.
      // This is the easier case of anti-dependency (the other case is T1
      // already read a (then latest) version, then T2 comes to overwrite it).
      if (not tuple_sstamp) {   // no overwrite so far
        serial_register_reader_tx(tuple, xid);
        read_set.emplace_back(tuple, btr_ptr, oid);
      }
      else if (xc->sstamp > tuple_sstamp)
        xc->sstamp = tuple_sstamp; // \pi

#ifdef DO_EARLY_SSN_CHECKS
      if (not ssn_check_exclusion(xc))
        return {RC_ABORT_SSN_EXCLUSION};
#endif
    }
    else {
      if (tuple_sstamp and xc->sstamp > tuple_sstamp)
        xc->sstamp = tuple_sstamp; // \pi

      uint64_t bs = 0;
      do {
        bs = volatile_read(tuple->bstamp);
      }
      while (tuple->bstamp < xc->begin.offset() and
             not __sync_bool_compare_and_swap(&tuple->bstamp, bs, xc->begin.offset()));
      serial_register_reader_tx(tuple, xid);
    }
  }
#endif
#ifdef USE_PARALLEL_SSI
  // Consider the dangerous structure that could lead to non-serializable
  // execution: T1 r:w T2 r:w T3 where T3 committed first. Read() needs
  // to check if I'm the T1 and do bookeeping if I'm the T2 (pivot).
  if (tuple->clsn.asi_type() == fat_ptr::ASI_LOG) {
    // See tuple.h for explanation on what s2 means.
    if (volatile_read(tuple->s2)) {
      ASSERT(tuple->sstamp);    // sstamp will be valid too if s2 is valid
      tls_serial_abort_count++;
      return rc_t{RC_ABORT_SSI};
    }

    uint64_t tuple_s1 = volatile_read(tuple->sstamp);
    // see if there was a guy with cstamp=tuple_s1 who overwrote this version
    if (tuple_s1) { // I'm T2
      // remember the smallest sstamp so that I can re-check at precommit
      if (xc->ct3 > tuple_s1)
        xc->ct3 = tuple_s1;
    }

    // survived, register as a reader and add to read set
    if (serial_register_reader_tx(tuple, xid))
      read_set.emplace_back(tuple, btr_ptr, oid);
  }
#endif

  // do the actual tuple read
  dbtuple::ReadStatus stat;
  {
    PERF_DECL(static std::string probe0_name(std::string(__PRETTY_FUNCTION__) + std::string(":do_read:")));
    ANON_REGION(probe0_name.c_str(), &private_::txn_btree_search_probe0_cg);
    tuple->prefetch();
    stat = tuple->stable_read(value_reader, this->string_allocator());
    if (unlikely(stat == dbtuple::READ_FAILED))
      return rc_t{RC_ABORT};
  }
  INVARIANT(stat == dbtuple::READ_EMPTY ||
            stat == dbtuple::READ_RECORD);
  if (stat == dbtuple::READ_EMPTY) {
    ++transaction_base::g_evt_read_logical_deleted_node_search;
    return {RC_FALSE};
  }

  return {RC_TRUE};
}

#ifdef PHANTOM_PROT_NODE_SET
template <template <typename> class Protocol, typename Traits>
rc_t
transaction<Protocol, Traits>::do_node_read(
    const typename concurrent_btree::node_opaque_t *n, uint64_t v)
{
  INVARIANT(n);
  auto it = absent_set.find(n);
  if (it == absent_set.end()) {
    absent_set[n].version = v;
  } else if (it->second.version != v) {
    return rc_t{RC_ABORT};
  }
  return rc_t{RC_TRUE};
}
#endif

#endif /* _NDB_TXN_IMPL_H_ */
