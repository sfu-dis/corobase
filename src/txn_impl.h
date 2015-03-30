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
  //write_set.clear();
  xc->read_set.clear();

#ifdef ENABLE_GC
  MM::epoch_exit(xc->end, epoch);
#endif
  xid_free(xid);    // must do this after epoch_exit, which uses xc.end
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::abort_impl()
{
  if (likely(state() != TXN_COMMITTING))
    volatile_write(xc->state, TXN_ABRTD);

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
    w.btr->unlink_tuple(w.oid, tuple);
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
    object *obj = (object *)((char *)tuple - sizeof(object));
    op->put(xc->end.offset(), obj);
#endif
  }

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  for (auto &r : xc->read_set) {
    ASSERT(not r->is_defunct());
    ASSERT(r->clsn.asi_type() == fat_ptr::ASI_LOG);
    // remove myself from reader list
    serial_deregister_reader_tx(r);
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
    return rc_t{RC_ABORT_INTERNAL};

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
      auto reader_owner = volatile_read(reader_xc->owner);
      auto reader_end = volatile_read(reader_xc->end).offset();
      auto reader_begin = volatile_read(reader_xc->begin).offset();
      if (reader_owner != rxid)
          goto get_reader;
      if (not overwritten_tuple->is_old(xc)) {
        if (reader_end and reader_end < cstamp and wait_for_commit_result(reader_xc)) {
          if (xc->pstamp < reader_end)
            xc->pstamp = reader_end;
        }
      }
      else {  // old version
        auto tuple_bs = volatile_read(overwritten_tuple->bstamp);
        // I (as the writer) need to backoff if the reader has the
        // possibility of having read the version, and it is or will
        // be serialized after me (ie in-flight, end=0).
        if ((not reader_end or reader_end >= cstamp) and reader_begin <= tuple_bs) {
          return rc_t{RC_ABORT_RW_CONFLICT};
        }
        xc->pstamp = cstamp - 1;
      }
    }
  }
  ASSERT(xc->pstamp <= cstamp - 1);

  for (auto &r : xc->read_set) {
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

      // overwriter might haven't committed, be serialized after me, or before me
      // we only care if the successor is serialized *before* me.
      if (sucessor_end and sucessor_end < cstamp and wait_for_commit_result(sucessor_xc)) {
        if (sucessor_end < xc->sstamp)
          xc->sstamp = sucessor_end;
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
    tuple->do_write(w.writer);
    dbtuple *next_tuple = tuple->next();
    ASSERT(not next_tuple or
           ((object *)((char *)tuple - sizeof(object)))->_next.offset() ==
           (uint64_t)((char *)next_tuple - sizeof(object)));
    if (next_tuple) {   // update, not insert
      ASSERT(volatile_read(next_tuple->clsn).asi_type());
      ASSERT(xc->sstamp and xc->sstamp != ~uint64_t{0});
      ASSERT(not next_tuple->sstamp.offset());
      volatile_write(next_tuple->sstamp, LSN::make(xc->sstamp, 0).to_log_ptr());
      ASSERT(next_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
    }
    volatile_write(tuple->xstamp, cstamp);
    volatile_write(tuple->clsn, clsn.to_log_ptr());
    ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
    if (tuple->next()) {
      // construct the (sub)list here so that we have only one CAS per tx
      recycle_oid *r = new recycle_oid((uintptr_t)w.btr, w.oid);
      if (not updated_oids_head)
        updated_oids_head = updated_oids_tail = r;
      else {
        updated_oids_tail->next = r;
        updated_oids_tail = r;
      }
    }
#endif
  }

  // change state
  volatile_write(xc->state, TXN_CMMTD);

#ifdef ENABLE_GC
  if (updated_oids_head) {
    ASSERT(updated_oids_tail);
    MM::recycle(updated_oids_head, updated_oids_tail);
  }
#endif

  for (auto &r : xc->read_set) {
    ASSERT(r->clsn.asi_type() == fat_ptr::ASI_LOG);
    uint64_t xlsn;
    do {
      xlsn = volatile_read(r->xstamp);
    }
    while (xlsn < cstamp and not __sync_bool_compare_and_swap(&r->xstamp, xlsn, cstamp));
    // remove myself from readers set, so others won't see "invalid XID" while enumerating readers
    serial_deregister_reader_tx(r);
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
  if (xc->end == INVALID_LSN)
    return rc_t{RC_ABORT_INTERNAL};
  auto cstamp = xc->end.offset();

  // get the smallest s1 in each tuple we have read (ie, the smallest cstamp
  // of T3 in the dangerous structure that clobbered our read)
  uint64_t ct3 = volatile_read(xc->ct3);   // this will be the s2 of versions I clobbered
  if (ct3 == 1) // poor guy clobbered some old version...
    goto examine_writes;
  for (auto &r : xc->read_set) {
  get_overwriter:
    // need to see if there's any overwriter (if so also its state)
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
      // if the overwriter is serialized **before** me, I need to spin to find
      // out its final commit result
      // don't bother if it's serialized after me or not even in precommit
      if (overwriter_end and overwriter_end < cstamp and wait_for_commit_result(overwriter_xc))
        tuple_s1 = overwriter_end;
    }
    else {    // already committed, read tuple's sstamp
        ASSERT(overwriter_clsn.asi_type() == fat_ptr::ASI_LOG);
        tuple_s1 = volatile_read(overwriter_clsn).offset();
    }

    if (tuple_s1 and (not ct3 or ct3 > tuple_s1))
      ct3 = tuple_s1;
    // will release read lock (readers bitmap) in post-commit
  }

examine_writes:
  // now see if I'm the unlucky T2
  uint64_t max_xstamp = 0;
  for (auto &w : write_set) {
    if (w.new_tuple->is_defunct())
      continue;
    dbtuple *overwritten_tuple = w.new_tuple->next();
    if (not overwritten_tuple)
      continue;
    // abort if there's any in-flight reader of this tuple or
    // if someone has read the tuple after someone else clobbered it...
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
      auto reader_owner = volatile_read(reader_xc->owner);
      auto reader_begin = volatile_read(reader_xc->begin).offset();
      auto reader_state = volatile_read(reader_xc->state);
      if (reader_owner != rxid)
          goto get_reader;
      // If I clobbered an old version, I have to assume a committed T3
      // exists (since there's no way to check my reads). If a reader of
      // that version exists, I also have to assume it (as T1) is serialized
      // **after** the committed T3. The the remaining is to check if this
      // "T1" is serialized **after** me and has a begin ts <= the version's
      // bstamp (to reduce false +ves, b/c the readers list is not accurate
      // for old versions; it's accurate for young versions tho).
      // (might also spin for reader to really commit to reduce false +ves)

      if (overwritten_tuple->is_old(xc)) {
        uint64_t tuple_bs = volatile_read(overwritten_tuple->bstamp);
        if (reader_begin > tuple_bs) {  // then this is not our guy
          INVARIANT(ct3 == 1);
          continue;
        }
        return rc_t{RC_ABORT_RW_CONFLICT};
      }
      else {    // young tuple, do usual SSI
        // if the reader is still alive, I'm doomed (standard SSI)
        if (reader_state == TXN_ACTIVE)
          return rc_t{RC_ABORT_SERIAL};
        // find the latest reader (ie, largest xstamp of tuples I clobber)
        auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
        if (max_xstamp < tuple_xstamp)
          max_xstamp = tuple_xstamp;
        if (ct3 and max_xstamp >= ct3)
          return rc_t{RC_ABORT_SERIAL};
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
    tuple->do_write(w.writer);
    dbtuple *overwritten_tuple = tuple->next();
    if (overwritten_tuple) {    // update
      ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);
      volatile_write(overwritten_tuple->sstamp, LSN::make(cstamp, 0).to_log_ptr());
      if (ct3 > overwritten_tuple->s2)
        volatile_write(overwritten_tuple->s2, ct3);
    }
    tuple->clsn = xc->end.to_log_ptr();
    INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
    if (tuple->next()) {
      // construct the (sub)list here so that we have only one CAS per tx
      recycle_oid *r = new recycle_oid((uintptr_t)w.btr, w.oid);
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

  // update xstamps in read versions
  for (auto &r : xc->read_set) {
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
    serial_deregister_reader_tx(r);
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
  for (auto &w : write_set) {
    dbtuple* tuple = w.new_tuple;
    if (tuple->is_defunct())
      continue;
    tuple->do_write(w.writer);
    tuple->clsn = xc->end.to_log_ptr();
    INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
#ifdef ENABLE_GC
    if (tuple->next()) {
      // construct the (sub)list here so that we have only one CAS per tx
      recycle_oid *r = new recycle_oid((uintptr_t)w.btr, w.oid);
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
#elif defined(PHANTOM_PROT_NODE_SET)
  // update node #s
  INVARIANT(insert_info.node);
  if (!absent_set.empty()) {
    auto it = absent_set.find(insert_info.node);
    if (it != absent_set.end()) {
      if (unlikely(it->second.version != insert_info.old_version)) {
        // important: unlink the version, otherwise we risk leaving a dead
        // version at chain head -> infinite loop or segfault...
        btr->unlink_tuple(oid, tuple);
        return false;
      }
      // otherwise, bump the version
      it->second.version = insert_info.new_version;
      SINGLE_THREADED_INVARIANT(concurrent_btree::ExtractVersionNumber(it->first) == it->second);
    }
  }
#endif

  // insert to log
  // FIXME: tzwang: leave pdest as null and FID is always 1 now.
  INVARIANT(log);
  ASSERT(tuple->size == value->size());
  auto record_size = align_up((size_t)tuple->size + sizeof(varstr));
  auto size_code = encode_size_aligned(record_size);
  ASSERT(not ((uint64_t)value & ((uint64_t)0xf)));
  ASSERT(tuple->size);
  log->log_insert(1,
                  oid,
                  fat_ptr::make((void *)value, size_code),
                  DEFAULT_ALIGNMENT_BITS, NULL);
  // update write_set
  write_set.emplace_back(tuple, writer, btr, oid);
  return true;
}

template <template <typename> class Protocol, typename Traits>
template <typename ValueReader>
rc_t
transaction<Protocol, Traits>::do_tuple_read(
    dbtuple *tuple, ValueReader &value_reader)
{
  INVARIANT(tuple);
  ++evt_local_search_lookups;
  ASSERT(xc);
  bool read_my_own = (tuple->clsn.asi_type() == fat_ptr::ASI_XID);
  ASSERT(not read_my_own or (read_my_own and XID::from_ptr(volatile_read(tuple->clsn)) == xc->owner));

#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
  if (not read_my_own) {
    rc_t rc = {RC_INVALID};
#ifdef USE_PARALLEL_SSN
    rc = ssn_read(xc, tuple);
#else
    rc = ssi_read(xc, tuple);
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
  INVARIANT(stat == dbtuple::READ_EMPTY ||
            stat == dbtuple::READ_RECORD);
  if (stat == dbtuple::READ_EMPTY) {
    ++transaction_base::g_evt_read_logical_deleted_node_search;
    return rc_t{RC_FALSE};
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
    return rc_t{RC_ABORT_PHANTOM};
  }
  return rc_t{RC_TRUE};
}
#endif

#endif /* _NDB_TXN_IMPL_H_ */
