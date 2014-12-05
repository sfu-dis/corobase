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
  : transaction_base(flags), sa(&sa)
{
  RA::epoch_enter();
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::NodeLockRegionBegin();
#endif
  xid_context *xc = xid_get_context(xid);
  RCU::rcu_enter();
  xc->begin = logger->cur_lsn();
  RCU::rcu_enter();
  xc->end = INVALID_LSN;
  xc->state = TXN_EMBRYO;
}

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_EMBRYO, TXN_CMMTD, and TXN_ABRTD
  INVARIANT(state() != TXN_ACTIVE && state() != TXN_COMMITTING);

#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::AssertAllNodeLocksReleased();
#endif
  access_set.clear();
  RA::epoch_exit();
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::signal_abort(abort_reason reason)
{
  abort_trap(reason);
  volatile_write(xid_get_context(xid)->state, TXN_ABRTD);
  throw transaction_abort_exception(reason);
}
  
template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::abort_impl()
{
  INVARIANT(state() == TXN_ABRTD);
  
  // unlink from version chain
  typename access_set_map::iterator it     = access_set.begin();
  typename access_set_map::iterator it_end = access_set.end();
  for (; it != it_end; ++it) {
    if (not it->second.is_write())
      continue;
    dbtuple* tuple = it->second.get_tuple();
	concurrent_btree* btr = it->first.tree_ptr;
	btr->unlink_tuple( tuple->oid, (typename concurrent_btree::value_type)tuple );
  }

  // log discard
  RCU::rcu_enter();
  log->pre_commit();
  log->discard();
  RCU::rcu_exit();

  // now. safe to free XID
  xid_free(xid);

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
  std::cerr << "  Abort Reason: " << AbortReasonStr(reason) << std::endl;
  std::cerr << "  Flags: " << transaction_flags_to_str(flags) << std::endl;
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::commit()
{
#ifdef USE_SERIAL_SSN
  ssn_serial_si_commit();
#else
#ifdef USE_PARALLEL_SSN
  ssn_parallel_si_commit();
#else
  si_commit();
#endif
#endif
}

#ifdef USE_SERIAL_SSN
template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::ssn_serial_si_commit()
{
  xid_context* xc = xid_get_context(xid);

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

  // avoid cross init after goto do_abort
  typename access_set_map::iterator it     = access_set.begin();
  typename access_set_map::iterator it_end = access_set.end();

  // ssn protocol
  ssn_commit_mutex.lock();

  INVARIANT(log);
  // get clsn, abort if failed
  RCU::rcu_enter();
  xc->end = log->pre_commit();
  LSN clsn = xc->end;
  if (xc->end == INVALID_LSN)
    signal_abort(ABORT_REASON_INTERNAL);

  for (it = access_set.begin(); it != it_end; ++it) {
    dbtuple* tuple = it->second.get_tuple();
    if (it->second.is_write()) {
      if (tuple->prev) {    // ok, this is an update
        // need access stamp , i.e., who read this version that I'm trying to overwrite?
        // (it's the predecessor, \eta)
        // hehe need to go to that committed version really.... because the one we copied
        // during update might be out-of-date now (sb. already stuffed a new pstamp).
        // so go to prev (do_tree_put and obj_vec->put make sure this is the committed version) to 
        // get access stamp
        if (xc->hi < tuple->prev->xlsn)
          xc->hi = tuple->prev->xlsn;
      }
    }
    else {
      if (xc->lo > clsn)
        xc->lo = clsn;
      // need to finalize the sucessor stamp (\pi)
      if (xc->lo > tuple->slsn)
        xc->lo = tuple->slsn;
    }
  }

  if (not ssn_check_exclusion(xc))
    signal_abort(ABORT_REASON_SSN_EXCLUSION_FAILURE);

  // survived! stuff access stamps for reads, and init new versions
  for (it = access_set.begin(); it != it_end; ++it) {
    dbtuple *tuple = it->second.get_tuple();
    if (it->second.is_write()) {
      if (tuple->prev)  // could be an insert...
        tuple->prev->slsn = xc->lo;
      tuple->clsn = clsn.to_log_ptr();
      tuple->xlsn = clsn;
    }
    else {
      if (tuple->xlsn < clsn)
        tuple->xlsn = clsn;
    }
  }
  ssn_commit_mutex.unlock();

  // ok, can really commit if we reach here
  log->commit(NULL);
  RCU::rcu_exit();

  // change state
  volatile_write(xid_get_context(xid)->state, TXN_CMMTD);

  // done
  xid_free(xid);
}
#else
#ifdef USE_PARALLEL_SSN
template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::ssn_parallel_si_commit()
{
  xid_context* xc = xid_get_context(xid);

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

  // avoid cross init after goto do_abort
  typename access_set_map::iterator it     = access_set.begin();
  typename access_set_map::iterator it_end = access_set.end();

  INVARIANT(log);
  // get clsn, abort if failed
  RCU::rcu_enter();
  xc->end = log->pre_commit();
  LSN clsn = xc->end;
  if (xc->end == INVALID_LSN)
    signal_abort(ABORT_REASON_INTERNAL);

  // find out my largest predecessor (\eta) and smallest sucessor (\pi)
  // for reads, see if sb. has written the tuples - look at sucessor lsn
  // for writes, see if sb. has read the tuples - look at access lsn
  for (it = access_set.begin(); it != it_end; ++it) {
    dbtuple* tuple = it->second.get_tuple();
    if (it->second.is_write()) {
      if (tuple->prev) {    // ok, this is an update
        // need access stamp , i.e., who read this version that I'm trying to overwrite?
        // (it's the predecessor, \eta)
        // need to go to that committed version, because the one we copied
        // during update might be out-of-date now (sb. already stuffed a new pstamp).
        // so go to prev (do_tree_put and obj_vec->put make sure this is the committed version)
      // FIXME
      //try_get_predecessor:
      //  LSN prev_xlsn = volatile_read(tuple->prev->xlsn);
      //  if (xc->hi < prev_xlsn)
      //    xc->hi = prev_xlsn;
      }
    }
    else {
      // so tuple should be the committed version I read
      ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);

    try_get_sucessor:
      // read tuple->slsn to a local variable before doing anything relying on it,
      // it might be changed any time...
      fat_ptr tuple_slsn = volatile_read(tuple->slsn);

      // overwriter in progress?
      if (tuple_slsn.asi_type() == fat_ptr::ASI_XID) {
        xid_context *sucessor_xc = xid_get_context(XID::from_ptr(tuple_slsn));
        if (sucessor_xc->owner != XID::from_ptr(tuple_slsn))
          goto try_get_sucessor;

        // overwriter might haven't committed, be serialized after me, or before me
        if (sucessor_xc->end == INVALID_LSN) // not even in precommit, don't bother
            ;
        else if (sucessor_xc->end > clsn)    // serialzed after me, (dependency trivially satisfied as I as the reader will (hopefully) commit first)
            ;
        else {
          // either wait or give conservative estimation
          while (sucessor_xc->state != TXN_CMMTD);

          // now read the sucessor stamp (sucessor's clsn, as sucessor needs to fill its clsn to the overwritten tuple's slsn at post-commit)
          if (sucessor_xc->end < xc->lo)
            xc->lo = sucessor_xc->end;
        }
      }
      else {    // overwriter already fully committed (slsn available)
        ASSERT(tuple_slsn.asi_type() == fat_ptr::ASI_LOG);
        if (LSN::from_ptr(tuple_slsn) < xc->lo)
          xc->lo = LSN::from_ptr(tuple_slsn);
      }
    }
  }

  if (not ssn_check_exclusion(xc))
    signal_abort(ABORT_REASON_SSN_EXCLUSION_FAILURE);

  // ok, can really commit if we reach here
  log->commit(NULL);
  RCU::rcu_exit();

  // change state
  volatile_write(xc->state, TXN_CMMTD);

  // post-commit: stuff access stamps for reads; init new versions
  for (it = access_set.begin(); it != it_end; ++it) {
    dbtuple *tuple = it->second.get_tuple();
    if (it->second.is_write()) {
      if (tuple->prev) {    // could be an insert...
        volatile_write(tuple->prev->slsn, xc->lo.to_log_ptr());
        // wait for the older reader to finish pre-commit
        // FIXME: well, need a list of readers here...
      }
      //tuple->clsn = clsn.to_log_ptr();
      //tuple->xlsn = clsn;
    }
    else {
      fat_ptr tuple_slsn = volatile_read(tuple->slsn);
      if (tuple_slsn.asi_type() == fat_ptr::ASI_XID) {
        xid_context *overwriter_xc = xid_get_context(XID::from_ptr(tuple_slsn));
        // wait for the older overwriter (smaller clsn) to finish pre-commit
        // (can't allow a newer reader to update xlsn before an older overwriter
        // finishes precommit, otherwise xlsn (\pi) will always be larger than
        // the overwriter's clsn (its \eta too) => will always abort the overwriter.
        if (overwriter_xc->end != INVALID_LSN and overwriter_xc->end < clsn)
          while (overwriter_xc->state != TXN_CMMTD);
      }
      if (volatile_read(tuple->xlsn) < clsn)
        volatile_write(tuple->xlsn._val, clsn._val);
    }
  }

  // done
  xid_free(xid);
}
#else
template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::si_commit()
{
  xid_context* xc = xid_get_context(xid);

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
  
  // avoid cross init after goto do_abort
  typename access_set_map::iterator it     = access_set.begin();
  typename access_set_map::iterator it_end = access_set.end();

  INVARIANT(log);
  // get clsn, abort if failed
  RCU::rcu_enter();
  xc->end = log->pre_commit();
  if (xc->end == INVALID_LSN)
      signal_abort(ABORT_REASON_INTERNAL);
  log->commit(NULL);
  RCU::rcu_exit();

  // change state
  volatile_write(xid_get_context(xid)->state, TXN_CMMTD);

  // post-commit cleanup: install clsn to tuples
  // (traverse write-tuple)
  // stuff clsn in tuples in write-set
  for (; it != it_end; ++it) {
    dbtuple* tuple = it->second.get_tuple();
    if (it->second.is_write()) {
      tuple->clsn = xc->end.to_log_ptr();
      INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
    }
  }

  // done
  xid_free(xid);
}
#endif
#endif

typedef object_vector<typename concurrent_btree::value_type> tuple_vector_type;
// FIXME: tzwang: note: we only try once in this function. If it
// failed (returns false) then the caller (supposedly do_tree_put)
// should fallback to normal update procedure.
template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::try_insert_new_tuple(
    concurrent_btree &btr,
    const std::string *key,
	object* value,
    dbtuple::tuple_writer_t writer)
{
  INVARIANT(key);
  char*p = (char*)value;
  dbtuple* tuple = reinterpret_cast<dbtuple*>(p + sizeof(object));
  tuple_vector_type* tuple_vector = btr.get_tuple_vector();
  tuple->oid = tuple_vector->alloc();
  fat_ptr new_head = fat_ptr::make( value, INVALID_SIZE_CODE, 0);
  while(!tuple_vector->put( tuple->oid, new_head));

  // XXX: underlying btree api should return the existing value if insert
  // fails- this would allow us to avoid having to do another search
  // FIXME: tzwang: didn't look like so, returns nullptr. bug?
  typename concurrent_btree::insert_info_t insert_info;
  if (unlikely(!btr.insert_if_absent(
          varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
    VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);
    ++transaction_base::g_evt_dbtuple_write_insert_failed;
    return false;
  }
  VERBOSE(std::cerr << "insert_if_absent suceeded for key: " << util::hexify(key) << std::endl
                    << "  new dbtuple is " << util::hexify(tuple) << std::endl);

  // insert to log
  // FIXME: tzwang: leave pdest as null and FID is always 1 now.
  INVARIANT(log);
  auto record_size = align_up((size_t)tuple->size);
  auto size_code = encode_size_aligned(record_size);
  log->log_insert(1,
                  tuple->oid,
                  fat_ptr::make(tuple, size_code),
                  DEFAULT_ALIGNMENT_BITS, NULL);
  // update access_set
  access_set.emplace(access_set_key(&btr, tuple->oid), access_record_t(tuple, true));
  return true;
}

template <template <typename> class Protocol, typename Traits>
template <typename ValueReader>
bool
transaction<Protocol, Traits>::do_tuple_read(
    concurrent_btree *btr_ptr, dbtuple *tuple, ValueReader &value_reader)
{
  INVARIANT(tuple);
  ++evt_local_search_lookups;

  // do the actual tuple read
  dbtuple::ReadStatus stat;
  {
    PERF_DECL(static std::string probe0_name(std::string(__PRETTY_FUNCTION__) + std::string(":do_read:")));
    ANON_REGION(probe0_name.c_str(), &private_::txn_btree_search_probe0_cg);
    tuple->prefetch();
    stat = tuple->stable_read(value_reader, this->string_allocator());
    if (unlikely(stat == dbtuple::READ_FAILED)) {
      const transaction_base::abort_reason r = transaction_base::ABORT_REASON_UNSTABLE_READ;
      signal_abort(r);
    }
  }
  INVARIANT(stat == dbtuple::READ_EMPTY ||
            stat == dbtuple::READ_RECORD);
  const bool v_empty = (stat == dbtuple::READ_EMPTY);
  if (v_empty)
    ++transaction_base::g_evt_read_logical_deleted_node_search;

  // SSN stamps and check
  access_set_key askey(btr_ptr, tuple->oid);
  if (find_access_set(askey) == access_set.end() and tuple->clsn.asi_type() == fat_ptr::ASI_LOG) {
      xid_context* xc = xid_get_context(xid);
      ASSERT(xc);
      LSN v_clsn = LSN::from_ptr(tuple->clsn);
      // \eta - largest predecessor. So if I read this tuple, I should commit
      // after the tuple's creator (trivial, as this is committed version, so
      // this tuple's clsn can only be a predecessor of me): so just update
      // my \eta if needed.
      if (xc->hi < v_clsn)
          xc->hi = v_clsn;

      // Now if this tuple was overwritten by somebody, this means if I read
      // it, that overwriter will have anti-dependency on me (I must be
      // serialized before the overwriter), and it already committed (as a
      // successor of mine), so I need to update my \pi for the SSN check.
      // This is the easier case of anti-dependency (the other case is T1
      // already read a (then latest) version, then T2 comes to overwrite it).
      fat_ptr tuple_slsn = volatile_read(tuple->slsn);
      if (tuple_slsn.asi_type() == fat_ptr::ASI_LOG) {
        if (LSN::from_ptr(tuple_slsn) == INVALID_LSN)   // no overwrite so far
          access_set.emplace(askey, access_record_t(tuple, false));
        else if (xc->lo > LSN::from_ptr(tuple_slsn))
          xc->lo = LSN::from_ptr(tuple_slsn); // \pi
      }
      if (not ssn_check_exclusion(xc))
        signal_abort(ABORT_REASON_SSN_EXCLUSION_FAILURE);
  }

  return !v_empty;
}

#endif /* _NDB_TXN_IMPL_H_ */
