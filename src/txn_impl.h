#ifndef _NDB_TXN_IMPL_H_
#define _NDB_TXN_IMPL_H_

#include "txn.h"
#include "lockguard.h"

#ifdef HACK_SILO
#include "object.h"
#endif

using namespace TXN;

// base definitions

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::transaction(uint64_t flags, string_allocator_type &sa)
  : transaction_base(flags), sa(&sa)
{
  INVARIANT(RCU::rcu_is_active());
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::NodeLockRegionBegin();
#endif
  xid_context *xc = xid_get_context(xid);
  xc->begin = logger->cur_lsn();
  xc->end = INVALID_LSN;
  xc->state = TXN_EMBRYO;
}

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_EMBRYO, TXN_CMMTD, and TXN_ABRTD
  INVARIANT(state() != TXN_ACTIVE && state() != TXN_COMMITTING);
  INVARIANT(RCU::rcu_is_active());

  // FIXME: tzwang: free txn desc.
  const unsigned cur_depth = rcu_guard_->depth();
  rcu_guard_.destroy();
  if (cur_depth == 1)
    INVARIANT(!RCU::rcu_is_active());
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::AssertAllNodeLocksReleased();
#endif
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::signal_abort(abort_reason reason)
{
  abort_trap(reason);
  
  // atomic state change. after this, write-sets are invisible.
  volatile_write(xid_get_context(xid)->state, TXN_ABRTD);
  throw transaction_abort_exception(reason);
}
  
template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::abort_impl()
{
  INVARIANT(state() == TXN_ABRTD);
  
  // write-set uninstall
  //	1. unlink from version chain
  //	2. rcu-free the out-of-chain versions for rcu callback to reclaim them.
  typename write_set_map::iterator it     = write_set.begin();
  typename write_set_map::iterator it_end = write_set.end();
  for (; it != it_end; ++it) {
    dbtuple* tuple = it->get_tuple();
	concurrent_btree* btr = it->get_table();

	// TODO. update to INVALID LSN
	// INVALID_LSN -> FATPTR needed

	// overwritten tuple is already out of chain
	if( !tuple->overwritten )
		btr->unlink_tuple( tuple->oid, (typename concurrent_btree::value_type)tuple );

    dbtuple::release(tuple);
  }

  // log discard
  log->pre_commit();
  log->discard();

  // now. safe to free XID
  xid_free(xid);
  
  RCU::rcu_quiesce();

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
  std::cerr << "  Read/Write sets:" << std::endl;

  std::cerr << "      === Write Set ===" << std::endl;
  // write-set
  for (typename write_set_map::const_iterator ws_it = write_set.begin();
       ws_it != write_set.end(); ++ws_it)
    std::cerr << *ws_it << std::endl;

  std::cerr << "      === Absent Set ===" << std::endl;
}

template <template <typename> class Protocol, typename Traits>
std::map<std::string, uint64_t>
transaction<Protocol, Traits>::get_txn_counters() const
{
  std::map<std::string, uint64_t> ret;

  // max_write_set_size
  ret["write_set_size"] = write_set.size();
  ret["write_set_is_large?"] = !write_set.is_small_type();

  return ret;
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::commit()
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
    INVARIANT(false);
  }
  
  // avoid cross init after goto do_abort
  typename write_set_map::iterator it     = write_set.begin();
  typename write_set_map::iterator it_end = write_set.end();

  INVARIANT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit();
  if (xc->end == INVALID_LSN)
      signal_abort(ABORT_REASON_INTERNAL);

  log->commit(NULL);
  // change state
  volatile_write(xid_get_context(xid)->state, TXN_CMMTD);

  // post-commit cleanup: install clsn to tuples
  // (traverse write-tuple)
  // stuff clsn in tuples in write-set
  for (; it != it_end; ++it) {
    dbtuple* tuple = it->get_tuple();
    if (not tuple->overwritten) {
      tuple->clsn = xc->end.to_log_ptr();
      INVARIANT(tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
    } 
    else {
      // FIXME: tzwang: add this callback to adjust pointers in version chain
      dbtuple::release(tuple);
      //RCU::free_with_fn(tuple, tuple_remove_callback);
    }
  }

  RCU::rcu_quiesce();
  // done
  xid_free(xid);
}

// FIXME: tzwang: note: we only try once in this function. If it
// failed (returns false) then the caller (supposedly do_tree_put)
// should fallback to normal update procedure.
template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::try_insert_new_tuple(
    concurrent_btree &btr,
    const std::string *key,
    const void *value,
    dbtuple::tuple_writer_t writer)
{
  INVARIANT(key);
  const size_t sz =
    value ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED,
      value, nullptr, 0) : 0;

  // perf: ~900 tsc/alloc on istc11.csail.mit.edu
  dbtuple * const tuple = dbtuple::alloc_first(sz);
  if (value)
    writer(dbtuple::TUPLE_WRITER_DO_WRITE,
        value, tuple->get_value_start(), 0);

  tuple->clsn = this->xid.to_ptr();

  // insert to log
  // FIXME: tzwang: leave pdest as null and FID is always 1 now.
  INVARIANT(log);
  auto record_size = align_up(sz);
  auto size_code = encode_size_aligned(record_size);
  log->log_insert(1,
                  tuple->oid,
                  fat_ptr::make(tuple, size_code),
                  DEFAULT_ALIGNMENT_BITS, NULL);
  // update write_set
  // FIXME: tzwang: so the caller shouldn't do this again if we returned true here.
  //write_set.emplace_back(tuple, key, value, writer, &btr, true);
  write_set.emplace_back(tuple, &btr);

  return true;
}

template <template <typename> class Protocol, typename Traits>
template <typename ValueReader>
bool
transaction<Protocol, Traits>::do_tuple_read(
    const dbtuple *tuple, ValueReader &value_reader)
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
    // FIXME: tzwang: give better reason here, basically for not-visible
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
  return !v_empty;
}

#endif /* _NDB_TXN_IMPL_H_ */
