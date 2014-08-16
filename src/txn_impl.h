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
  xid_free(xid);
  const unsigned cur_depth = rcu_guard_->depth();
  rcu_guard_.destroy();
  if (cur_depth == 1) {
    INVARIANT(!RCU::rcu_is_active());
    cast()->on_post_rcu_region_completion();
  }
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::AssertAllNodeLocksReleased();
#endif
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::abort_impl(abort_reason reason)
{
  abort_trap(reason);
  switch (state()) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    break;
  case TXN_ABRTD:
    return;
  case TXN_CMMTD:
  case TXN_COMMITTING:
    throw transaction_unusable_exception();
  }
  xid_get_context(xid)->state = TXN_ABRTD;
  this->reason = reason;

  // on abort, we need to go over all insert nodes and
  // release the locks
  typename write_set_map::iterator it     = write_set.begin();
  typename write_set_map::iterator it_end = write_set.end();
  for (; it != it_end; ++it) {
    dbtuple * const tuple = it->get_tuple();
    if (it->is_insert()) {
      //INVARIANT(tuple->is_locked());
      this->cleanup_inserted_tuple_marker(tuple, it->get_key(), it->get_btree());
      //tuple->unlock();
    }
  }
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::cleanup_inserted_tuple_marker(
    dbtuple *marker, const std::string &key, concurrent_btree *btr)
{
  // XXX: this code should really live in txn_proto2_impl.h
  //INVARIANT(marker->version == dbtuple::MAX_TID);
  //INVARIANT(marker->is_locked());
  //INVARIANT(marker->is_lock_owner());
  typename concurrent_btree::value_type removed = 0;
  const bool did_remove = btr->remove(varkey(key), &removed);
  if (unlikely(!did_remove)) {
#ifdef CHECK_INVARIANTS
    std::cerr << " *** could not remove key: " << util::hexify(key)  << std::endl;
#ifdef TUPLE_CHECK_KEY
    std::cerr << " *** original key        : " << util::hexify(marker->key) << std::endl;
#endif
#endif
    ALWAYS_ASSERT(false);
  }
  INVARIANT(removed == (typename concurrent_btree::value_type) marker);
  //INVARIANT(marker->is_latest());
  //marker->clear_latest();
  //dbtuple::release(marker); // rcu free
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

  std::cerr << "      === Read Set ===" << std::endl;
  // read-set
  for (typename read_set_map::const_iterator rs_it = read_set.begin();
       rs_it != read_set.end(); ++rs_it)
    std::cerr << *rs_it << std::endl;

  std::cerr << "      === Write Set ===" << std::endl;
  // write-set
  for (typename write_set_map::const_iterator ws_it = write_set.begin();
       ws_it != write_set.end(); ++ws_it)
    std::cerr << *ws_it << std::endl;

  std::cerr << "      === Absent Set ===" << std::endl;
  // absent-set
  for (typename absent_set_map::const_iterator as_it = absent_set.begin();
       as_it != absent_set.end(); ++as_it)
    std::cerr << "      B-tree Node " << util::hexify(as_it->first)
              << " : " << as_it->second << std::endl;

}

template <template <typename> class Protocol, typename Traits>
std::map<std::string, uint64_t>
transaction<Protocol, Traits>::get_txn_counters() const
{
  std::map<std::string, uint64_t> ret;

  // max_read_set_size
  ret["read_set_size"] = read_set.size();;
  ret["read_set_is_large?"] = !read_set.is_small_type();

  // max_absent_set_size
  ret["absent_set_size"] = absent_set.size();
  ret["absent_set_is_large?"] = !absent_set.is_small_type();

  // max_write_set_size
  ret["write_set_size"] = write_set.size();
  ret["write_set_is_large?"] = !write_set.is_small_type();

  return ret;
}

template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::commit(bool doThrow)
{
  return true;
// FIXME: tzwang: basically needs re-write

#ifdef TUPLE_MAGIC
      try {
#endif

      PERF_DECL(
          static std::string probe0_name(
            std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
      ANON_REGION(probe0_name.c_str(), &transaction_base::g_txn_commit_probe0_cg);

      switch (state()) {
      case TXN_EMBRYO:
      case TXN_ACTIVE:
        xid_get_context(xid)->state = TXN_COMMITTING;
        break;
      case TXN_CMMTD:
      case TXN_COMMITTING:  // FIXME: tzwang: correct?
        INVARIANT(false);
      case TXN_ABRTD:
        if (doThrow)
          throw transaction_abort_exception(reason);
        return false;
      }

      // get clsn and tx_log, abort if failed
      //bool failed = get_XXX;
      //if (failed)
      //  goto do_abort;

      // change state
      xid_get_context(xid)->state = TXN_CMMTD;

      // install clsn to tuples
      // (traverse write-tuple)

      // done
      xid_free(xid);

      return true;

    //do_abort:
      // XXX: these values are possibly un-initialized
      VERBOSE(std::cerr << "aborting txn" << std::endl);
      xid_get_context(xid)->state = TXN_ABRTD;

      // rcu-free write-set, set clsn in tuples to invalid_lsn

      if (doThrow)
        throw transaction_abort_exception(reason);
      return false;

#ifdef TUPLE_MAGIC
      } catch (dbtuple::magic_failed_exception &) {
        dump_debug_info();
        ALWAYS_ASSERT(false);
      }
#endif
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

  tuple->is_xid = true;
  tuple->v_.xid = this->xid;

  // XXX: underlying btree api should return the existing value if insert
  // fails- this would allow us to avoid having to do another search
  // FIXME: tzwang: didn't look like so, returns nullptr. bug?
  typename concurrent_btree::insert_info_t insert_info;
  // FIXME: tzwang: this is aware of object.h already?
  if (unlikely(!btr.insert_if_absent(
          varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
    VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);
    dbtuple::release_no_rcu(tuple);
    ++transaction_base::g_evt_dbtuple_write_insert_failed;
    return false;
  }
  VERBOSE(std::cerr << "insert_if_absent suceeded for key: " << util::hexify(key) << std::endl
                    << "  new dbtuple is " << util::hexify(tuple) << std::endl);
  // update write_set
  // FIXME: tzwang: so the caller shouldn't do this again if we returned true here.
  write_set.emplace_back(tuple, key, value, writer, &btr, true);

  // update node #s
  INVARIANT(insert_info.node);
  if (!absent_set.empty()) {
    auto it = absent_set.find(insert_info.node);
    if (it != absent_set.end()) {
      if (unlikely(it->second.version != insert_info.old_version)) {
        abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
        return true;
      }
      VERBOSE(std::cerr << "bump node=" << util::hexify(it->first) << " from v=" << insert_info.old_version
                        << " -> v=" << insert_info.new_version << std::endl);
      // otherwise, bump the version
      it->second.version = insert_info.new_version;
      SINGLE_THREADED_INVARIANT(concurrent_btree::ExtractVersionNumber(it->first) == it->second);
    }
  }
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
/*
  const bool is_snapshot_txn = is_snapshot();
  const transaction_base::tid_t snapshot_tid = is_snapshot_txn ?
    cast()->snapshot_tid() : static_cast<transaction_base::tid_t>(dbtuple::MAX_TID);
  transaction_base::tid_t start_t = 0;

  if (Traits::read_own_writes) {
    // this is why read_own_writes is not performant, because we have
    // to do linear scan
    auto write_set_it = find_write_set(const_cast<dbtuple *>(tuple));
    if (unlikely(write_set_it != write_set.end())) {
      ++evt_local_search_write_set_hits;
      if (!write_set_it->get_value())
        return false;
      const typename ValueReader::value_type * const px =
        reinterpret_cast<const typename ValueReader::value_type *>(
            write_set_it->get_value());
      value_reader.dup(*px, this->string_allocator());
      return true;
    }
  }
*/
  // do the actual tuple read
  dbtuple::ReadStatus stat;
  {
    PERF_DECL(static std::string probe0_name(std::string(__PRETTY_FUNCTION__) + std::string(":do_read:")));
    ANON_REGION(probe0_name.c_str(), &private_::txn_btree_search_probe0_cg);
    tuple->prefetch();
    stat = tuple->stable_read(value_reader, this->string_allocator());
    // FIXME: tzwang: bug? is_snapshot_txn is actually allow_write_intent in stable_read
    //stat = tuple->stable_read(snapshot_tid, start_t, value_reader, this->string_allocator(), is_snapshot_txn);
    // FIXME: tzwang: give better reason here, basically for not-visible
    if (unlikely(stat == dbtuple::READ_FAILED)) {
      const transaction_base::abort_reason r = transaction_base::ABORT_REASON_UNSTABLE_READ;
      abort_impl(r);
      throw transaction_abort_exception(r);
    }
  }
  /* FIXME: tzwang: handled above
  if (unlikely(!cast()->can_read_tid(start_t))) {
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_FUTURE_TID_READ;
    abort_impl(r);
    throw transaction_abort_exception(r);
  }
  */
  INVARIANT(stat == dbtuple::READ_EMPTY ||
            stat == dbtuple::READ_RECORD);
  const bool v_empty = (stat == dbtuple::READ_EMPTY);
  if (v_empty)
    ++transaction_base::g_evt_read_logical_deleted_node_search;
  //if (!is_snapshot_txn)
    // read-only txns do not need read-set tracking
    // (b/c we know the values are consistent)
  read_set.emplace_back(tuple);
  return !v_empty;
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::do_node_read(
    const typename concurrent_btree::node_opaque_t *n, uint64_t v)
{
  INVARIANT(n);
  if (is_snapshot())
    return;
  auto it = absent_set.find(n);
  if (it == absent_set.end()) {
    absent_set[n].version = v;
  } else if (it->second.version != v) {
    const transaction_base::abort_reason r =
      transaction_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
    abort_impl(r);
    throw transaction_abort_exception(r);
  }
}

#endif /* _NDB_TXN_IMPL_H_ */
