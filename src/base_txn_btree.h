#ifndef _NDB_BASE_TXN_BTREE_H_
#define _NDB_BASE_TXN_BTREE_H_

#include "btree_choice.h"
#include "txn.h"
#include "lockguard.h"
#include "util.h"
#include "ndb_type_traits.h"

#include <string>
#include <map>
#include <type_traits>
#include <memory>

using namespace TXN;

template <template <typename> class Transaction, typename P>
class base_txn_btree {
public:

  typedef transaction_base::size_type size_type;
  typedef transaction_base::string_type string_type;
  typedef concurrent_btree::string_type keystring_type;

  base_txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : value_size_hint(value_size_hint),
      name(name),
      been_destructed(false)
  {
#ifdef TRACE_FOOTPRINT
    TRACER::register_table((uintptr_t)underlying_btree.get_tuple_vector(), name);
#endif
  }

  ~base_txn_btree()
  {
    if (!been_destructed)
      unsafe_purge(false);
  }

  inline size_t
  size_estimate() const
  {
    return underlying_btree.size();
  }

  inline size_type
  get_value_size_hint() const
  {
    return value_size_hint;
  }

  inline void
  set_value_size_hint(size_type value_size_hint)
  {
    this->value_size_hint = value_size_hint;
  }

  inline void print() {
    underlying_btree.print();
  }

  /**
   * only call when you are sure there are no concurrent modifications on the
   * tree. is neither threadsafe nor transactional
   *
   * Note that when you call unsafe_purge(), this txn_btree becomes
   * completely invalidated and un-usable. Any further operations
   * (other than calling the destructor) are undefined
   */
  std::map<std::string, uint64_t> unsafe_purge(bool dump_stats = false);

private:

  struct purge_tree_walker : public concurrent_btree::tree_walk_callback {
    virtual void on_node_begin(const typename concurrent_btree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();

  private:
    std::vector< std::pair<typename concurrent_btree::value_type, bool> > spec_values;
  };

protected:

  // readers are placed here so they can be shared amongst
  // derived implementations

  template <typename Traits, typename Callback,
            typename KeyReader, typename ValueReader>
  struct txn_search_range_callback : public concurrent_btree::low_level_search_range_callback {
    constexpr txn_search_range_callback(
          Transaction<Traits> *t,
          Callback *caller_callback,
          KeyReader *key_reader,
          ValueReader *value_reader)
      : t(t), caller_callback(caller_callback),
        key_reader(key_reader), value_reader(value_reader) {}

    virtual void on_resp_node(const typename concurrent_btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const concurrent_btree *btr_ptr,
                        const typename concurrent_btree::string_type &k, oid_type o, dbtuple* v,
                        const typename concurrent_btree::node_opaque_t *n, uint64_t version);

  private:
    Transaction<Traits> *const t;
    Callback *const caller_callback;
    KeyReader *const key_reader;
    ValueReader *const value_reader;
  };

  template <typename Traits, typename ValueReader>
  inline rc_t
  do_search(Transaction<Traits> &t,
            const typename P::Key &k,
            ValueReader &value_reader);

  template <typename Traits, typename Callback,
            typename KeyReader, typename ValueReader>
  inline void
  do_search_range_call(Transaction<Traits> &t,
                       const typename P::Key &lower,
                       const typename P::Key *upper,
                       Callback &callback,
                       KeyReader &key_reader,
                       ValueReader &value_reader);

  template <typename Traits, typename Callback,
            typename KeyReader, typename ValueReader>
  inline void
  do_rsearch_range_call(Transaction<Traits> &t,
                        const typename P::Key &upper,
                        const typename P::Key *lower,
                        Callback &callback,
                        KeyReader &key_reader,
                        ValueReader &value_reader);

  // expect_new indicates if we expect the record to not exist in the tree-
  // is just a hint that affects perf, not correctness. remove is put with nullptr
  // as value.
  //
  // NOTE: both key and value are expected to be stable values already
  template <typename Traits>
  rc_t do_tree_put(Transaction<Traits> &t,
                   const varstr *k,
                   const varstr *v,
                   dbtuple::tuple_writer_t writer,
                   bool expect_new);

  concurrent_btree underlying_btree;
  size_type value_size_hint;
  std::string name;
  bool been_destructed;
};

namespace private_ {
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe0, txn_btree_search_probe0_cg)
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe1, txn_btree_search_probe1_cg)
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename ValueReader>
rc_t
base_txn_btree<Transaction, P>::do_search(
    Transaction<Traits> &t,
    const typename P::Key &k,
    ValueReader &value_reader)
{
  t.ensure_active();

  typename P::KeyWriter key_writer(&k);
  const varstr * const key_str =
    key_writer.fully_materialize(true, t.string_allocator());

  // search the underlying btree to map k=>(btree_node|tuple)
  dbtuple * tuple{};
  oid_type oid;
  concurrent_btree::versioned_node_t search_info;
  const bool found = this->underlying_btree.search(varkey(key_str), oid, tuple, t.xc, &search_info);
  if (found)
    return t.do_tuple_read(tuple, value_reader);
#ifdef PHANTOM_PROT_NODE_SET
  else {
    rc_t rc = t.do_node_read(search_info.first, search_info.second);
    if (rc_is_abort(rc))
      return rc;
  }
#endif
  return rc_t{RC_FALSE};
}

template <template <typename> class Transaction, typename P>
std::map<std::string, uint64_t>
base_txn_btree<Transaction, P>::unsafe_purge(bool dump_stats)
{
  ALWAYS_ASSERT(!been_destructed);
  been_destructed = true;
  purge_tree_walker w;
  underlying_btree.tree_walk(w);
  underlying_btree.clear();
  return std::map<std::string, uint64_t>();
}

template <template <typename> class Transaction, typename P>
void
base_txn_btree<Transaction, P>::purge_tree_walker::on_node_begin(const typename concurrent_btree::node_opaque_t *n)
{
  INVARIANT(spec_values.empty());
  spec_values = concurrent_btree::ExtractValues(n);
}

template <template <typename> class Transaction, typename P>
void
base_txn_btree<Transaction, P>::purge_tree_walker::on_node_success()
{
  spec_values.clear();
}

template <template <typename> class Transaction, typename P>
void
base_txn_btree<Transaction, P>::purge_tree_walker::on_node_failure()
{
  spec_values.clear();
}

template <template <typename> class Transaction, typename P>
template <typename Traits>
rc_t base_txn_btree<Transaction, P>::do_tree_put(
    Transaction<Traits> &t,
    const varstr *k,
    const varstr *v,
    dbtuple::tuple_writer_t writer,
    bool expect_new)
{
  INVARIANT(k);
  INVARIANT(!expect_new || v); // makes little sense to remove() a key you expect
                               // to not be present, so we assert this doesn't happen
                               // for now [since this would indicate a suboptimality]
  t.ensure_active();

  // Calculate Tuple Size
  const size_t sz =
    v ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED, v, nullptr, 0) : 0;
  size_t alloc_sz = sizeof(dbtuple) + sizeof(object) +  align_up(sz);

  // Allocate a version
  object *obj = NULL;
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
  obj = t.op->get(alloc_sz);
  if (not obj)
#endif
    obj = new (MM::allocate(alloc_sz)) object(alloc_sz);

  // Tuple setup
  dbtuple* tuple = (dbtuple *)obj->payload();
  tuple = dbtuple::init((char*)tuple, sz);
  tuple->pvalue = (varstr *)v;

  // initialize the version
  tuple->clsn = t.xid.to_ptr();		// XID state is set

  // FIXME: tzwang: try_insert_new_tuple only tries once (no waiting, just one cas),
  // it fails if somebody else acted faster to insert new, we then
  // (fall back to) with the normal update procedure.
  // try_insert_new_tuple should add tuple to write-set too, if succeeded.
  if (expect_new and t.try_insert_new_tuple(&this->underlying_btree, k, v, obj, writer))
    return rc_t{RC_TRUE};

  // do regular search
  dbtuple * bv = 0;
  oid_type oid = 0;
  if (!this->underlying_btree.search(varkey(k), oid, bv, t.xc))
    return rc_t{RC_ABORT_INTERNAL};
#ifdef PHANTOM_PROT_TABLE_LOCK
  // for delete
  bool instant_lock = false;
  table_lock_t *l = NULL;
  if (not v) {
    l = this->underlying_btree.get_tuple_vector()->lock_ptr();
    typename transaction<Transaction, Traits>::table_lock_set_t::iterator it =
      std::find(t.table_locks.begin(), t.table_locks.end(), l);
    if (it == t.table_locks.end()) {
      if (not object_vector::lock(l, TABLE_LOCK_X))
        return rc_t{RC_ABORT_PHANTOM};
      instant_lock = true;
    }
    else {
      if (not object_vector::upgrade_lock(l))
        return rc_t{RC_ABORT_PHANTOM};
    }
    ASSERT((volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_X or
           (volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX);
  }
#endif

  // After read the latest committed, and holding the version:
  // if the latest tuple in the chained is dirty then abort
  // else
  //   if the latest tuple in the chain is clean but latter than my ts then abort
  // else
  //   if CAS my tuple to the head failed (means sb. else acted faster) then abort
  // else
  //   succeeded!
  //
  // The above is hidden in the APIs provided by abstract tree and object.h.
  // Here we just call the provided update_tulpe function which returns the
  // result (either succeeded or failed, i.e., need to abort).


  dbtuple *prev = this->underlying_btree.update_version(oid, obj, t.xc);

  if (prev) { // succeeded
    ASSERT(t.xc);
#ifdef USE_PARALLEL_SSI
    // if I clobberred an old tuple, then as the T2 I have to assume
    // a committed T3 exists for me; so set it to its minimum.
    if (prev->is_old(t.xc))
      volatile_write(t.xc->ct3, 1);
    // check if there's any in-flight readers of the overwritten tuple
    // (will form an inbound r:w edge to me) ie, am I the T2 (pivot)
    // with T1 in-flight and T3 committed first (ie, before T1, ie,
    // prev's creator) in the dangerous structure?
    ASSERT(prev->sstamp == NULL_PTR);
    // the read-opt makes the readers list inaccurate, so we only do
    // the check here if read-opt is not enabled
    if (not has_read_opt()) {
      if (t.xc->ct3 and serial_get_tuple_readers(prev, true)) {
        // unlink the version here (note abort_impl won't be able to catch
        // it because it's not yet in the write set), same as in SSN impl.
        this->underlying_btree.unlink_tuple(oid, tuple);
#ifdef PHANTOM_PROT_TABLE_LOCK
        if (instant_lock)
          object_vector::unlock(l);
#endif
        return rc_t{RC_ABORT_SERIAL};
      }
    }
#endif
    object *prev_obj = (object *)((char *)prev - sizeof(object));
#ifdef USE_PARALLEL_SSN
    // update hi watermark
    // Overwriting a version could trigger outbound anti-dep,
    // i.e., I'll depend on some tx who has read the version that's
    // being overwritten by me. So I'll need to see the version's
    // access stamp to tell if the read happened.
    ASSERT(prev->sstamp == NULL_PTR);
    auto prev_xstamp = volatile_read(prev->xstamp);
    if (t.xc->pstamp < prev_xstamp)
      t.xc->pstamp = prev_xstamp;

#ifdef DO_EARLY_SSN_CHECKS
    if (not ssn_check_exclusion(t.xc)) {
      // unlink the version here (note abort_impl won't be able to catch
      // it because it's not yet in the write set)
      this->underlying_btree.unlink_tuple(oid, tuple);
#ifdef PHANTOM_PROT_TABLE_LOCK
      if (instant_lock)
        object_vector::unlock(l);
#endif
      return rc_t{RC_ABORT_SERIAL};
    }
#endif

    // copy access stamp to new tuple from overwritten version
    // (no need to copy sucessor lsn (slsn))
    volatile_write(tuple->xstamp, prev->xstamp);
#endif

    // read prev's clsn first, in case it's a committing XID, the clsn's state
    // might change to ASI_LOG anytime
    fat_ptr prev_clsn = volatile_read(prev->clsn);
    if (prev_clsn.asi_type() == fat_ptr::ASI_XID and XID::from_ptr(prev_clsn) == t.xid) {  // in-place update!
      volatile_write(obj->_next._ptr, prev_obj->_next._ptr); // prev's prev: previous *committed* version
      prev->mark_defunct();
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
      t.op->put(t.epoch, prev_obj);
#endif
      ASSERT(obj->_next.offset() != (uintptr_t)prev_obj);
    }
    else {  // prev is committed (or precommitted but in post-commit now) head
      volatile_write(obj->_next, fat_ptr::make(prev_obj, 0));
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
      volatile_write(prev->sstamp, t.xc->owner.to_ptr());
#endif
    }

    t.write_set.emplace_back(tuple, writer, &this->underlying_btree, oid);
    ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_XID);
    ASSERT((dbtuple *)this->underlying_btree.fetch_version(oid, t.xc) == tuple);

#ifdef PHANTOM_PROT_TABLE_LOCK
      if (instant_lock)
        object_vector::unlock(l);
#endif

    INVARIANT(t.log);
    // FIXME: tzwang: so we insert log here, assuming the logmgr only assigning
    // pointers, instead of doing memcpy here (looks like this is the case unless
    // the record is tooooo large).
    // for simplicity and alignment, we write the whole varstr
    // (because varstr's data must be the last field, which has a size
    // field (size_t) before it; putting data before size will make it hard to
    // get the data field 16-byte alignment, although varstr* itself is aligned
    // by posix_memalign).
    //
    // FIXME: combine the size field in dbtuple and varstr.
    ASSERT((not sz and not v) or (sz and v and sz == v->size()));
    auto record_size = align_up(sz + v ? sizeof(varstr) : 0);
    auto size_code = encode_size_aligned(record_size);
    ASSERT(not ((uint64_t)v & ((uint64_t)0xf)));
    t.log->log_update(1,
                      oid,
                      fat_ptr::make((void *)v, size_code),
                      DEFAULT_ALIGNMENT_BITS,
                      NULL);
    return rc_t{RC_TRUE};
  }
  else {  // somebody else acted faster than we did
#ifdef PHANTOM_PROT_TABLE_LOCK
    if (instant_lock)
      object_vector::unlock(l);
#endif
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
void
base_txn_btree<Transaction, P>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader>
  ::on_resp_node(
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
               << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << concurrent_btree::NodeStringify(n) << std::endl);
#ifdef PHANTOM_PROT_NODE_SET
  rc_t rc = t->do_node_read(n, version);
  if (rc_is_abort(rc))
      caller_callback->return_code = rc;
#endif
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
bool
base_txn_btree<Transaction, P>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader>
  ::invoke(
    const concurrent_btree *btr_ptr,
    const typename concurrent_btree::string_type &k, oid_type o, dbtuple *v,
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x" << util::hexify(n)
                    << ", version=" << version << ">" << std::endl
                    << "  " << *((dbtuple *) v) << std::endl);
  caller_callback->return_code = t->do_tuple_read(v, *value_reader);
  if (caller_callback->return_code._val == RC_TRUE)
    return caller_callback->invoke((*key_reader)(k), value_reader->results());
  else if (rc_is_abort(caller_callback->return_code))
    return false;   // don't continue the read if the tx should abort
                    // ^^^^^ note: see masstree_scan.hh, whose scan() calls
                    // visit_value(), which calls this function to determine
                    // if it should stop reading.
  return true;
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
void
base_txn_btree<Transaction, P>::do_search_range_call(
    Transaction<Traits> &t,
    const typename P::Key &lower,
    const typename P::Key *upper,
    Callback &callback,
    KeyReader &key_reader,
    ValueReader &value_reader)
{
  t.ensure_active();
  if (upper)
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                 << ")::search_range_call [" << util::hexify(lower)
                 << ", " << util::hexify(*upper) << ")" << std::endl);
  else
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                 << ")::search_range_call [" << util::hexify(lower)
                 << ", +inf)" << std::endl);

  typename P::KeyWriter lower_key_writer(&lower);
  const varstr * const lower_str =
    lower_key_writer.fully_materialize(true, t.string_allocator());

  typename P::KeyWriter upper_key_writer(upper);
  const varstr * const upper_str =
    upper_key_writer.fully_materialize(true, t.string_allocator());

  if (unlikely(upper_str && *upper_str <= *lower_str))
    return;

#ifdef PHANTOM_PROT_TABLE_LOCK
  table_lock_t *l = this->underlying_btree.get_tuple_vector()->lock_ptr();
  if (std::find(t.table_locks.begin(), t.table_locks.end(), l) == t.table_locks.end()) {
    if (object_vector::lock(l, TABLE_LOCK_S))
      t.table_locks.push_back(l);
    else {
      callback.return_code = rc_t{RC_ABORT_PHANTOM};
      return;
    }
  }
  else {
    ASSERT((volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_S or
           (volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX);
  }
#endif

  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader> c(
			&t, &callback, &key_reader, &value_reader);

  varkey uppervk;
  if (upper_str)
    uppervk = varkey(upper_str);
  this->underlying_btree.search_range_call(
      varkey(lower_str), upper_str ? &uppervk : nullptr,
      c, t.xc);
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
void
base_txn_btree<Transaction, P>::do_rsearch_range_call(
    Transaction<Traits> &t,
    const typename P::Key &upper,
    const typename P::Key *lower,
    Callback &callback,
    KeyReader &key_reader,
    ValueReader &value_reader)
{
  t.ensure_active();

  typename P::KeyWriter lower_key_writer(lower);
  const varstr * const lower_str =
    lower_key_writer.fully_materialize(true, t.string_allocator());

  typename P::KeyWriter upper_key_writer(&upper);
  const varstr * const upper_str =
    upper_key_writer.fully_materialize(true, t.string_allocator());

  if (unlikely(lower_str && *upper_str <= *lower_str))
    return;

#ifdef PHANTOM_PROT_TABLE_LOCK
  table_lock_t *l = this->underlying_btree.get_tuple_vector()->lock_ptr();
  if (std::find(t.table_locks.begin(), t.table_locks.end(), l) == t.table_locks.end()) {
    if (object_vector::lock(l, TABLE_LOCK_S))
      t.table_locks.push_back(l);
    else {
      callback.return_code = rc_t{RC_ABORT_PHANTOM};
      return;
    }
  }
  else {
    ASSERT((volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_S or
           (volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX);
  }
#endif

  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader> c(
			&t, &callback, &key_reader, &value_reader);

  varkey lowervk;
  if (lower_str)
    lowervk = varkey(lower_str);
  this->underlying_btree.rsearch_range_call(
      varkey(upper_str), lower_str ? &lowervk : nullptr,
      c,t.xc);
}

#endif /* _NDB_BASE_TXN_BTREE_H_ */
