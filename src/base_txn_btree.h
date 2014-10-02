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

// each Transaction implementation should specialize this for special
// behavior- the default implementation is just nops
template <template <typename> class Transaction>
struct base_txn_btree_handler {
  static inline void on_construct() {} // called when initializing
  static const bool has_background_task = false;
};

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
    base_txn_btree_handler<Transaction>::on_construct();
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
    virtual bool invoke(const typename concurrent_btree::string_type &k, typename concurrent_btree::value_type v,
                        const typename concurrent_btree::node_opaque_t *n, uint64_t version);

  private:
    Transaction<Traits> *const t;
    Callback *const caller_callback;
    KeyReader *const key_reader;
    ValueReader *const value_reader;
  };

  template <typename Traits, typename ValueReader>
  inline bool
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
  void do_tree_put(Transaction<Traits> &t,
                   const std::string *k,
                   const typename P::Value *v,
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
bool
base_txn_btree<Transaction, P>::do_search(
    Transaction<Traits> &t,
    const typename P::Key &k,
    ValueReader &value_reader)
{
  t.ensure_active();

  typename P::KeyWriter key_writer(&k);
  const std::string * const key_str =
    key_writer.fully_materialize(true, t.string_allocator());

  // search the underlying btree to map k=>(btree_node|tuple)
  typename concurrent_btree::value_type underlying_v{};
  concurrent_btree::versioned_node_t search_info;
  // FIXME: tzwang: so this search function (or the lowest responsible function) should be aware
  // of the versions.
  const bool found = this->underlying_btree.search(varkey(*key_str), underlying_v, t.xid, &search_info);
  if (found) {
    const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(underlying_v);
    return t.do_tuple_read(tuple, value_reader);
  } else {
    // FIXME: tzwang: what's this?
    // not found, add to absent_set
    t.do_node_read(search_info.first, search_info.second);
    return false;
  }
}

template <template <typename> class Transaction, typename P>
std::map<std::string, uint64_t>
base_txn_btree<Transaction, P>::unsafe_purge(bool dump_stats)
{
  ALWAYS_ASSERT(!been_destructed);
  been_destructed = true;
  purge_tree_walker w;
  scoped_rcu_region guard;
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
  for (size_t i = 0; i < spec_values.size(); i++) {
    dbtuple *tuple = (dbtuple *) spec_values[i].first;
    INVARIANT(tuple);
    if (base_txn_btree_handler<Transaction>::has_background_task) {
      if (!tuple->size == 0) {
        dbtuple::release(tuple);
      } else {
        // enqueued already to background gc by the writer of the delete
      }
    } else {
      // XXX: this path is probably not right
      dbtuple::release_no_rcu(tuple);
    }
  }
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
void base_txn_btree<Transaction, P>::do_tree_put(
    Transaction<Traits> &t,
    const std::string *k,
    const typename P::Value *v,
    dbtuple::tuple_writer_t writer,
    bool expect_new)
{
  INVARIANT(k);
  INVARIANT(!expect_new || v); // makes little sense to remove() a key you expect
                               // to not be present, so we assert this doesn't happen
                               // for now [since this would indicate a suboptimality]
  t.ensure_active();

  // FIXME: tzwang: try_insert_new_tuple only tries once (no waiting, just one cas),
  // it fails if somebody else acted faster to insert new, we then
  // (fall back to) with the normal update procedure.
  // Note: the original return value of try_insert_new_tuple is:
  // <tuple, should_abort>. Here we use it as <tuple, failed>.
  // try_insert_new_tuple should add tuple to write-set too, if succeeded.
  if (expect_new) {
    if (t.try_insert_new_tuple(this->underlying_btree, k, v, writer)) 
		return;
  }

  // do regular search
  typename concurrent_btree::value_type bv = 0;
  if (!this->underlying_btree.search(varkey(*k), bv, t.xid)) {
    // only version is uncommitted -> cannot overwrite it
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_VERSION_INTERFERENCE;
    t.signal_abort(r);
  }

  // create new version
  const size_t sz =
    v ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED, v, nullptr, 0) : 0;
  dbtuple * const tuple = dbtuple::alloc_first(sz);
  if (v)
    writer(dbtuple::TUPLE_WRITER_DO_WRITE, v, tuple->get_value_start(), 0);
  INVARIANT(tuple);


  // initialize the version
  tuple->clsn = t.xid.to_ptr();		// XID state is set
  tuple->oid = reinterpret_cast<dbtuple*>(bv)->oid;

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

  std::pair<bool, concurrent_btree::value_type> ret =
                          this->underlying_btree.update_version(tuple->oid,
                          reinterpret_cast<concurrent_btree::value_type>(tuple),
                          t.xid);

  // FIXME: tzwang: now the above update returns a pair:
  // <bool, dbtuple*>, bool indicates if the update op has succeeded or not.
  // dbtuple*'s value dependes on if the update is in-place or out-of-place.
  // Though we do multi-version, we do in-place update if the tx is repeatedly
  // updating its own data. If it's normal insert-to-chain, i.e., out-of-place
  // update, dbtuple* will be null. Otherwise it will be the older dirty tuple's
  // address. It's like this mainly because we might have different sizes for
  // the older and new dirty tuple. So in case of in-place update, we don't copy
  // data provided by the tx, but rather simply use the one provided by the tx;
  // then the older (obsolete) dirty tuple is returned here to get rcu_freed,
  // by traversing the write-set, which contains pointers to updated tuples.
  // So in summary, this requires a traversal of the write-set only if in-place
  // update happened, which we anticipate to be rare.
  //
  // One way to avoid this traversal is to store OIDs, rather than pointers to
  // new tuple versions in the write-set. Then we will be able to blindly add
  // the OID of the updated tuple no matter it's an in-place or out-of-place
  // update. During commit we just follow the OIDs to find the physical pointer
  // to tuples and write it to the log, becaues the OID table always has the
  // pointer to the latest updated version. But again, this might need to
  // traverse the OID table or at least some fancy index to build the OID table
  // (such as a hash table).
  //
  // For now we stick to the pointer (former) way because it's simple, and
  // silo's write-set infrastructure is already in that format, plus in-place
  // update should be very rare. This could be avoided too (with the cost of
  // checking a valid bit at commit time). See comments next.

  // check return value
  if (ret.first) { // succeeded
    INVARIANT(log);
    // FIXME: tzwang: so we insert log here, assuming the logmgr only assigning
    // pointers, instead of doing memcpy here (looks like this is the case unless
    // the record is tooooo large).
    auto record_size = align_up(sz);
    auto size_code = encode_size_aligned(record_size);
    t.log->log_update(1,
                      tuple->oid,
                      fat_ptr::make(tuple, size_code),
                      DEFAULT_ALIGNMENT_BITS,
                      NULL);
    dbtuple* ret_tuple = reinterpret_cast<dbtuple*>(ret.second);
    if (ret_tuple) {  // in-place update
      INVARIANT(ret_tuple != tuple);
      // Even simpler, if we have a valid bit in dbtuple header, we don't need
      // to traverse the write-set. Just mark it as invalid if in-place update
      // happened, because the write set just holds the pointer to tuples. During
      // commit we'll need to check this bit and rcu_free tuples in the write-set
      // with valid=false.
      ret_tuple->overwritten = true;
    } // else we're done.
  }
  else {  // somebody else acted faster than we did
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_VERSION_INTERFERENCE;
    t.signal_abort(r);
  }

  // put to write-set, done.
  t.write_set.emplace_back(tuple, &this->underlying_btree);
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
  t->do_node_read(n, version);
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
bool
base_txn_btree<Transaction, P>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader>
  ::invoke(
    const typename concurrent_btree::string_type &k, typename concurrent_btree::value_type v,
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x" << util::hexify(n)
                    << ", version=" << version << ">" << std::endl
                    << "  " << *((dbtuple *) v) << std::endl);
  const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(v);
  if (t->do_tuple_read(tuple, *value_reader))
    return caller_callback->invoke(
        (*key_reader)(k), value_reader->results());
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
  const std::string * const lower_str =
    lower_key_writer.fully_materialize(true, t.string_allocator());

  typename P::KeyWriter upper_key_writer(upper);
  const std::string * const upper_str =
    upper_key_writer.fully_materialize(true, t.string_allocator());

  if (unlikely(upper_str && *upper_str <= *lower_str))
    return;

  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader> c(
			&t, &callback, &key_reader, &value_reader);

  varkey uppervk;
  if (upper_str)
    uppervk = varkey(*upper_str);
  this->underlying_btree.search_range_call(
      varkey(*lower_str), upper_str ? &uppervk : nullptr,
      c, t.xid, t.string_allocator()());
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
  const std::string * const lower_str =
    lower_key_writer.fully_materialize(true, t.string_allocator());

  typename P::KeyWriter upper_key_writer(&upper);
  const std::string * const upper_str =
    upper_key_writer.fully_materialize(true, t.string_allocator());

  if (unlikely(lower_str && *upper_str <= *lower_str))
    return;

  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader> c(
			&t, &callback, &key_reader, &value_reader);

  varkey lowervk;
  if (lower_str)
    lowervk = varkey(*lower_str);
  this->underlying_btree.rsearch_range_call(
      varkey(*upper_str), lower_str ? &lowervk : nullptr,
      c,t.xid, t.string_allocator()());
}

#endif /* _NDB_BASE_TXN_BTREE_H_ */
