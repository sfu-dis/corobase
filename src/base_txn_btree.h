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
  //static const bool has_background_task = false;
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
	//RA::register_table(&underlying_btree, name);		// Register to GC system 
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
  dbtuple * tuple{};
  oid_type oid;
  concurrent_btree::versioned_node_t search_info;
  const bool found = this->underlying_btree.search(varkey(*key_str), oid, tuple, t.xid, &search_info);
  if (found) {
    return t.do_tuple_read(&this->underlying_btree, oid, tuple, value_reader);
  } else {
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

  // Prepare new tuple

  // Calculate Tuple Size
  typedef uint16_t node_size_type;
  const size_t sz =
    v ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED, v, nullptr, 0) : 0;

  INVARIANT(sz <= std::numeric_limits<node_size_type>::max());
  const size_t max_alloc_sz =
	  std::numeric_limits<node_size_type>::max() + sizeof(dbtuple) + sizeof(object);
  const size_t alloc_sz =
	  std::min(
			  util::round_up<size_t, allocator::LgAllocAlignment>(sizeof(dbtuple) + sizeof(object) + sz),
			  max_alloc_sz);
  INVARIANT((alloc_sz - sizeof(dbtuple) - sizeof(object)) >= sz);

  // Allocate a version
  char *p = NULL;
  p = reinterpret_cast<char*>(RA::allocate(alloc_sz));
  INVARIANT(p);

  // Tuple setup
  dbtuple* tuple = reinterpret_cast<dbtuple*>(p + sizeof(object));
  tuple = dbtuple::init((char*)tuple, sz);
  if (v)
    writer(dbtuple::TUPLE_WRITER_DO_WRITE,
        v, tuple->get_value_start(), 0);

  // initialize the version
  tuple->clsn = t.xid.to_ptr();		// XID state is set

  // Create object
  object* version = new (p) object( sizeof(object) + alloc_sz );

  // FIXME: tzwang: try_insert_new_tuple only tries once (no waiting, just one cas),
  // it fails if somebody else acted faster to insert new, we then
  // (fall back to) with the normal update procedure.
  // try_insert_new_tuple should add tuple to write-set too, if succeeded.
  if (expect_new and t.try_insert_new_tuple(&this->underlying_btree, k, version, writer))
		return;

  // do regular search
  dbtuple * bv = 0;
  oid_type oid = 0;
  if (!this->underlying_btree.search(varkey(*k), oid, bv, t.xid)) {
    // only version is uncommitted -> cannot overwrite it
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_VERSION_INTERFERENCE;
    t.signal_abort(r);
  }

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


  dbtuple *prev = this->underlying_btree.update_version(oid, version, t.xid);

  if (prev) { // succeeded
    object *prev_obj = (object *)((char *)prev - sizeof(object));
#ifdef USE_PARALLEL_SSN
    // update hi watermark
    // Overwriting a version could trigger outbound anti-dep,
    // i.e., I'll depend on some tx who has read the version that's
    // being overwritten by me. So I'll need to see the version's
    // access stamp to tell if the read happened.
    xid_context* xc = xid_get_context(t.xid);
    ASSERT(xc);
    auto prev_xstamp = volatile_read(prev->xstamp);
    if (xc->pstamp < prev_xstamp)
      xc->pstamp = prev_xstamp;

#ifdef DO_EARLY_SSN_CHECKS
    if (not ssn_check_exclusion(xc))
      t.signal_abort(transaction_base::ABORT_REASON_SSN_EXCLUSION_FAILURE);
#endif

    // copy access stamp to new tuple from overwritten version
    // (no need to copy sucessor lsn (slsn))
    volatile_write(tuple->xstamp, prev->xstamp);
#endif

    // use the committed version as key, if not, use the new version
    // this should cover the update of myself's insert too
    dbtuple *key_tuple = NULL;
    if (prev->clsn.asi_type() == fat_ptr::ASI_XID and XID::from_ptr(prev->clsn) == t.xid) {  // in-place update!
      volatile_write(version->_next._ptr, prev_obj->_next._ptr); // prev's prev: previous *committed* version
      if (not prev_obj->_next.offset()) {    // update of myself's insert
        ASSERT(t.write_set[prev].new_tuple == key_tuple and t.write_set[prev].btr == &this->underlying_btree);
        key_tuple = tuple;
        t.write_set[prev].btr = NULL;
      }
      else {
        key_tuple = (dbtuple *)((object *)prev_obj->_next.offset())->payload();
        ASSERT(key_tuple->clsn.asi_type() == fat_ptr::ASI_LOG);
        ASSERT(t.write_set[key_tuple].new_tuple == prev);
      }
      RA::deallocate(prev_obj);
      ASSERT(version->_next.offset() != (uintptr_t)prev_obj);
    }
    else {  // prev is committed head
      volatile_write(version->_next, fat_ptr::make(prev_obj, 0));
      key_tuple = prev;
    }

    t.write_set[key_tuple] = typename transaction<Transaction, Traits>::write_record_t(tuple, &this->underlying_btree, oid);
    ASSERT(t.write_set[key_tuple].new_tuple == tuple and t.write_set[key_tuple].btr == &this->underlying_btree);

    ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_XID);
    ASSERT((dbtuple *)this->underlying_btree.fetch_version(oid, t.xid) == tuple);

    INVARIANT(log);
    // FIXME: tzwang: so we insert log here, assuming the logmgr only assigning
    // pointers, instead of doing memcpy here (looks like this is the case unless
    // the record is tooooo large).
    auto record_size = align_up(sz);
    auto size_code = encode_size_aligned(record_size);
    t.log->log_update(1,
                      oid,
                      fat_ptr::make(tuple, size_code),
                      DEFAULT_ALIGNMENT_BITS,
                      NULL);
  }
  else {  // somebody else acted faster than we did
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_VERSION_INTERFERENCE;
    t.signal_abort(r);
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
  if (t->do_tuple_read(const_cast<concurrent_btree*>(btr_ptr), o, v, *value_reader))
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
