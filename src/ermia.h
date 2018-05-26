#pragma once

#include <map>

#include "base_txn_btree.h"
#include "spinbarrier.h"
#include "txn.h"
#include "util.h"

namespace ermia {

class Database {
public:
  Database() {}
  ~Database() {}

  inline transaction *new_txn(uint64_t txn_flags, str_arena &arena, transaction *buf) {
    new (buf) transaction(txn_flags, arena);
    return buf;
  }

  inline rc_t commit_txn(transaction *t) {
    rc_t rc = t->commit();
    if (not rc_is_abort(rc)) t->~transaction();
    return rc;
  }

  inline void abort_txn(transaction *t) {
    t->abort_impl();
    t->~transaction();
  }
};

// The user-facing index; for now represents a table
class OrderedIndex {
  friend class sm_log_recover_impl;
  friend class sm_chkpt_mgr;

 public:
  OrderedIndex(IndexDescriptor *id) : tree_(id), descriptor_(id) {}

  class scan_callback {
   public:
    ~scan_callback() {}
    // XXX(stephentu): key is passed as (const char *, size_t) pair
    // because it really should be the string_type of the underlying
    // tree, but since ndb_ordered_index is not templated we can't
    // really do better than this for now
    //
    // we keep value as std::string b/c we have more control over how those
    // strings are generated
    virtual bool invoke(const char *keyp, size_t keylen,
                        const varstr &value) = 0;
  };

  /**
   * Get a key of length keylen. The underlying DB does not manage
   * the memory associated with key. Returns true if found, false otherwise
   */
  inline rc_t get(transaction *t, const varstr &key, varstr &value,
                  OID *oid = nullptr) {
    return tree_.do_search(*t, key, &value, oid);
  }

  /**
   * Put a key of length keylen, with mapping of length valuelen.
   * The underlying DB does not manage the memory pointed to by key or value
   * (a copy is made).
   *
   * If a record with key k exists, overwrites. Otherwise, inserts.
   *
   * If the return value is not NULL, then it points to the actual stable
   * location in memory where the value is located. Thus, [ret, ret+valuelen)
   * will be valid memory, bytewise equal to [value, value+valuelen), since the
   * implementations have immutable values for the time being. The value
   * returned is guaranteed to be valid memory until the key associated with
   * value is overriden.
   */
  inline rc_t put(transaction *t, const varstr &key, varstr &value) {
    return tree_.do_tree_put(*t, &key, &value, false, true, nullptr);
  }

  /**
   * Insert a key of length keylen.
   *
   * If a record with key k exists, behavior is unspecified- this function
   * is only to be used when you can guarantee no such key exists (ie in loading
   *phase)
   *
   * Default implementation calls put(). See put() for meaning of return value.
   */
  inline rc_t insert(transaction *t, const varstr &key, varstr &value,
                     OID *oid = nullptr) {
    return tree_.do_tree_put(*t, &key, &value, true, true, oid);
  }

  /**
   * Insert into a secondary index. Maps key to OID.
   */
  inline rc_t insert(transaction *t, const varstr &key, OID oid) {
    return tree_.do_tree_put(*t, &key, (varstr *)&oid, true, false, nullptr);
  }

  /**
   * Search [start_key, *end_key) if end_key is not null, otherwise
   * search [start_key, +infty)
   */
  rc_t scan(transaction *t, const varstr &start_key, const varstr *end_key,
            scan_callback &callback, str_arena *arena);
  /**
   * Search (*end_key, start_key] if end_key is not null, otherwise
   * search (-infty, start_key] (starting at start_key and traversing
   * backwards)
   */
  rc_t rscan(transaction *t, const varstr &start_key, const varstr *end_key,
             scan_callback &callback, str_arena *arena);

  /**
   * Default implementation calls put() with NULL (zero-length) value
   */
  inline rc_t remove(transaction *t, const varstr &key) {
    return tree_.do_tree_put(*t, &key, nullptr, false, false, nullptr);
  }

  /**
   * Only an estimate, not transactional!
   */
  inline size_t size() const { return tree_.size_estimate(); }

  /**
   * Not thread safe for now
   */
  inline std::map<std::string, uint64_t> clear() {
    return tree_.unsafe_purge(true);
  }

  inline IndexDescriptor *GetDescriptor() { return descriptor_; }
  inline void SetArrays() { tree_.set_arrays(descriptor_); }

 private:
  base_txn_btree tree_;
  IndexDescriptor *descriptor_;
};
}  // namespace ermia
