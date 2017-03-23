#pragma once
#include <unordered_map>

#include "../base_txn_btree.h"
#include "../dbcore/sm-index.h"
#include "../dbcore/sm-log.h"

#include "ndb_wrapper.h"

// The user-facing index; largely a refactor of ndb_ordered_index but with
// the IndexDescriptor thingy.
class OrderedIndex {
  friend class sm_log_recover_impl;
  friend class sm_chkpt_mgr;

public:
  OrderedIndex(IndexDescriptor* id) : tree_(id), descriptor_(id) {}

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
  rc_t get(transaction *t, const varstr &key, varstr &value);

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
  rc_t put(transaction *t, const varstr &key, const varstr &value);
  rc_t put(transaction *t, varstr &&key, varstr &&value);

  /**
   * Insert a key of length keylen.
   *
   * If a record with key k exists, behavior is unspecified- this function
   * is only to be used when you can guarantee no such key exists (ie in loading phase)
   *
   * Default implementation calls put(). See put() for meaning of return value.
   */
  rc_t insert(transaction *t, const varstr &key, const varstr &value, OID* oid = nullptr);
  rc_t insert(transaction *t, varstr &&key, varstr &&value);

  /**
   * Insert into a secondary index. Maps key to OID.
   */
  rc_t insert(transaction *t, const varstr &key, OID oid);

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
  rc_t remove(transaction *t, const varstr &key);
  rc_t remove(transaction *t, varstr &&key);

  /**
   * Only an estimate, not transactional!
   */
  inline size_t size() const { return tree_.size_estimate(); }

  /**
   * Not thread safe for now
   */
  std::map<std::string, uint64_t> clear();

  inline IndexDescriptor* GetDescriptor() { return descriptor_; }
  inline void SetArrays() { tree_.set_arrays(descriptor_); }

private:
  base_txn_btree tree_;
  IndexDescriptor* descriptor_;
};
