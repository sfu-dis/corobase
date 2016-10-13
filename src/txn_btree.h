#pragma once

#include "reader_writer.h"
#include "base_txn_btree.h"

/**
 * This class implements a serializable, multi-version b-tree
 *
 * It presents mostly same interface as the underlying concurrent b-tree,
 * but the interface is serializable. The main interface differences are,
 * insert() and put() do not return a boolean value to indicate whether or not
 * they caused the tree to mutate
 *
 * A txn_btree does not allow keys to map to NULL records, even though the
 * underlying concurrent btree does- this simplifies some of the book-keeping
 * Additionally, keys cannot map to zero length records.
 *
 * Note that the txn_btree *manages* the memory of both keys and values internally.
 * See the specific notes on search()/insert() about memory ownership
 */
class txn_btree : public base_txn_btree {
public:

  typedef typename base_txn_btree::keystring_type keystring_type;
  typedef typename dbtuple::size_type size_type;

  typedef varstr key_type;
  typedef varstr value_type;

private:
  static inline const varstr *
  stablize(transaction &t, const varstr &s)
  {
    varstr * const px = t.string_allocator()(s.size());
    px->copy_from(s.data(), s.size());
    return px;
  }

  static inline const varstr *
  stablize(transaction &t, const uint8_t *p, size_t sz)
  {
    if (!sz)
      return nullptr;
    varstr * const px = t.string_allocator()(sz);
    px->copy_from((const char *) p, sz);
    // copy_from will set size to readsz
    ASSERT(px->size() == sz);
    return px;
  }

public:

  txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : base_txn_btree(value_size_hint, mostly_append, name)
  {}

  // either returns false or v is set to not-empty with value
  // precondition: max_bytes_read > 0
  inline rc_t
  search(transaction &t,
         const key_type &k,
         value_type &v,
         size_type max_bytes_read = ~size_type(0))
  {
    value_reader r(&v, max_bytes_read, true);
    return this->do_search(t, k, r);
  }

  inline void
  search_range_call(transaction &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback,
                    size_type max_bytes_read = ~size_type(0))
  {
    key_reader kr;
    value_reader vr(max_bytes_read, false);
    this->do_search_range_call(t, lower, upper, callback, kr, vr);
  }

  inline void
  rsearch_range_call(transaction &t,
                     const key_type &upper,
                     const key_type *lower,
                     search_range_callback &callback,
                     size_type max_bytes_read = ~size_type(0))
  {
    key_reader kr;
    value_reader vr(max_bytes_read, false);
    this->do_rsearch_range_call(t, upper, lower, callback, kr, vr);
  }

  inline rc_t
  put(transaction &t, const varstr &k, const value_type &v)
  {
    return this->do_tree_put(t, stablize(t, k), stablize(t, v), false);
  }

  inline rc_t
  insert(transaction &t, const key_type &k, const value_type &v)
  {
    return this->do_tree_put(t, stablize(t, k), stablize(t, v), true);
  }

  // insert() methods below are for legacy use

  inline void
  insert(transaction &t, const varstr &k, const uint8_t *v, size_type sz)
  {
    ASSERT(v);
    ASSERT(sz);
    this->do_tree_put(t, stablize(t, k), stablize(t, v, sz), true);
  }

  inline rc_t
  remove(transaction &t, const varstr &k)
  {
    return this->do_tree_put(t, stablize(t, k), nullptr, false);
  }

  static void Test();
};
