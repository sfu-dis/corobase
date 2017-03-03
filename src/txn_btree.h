#pragma once

#include "reader_writer.h"
#include "base_txn_btree.h"

class txn_btree : public base_txn_btree {
public:

  typedef typename base_txn_btree::keystring_type keystring_type;
  typedef typename dbtuple::size_type size_type;

public:

  txn_btree(const std::string &name = "<unknown>") : base_txn_btree(name) {}

  static inline const varstr *stablize(transaction &t, const varstr &s) {
    varstr * const px = t.string_allocator()(s.size());
    px->copy_from(s.data(), s.size());
    return px;
  }

  // either returns false or v is set to not-empty with value
  // precondition: max_bytes_read > 0
  inline rc_t search(transaction &t, const varstr &k, varstr &v,
                     size_type max_bytes_read = ~size_type(0)) {
    value_reader r(&v, max_bytes_read, true);
    return this->do_search(t, k, r);
  }

  inline void search_range_call(transaction &t,
                                const varstr &lower,
                                const varstr *upper,
                                search_range_callback &callback,
                                size_type max_bytes_read = ~size_type(0)) {
    key_reader kr;
    value_reader vr(max_bytes_read, false);
    this->do_search_range_call(t, lower, upper, callback, kr, vr);
  }

  inline void rsearch_range_call(transaction &t,
                                 const varstr &upper,
                                 const varstr *lower,
                                 search_range_callback &callback,
                                 size_type max_bytes_read = ~size_type(0)) {
    key_reader kr;
    value_reader vr(max_bytes_read, false);
    this->do_rsearch_range_call(t, upper, lower, callback, kr, vr);
  }

  inline rc_t put(transaction &t, const varstr &k, const varstr &v) {
    return this->do_tree_put(t, stablize(t, k), stablize(t, v), false);
  }

  inline rc_t insert(transaction &t, const varstr &k, const varstr &v) {
    return this->do_tree_put(t, stablize(t, k), stablize(t, v), true);
  }

  inline rc_t remove(transaction &t, const varstr &k) {
    return this->do_tree_put(t, stablize(t, k), nullptr, false);
  }
};
