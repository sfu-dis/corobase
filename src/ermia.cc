#include "ermia.h"

namespace ermia {

class SearchRangeCallback : public base_txn_btree::search_range_callback {
 public:
  SearchRangeCallback(OrderedIndex::scan_callback &upcall)
      : base_txn_btree::search_range_callback(), upcall(&upcall) {}

  virtual bool invoke(const base_txn_btree::keystring_type &k,
                      const varstr &v) {
    return upcall->invoke(k.data(), k.length(), v);
  }

 private:
  OrderedIndex::scan_callback *upcall;
};

rc_t OrderedIndex::scan(transaction *t, const varstr &start_key,
                        const varstr *end_key, scan_callback &callback,
                        str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  tree_.do_search_range_call(*t, start_key, end_key, c);
  return c.return_code;
}

rc_t OrderedIndex::rscan(transaction *t, const varstr &start_key,
                         const varstr *end_key, scan_callback &callback,
                         str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  tree_.do_rsearch_range_call(*t, start_key, end_key, c);
  return c.return_code;
}
}  // namespace ermia
