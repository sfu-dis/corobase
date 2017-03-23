#include "../txn.h"
#include "ordered_index.h"

rc_t OrderedIndex::get(transaction *t, const varstr &key, varstr &value) {
  return tree_.do_search(*t, key, &value);
}

rc_t OrderedIndex::put(transaction *t, const varstr &key, const varstr &value) {
  return tree_.do_tree_put(*t, &key, &value, false, true, nullptr);
}

rc_t OrderedIndex::insert(transaction *t, const varstr &key, const varstr &value, OID* oid) {
  return tree_.do_tree_put(*t, &key, &value, true, true, oid);
}

rc_t OrderedIndex::insert(transaction* t, const varstr &key, OID oid) {
  return tree_.do_tree_put(*t, &key, (varstr*)&oid, true, false, nullptr);
}

rc_t OrderedIndex::remove(transaction *t, const varstr &key) {
  return tree_.do_tree_put(*t, &key, nullptr, false, false, nullptr);
}

class SearchRangeCallback : public base_txn_btree::search_range_callback {
public:
  SearchRangeCallback(OrderedIndex::scan_callback &upcall)
    : base_txn_btree::search_range_callback(), upcall(&upcall) {}

  virtual bool
  invoke(const base_txn_btree::keystring_type &k,
         const varstr &v)
  {
    return upcall->invoke(k.data(), k.length(), v);
  }

private:
  OrderedIndex::scan_callback *upcall;
};

rc_t OrderedIndex::scan(transaction *t, const varstr &start_key, const varstr *end_key,
                        scan_callback &callback, str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  key_reader kr;
  tree_.do_search_range_call(*t, start_key, end_key, c, kr);
  return c.return_code;
}

rc_t OrderedIndex::rscan(transaction *t, const varstr &start_key, const varstr *end_key,
                         scan_callback &callback, str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  key_reader kr;
  tree_.do_rsearch_range_call(*t, start_key, end_key, c, kr);
  return c.return_code;
}

std::map<std::string, uint64_t> OrderedIndex::clear() {
  return tree_.unsafe_purge(true);
}

