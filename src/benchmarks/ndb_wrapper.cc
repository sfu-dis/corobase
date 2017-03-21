#ifndef _NDB_WRAPPER_IMPL_H_
#define _NDB_WRAPPER_IMPL_H_

#include <stdint.h>
#include "ndb_wrapper.h"
#include "../dbcore/rcu.h"
#include "../dbcore/sm-index.h"
#include "../macros.h"
#include "../util.h"
#include "../txn.h"
#include "../tuple.h"

transaction* ndb_wrapper::new_txn(uint64_t flags, str_arena &arena, transaction *buf) {
  new (buf) transaction(flags, arena);
  return buf;
}

rc_t ndb_wrapper::commit_txn(transaction *t) {
  rc_t rc = t->commit();
  if (not rc_is_abort(rc))
    t->~transaction();
  return rc;
}

void ndb_wrapper::abort_txn(transaction *t) {
  t->abort_impl();
  t->~transaction();
}

void ndb_wrapper::close_index(ndb_ordered_index *idx) {
  delete idx;
}

rc_t ndb_ordered_index::get(transaction *t, const varstr &key,
                            varstr &value, size_t max_bytes_read) {
  return btr.search(*t, key, value, max_bytes_read);
}

rc_t ndb_ordered_index::put(transaction *t, const varstr &key, const varstr &value) {
  return btr.put(*t, key, value);
}

rc_t ndb_ordered_index::put(transaction *t, varstr &&key, varstr &&value) {
  return btr.put(*t, std::move(key), std::move(value));
}

rc_t ndb_ordered_index::insert(transaction *t, const varstr &key, const varstr &value) {
  return btr.insert(*t, key, value);
}

rc_t ndb_ordered_index::insert(transaction *t, varstr &&key, varstr &&value) {
  return btr.insert(*t, std::move(key), std::move(value));
}

class ndb_wrapper_search_range_callback : public txn_btree::search_range_callback {
public:
  ndb_wrapper_search_range_callback(ndb_ordered_index::scan_callback &upcall)
    : txn_btree::search_range_callback(), upcall(&upcall) {}

  virtual bool
  invoke(const txn_btree::keystring_type &k,
         const varstr &v)
  {
    return upcall->invoke(k.data(), k.length(), v);
  }

private:
  ndb_ordered_index::scan_callback *upcall;
};

rc_t
ndb_ordered_index::scan(transaction *t, const varstr &start_key, const varstr *end_key,
                        scan_callback &callback, str_arena *arena) {
  ndb_wrapper_search_range_callback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  btr.search_range_call(*t, start_key, end_key, c);
  return c.return_code;
}

rc_t
ndb_ordered_index::rscan(transaction *t, const varstr &start_key, const varstr *end_key,
                         scan_callback &callback, str_arena *arena) {
  ndb_wrapper_search_range_callback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  btr.rsearch_range_call(*t, start_key, end_key, c);
  return c.return_code;
}

rc_t
ndb_ordered_index::remove(transaction *t, const varstr &key) {
  return btr.remove(*t, key);
}

rc_t ndb_ordered_index::remove(transaction *t, varstr &&key) {
  return btr.remove(*t, std::move(key));
}

std::map<std::string, uint64_t>
ndb_ordered_index::clear()
{
  return btr.unsafe_purge(true);
}

#endif /* _NDB_WRAPPER_IMPL_H_ */
