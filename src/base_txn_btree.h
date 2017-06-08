#pragma once

#include "dbcore/sm-index.h"
#include "masstree_btree.h"
#include "util.h"

#include <string>
#include <map>

using namespace TXN;

class base_txn_btree {
  friend class sm_log_recover_impl;
  friend class sm_chkpt_mgr;

 public:
  typedef concurrent_btree::string_type keystring_type;

  struct search_range_callback {
   public:
    rc_t return_code;
    search_range_callback() : return_code(rc_t{RC_FALSE}) {}
    virtual ~search_range_callback() {}
    virtual bool invoke(const keystring_type &k, const varstr &v) = 0;
  };

  base_txn_btree(IndexDescriptor *id) : descriptor(id) {}

  void set_arrays(IndexDescriptor *id) { underlying_btree.set_arrays(id); }

  ~base_txn_btree() { unsafe_purge(false); }

  inline size_t size_estimate() const { return underlying_btree.size(); }

  inline void print() { underlying_btree.print(); }

  /**
   * only call when you are sure there are no concurrent modifications on the
   * tree. is neither threadsafe nor transactional
   *
   * Note that when you call unsafe_purge(), this txn_btree becomes
   * completely invalidated and un-usable. Any further operations
   * (other than calling the destructor) are undefined
   */
  std::map<std::string, uint64_t> unsafe_purge(bool dump_stats = false);

 public:
  struct txn_search_range_callback
      : public concurrent_btree::low_level_search_range_callback {
    constexpr txn_search_range_callback(transaction *t,
                                        search_range_callback *caller_callback)
        : t(t), caller_callback(caller_callback) {}

    virtual void on_resp_node(const typename concurrent_btree::node_opaque_t *n,
                              uint64_t version);
    virtual bool invoke(const concurrent_btree *btr_ptr,
                        const typename concurrent_btree::string_type &k,
                        dbtuple *v,
                        const typename concurrent_btree::node_opaque_t *n,
                        uint64_t version);

   private:
    transaction *const t;
    search_range_callback *const caller_callback;
  };

  rc_t do_search(transaction &t, const varstr &k, varstr *out_v, OID *out_oid);

  void do_search_range_call(transaction &t, const varstr &lower,
                            const varstr *upper,
                            search_range_callback &callback);

  void do_rsearch_range_call(transaction &t, const varstr &upper,
                             const varstr *lower,
                             search_range_callback &callback);

  // expect_new indicates if we expect the record to not exist in the tree-
  // is just a hint that affects perf, not correctness. remove is put with
  // nullptr
  // as value.
  //
  // NOTE: both key and value are expected to be stable values already
  rc_t do_tree_put(transaction &t, const varstr *k, varstr *v, bool expect_new,
                   bool upsert, OID *inserted_oid);

 private:
  concurrent_btree underlying_btree;
  IndexDescriptor *descriptor;

  struct purge_tree_walker : public concurrent_btree::tree_walk_callback {
    virtual void on_node_begin(
        const typename concurrent_btree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();

   private:
    std::vector<std::pair<typename concurrent_btree::value_type, bool> >
        spec_values;
  };
};
