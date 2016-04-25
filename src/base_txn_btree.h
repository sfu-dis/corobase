#pragma once

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

class base_txn_btree {
    friend class sm_log;    // for recover_index()
public:

  typedef dbtuple::size_type size_type;
  typedef concurrent_btree::string_type keystring_type;

  struct search_range_callback {
  public:
    rc_t return_code;
    search_range_callback() : return_code(rc_t{RC_FALSE}) {}
    virtual ~search_range_callback() {}
    virtual bool invoke(const keystring_type &k, const varstr &v) = 0;
  };

  base_txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : value_size_hint(value_size_hint),
      name(name),
      fid(0),
      been_destructed(false)
  {
  }

  ~base_txn_btree()
  {
    if (!been_destructed)
      unsafe_purge(false);
  }

  inline void set_tuple_vec(FID f)
  {
    fid = f;
    underlying_btree.set_tuple_vec(oidmgr->get_array(f));
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

  struct txn_search_range_callback : public concurrent_btree::low_level_search_range_callback {
    constexpr txn_search_range_callback(
          transaction *t,
          search_range_callback *caller_callback,
          key_reader *key_reader,
          value_reader *value_reader)
      : t(t), caller_callback(caller_callback),
        kr(key_reader), vr(value_reader) {}

    virtual void on_resp_node(const typename concurrent_btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const concurrent_btree *btr_ptr,
                        const typename concurrent_btree::string_type &k, dbtuple* v,
                        const typename concurrent_btree::node_opaque_t *n, uint64_t version);

  private:
    transaction *const t;
    search_range_callback *const caller_callback;
    key_reader *const kr;
    value_reader *const vr;
  };

  rc_t
  do_search(transaction &t, const varstr &k, value_reader &value_reader);

  void
  do_search_range_call(transaction &t,
                       const varstr &lower,
                       const varstr *upper,
                       search_range_callback &callback,
                       key_reader &key_reader,
                       value_reader &value_reader);

  void
  do_rsearch_range_call(transaction &t,
                        const varstr &upper,
                        const varstr *lower,
                        search_range_callback &callback,
                        key_reader &key_reader,
                        value_reader &value_reader);

  // expect_new indicates if we expect the record to not exist in the tree-
  // is just a hint that affects perf, not correctness. remove is put with nullptr
  // as value.
  //
  // NOTE: both key and value are expected to be stable values already
  rc_t do_tree_put(transaction &t,
                   const varstr *k,
                   const varstr *v,
                   bool expect_new);

  concurrent_btree underlying_btree;
  size_type value_size_hint;
  std::string name;
  FID fid;
  bool been_destructed;
};
