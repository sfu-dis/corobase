#pragma once
#include <unordered_map>

#include "abstract_db.h"
#include "../txn_btree.h"
#include "../dbcore/sm-log.h"

namespace private_ {
  struct ndbtxn {
    abstract_db::TxnProfileHint hint;
    char buf[0];
  } PACKED;
}

class ndb_wrapper : public abstract_db {
protected:
  typedef private_::ndbtxn ndbtxn;

public:

  ndb_wrapper() {}
  ~ndb_wrapper() {}

  virtual ssize_t txn_max_batch_size() const override { return 100; }

  virtual size_t
  sizeof_txn_object(uint64_t txn_flags) const;

  virtual void *new_txn(
      uint64_t txn_flags,
      str_arena &arena,
      void *buf,
      TxnProfileHint hint);
  virtual rc_t commit_txn(void *txn);
  virtual void abort_txn(void *txn);

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append);

  virtual void
  close_index(abstract_ordered_index *idx);
};

class ndb_ordered_index : public abstract_ordered_index {
    friend class sm_log_recover_impl;
protected:
  typedef private_::ndbtxn ndbtxn;

public:
  ndb_ordered_index(const std::string &name, size_t value_size_hint, bool mostly_append) :
    name(name), btr(value_size_hint, mostly_append, name) {}

  virtual rc_t get(
      void *txn,
      const varstr &key,
      varstr &value, size_t max_bytes_read);
  virtual rc_t put(
      void *txn,
      const varstr &key,
      const varstr &value);
  virtual rc_t put(
      void *txn,
      varstr &&key,
      varstr &&value);
  virtual rc_t
  insert(void *txn,
         const varstr &key,
         const varstr &value);
  virtual rc_t
  insert(void *txn,
         varstr &&key,
         varstr &&value);
  virtual rc_t scan(
      void *txn,
      const varstr &start_key,
      const varstr *end_key,
      scan_callback &callback,
      str_arena *arena);
  virtual rc_t rscan(
      void *txn,
      const varstr &start_key,
      const varstr *end_key,
      scan_callback &callback,
      str_arena *arena);
  virtual rc_t remove(
      void *txn,
      const varstr &key);
  virtual rc_t remove(
      void *txn,
      varstr &&key);
  virtual size_t size() const;
  virtual std::map<std::string, uint64_t> clear();
  inline void set_oid_array(FID fid) { btr.set_oid_array(fid); }
  inline oid_array* get_oid_array() { return btr.get_oid_array(); }
private:
  std::string name;
  txn_btree btr;
};
