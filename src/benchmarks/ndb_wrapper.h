#pragma once
#include <map>
#include "../dbcore/sm-index.h"
#include "../dbcore/sm-rc.h"
#include "../str_arena.h"

class ndb_wrapper {
 public:
  typedef std::map<std::string, uint64_t> counter_map;
  typedef std::map<std::string, counter_map> txn_counter_map;

  ndb_wrapper() {}
  ~ndb_wrapper() {}

  transaction *new_txn(uint64_t txn_flags, str_arena &arena, transaction *buf);
  rc_t commit_txn(transaction *txn);
  void abort_txn(transaction *txn);

  inline counter_map get_txn_counters(transaction *txn) const {
    return counter_map();
  }
  inline ssize_t txn_max_batch_size() const { return 10; }
};
