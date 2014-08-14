#ifndef __XID_TABLE_
#define __XID_TABLE_

#include <iostream>
#include "macros.h"
#include "rcu/xid.h"
#include "core.h"

using namespace RCU;

// TXN_EMBRYO - the transaction object has been allocated but has not
// done any operations yet
// TXN_COMMITTING - getting CLSN
enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_COMMITTING, TXN_CMMTD, TXN_ABRTD };

class txn_table {
  // FIXME: tzwang: got "invalid argument" at xid_free if table size
  // is as large as 8192
  static const unsigned int TXN_TABLE_SIZE = 960;
public:
  // FIXME: tzwang: use xid_context?
  typedef struct txn_descriptor {
    XID xid;
    LSN start_lsn;
    LSN commit_lsn;
    txn_state state;
    bool in_use;
    // FIXME: tzwang: add tx-log
  } txn_descriptor;

private:
  // FIXME: tzwang: each core owns a subset of the whole table, and
  // only finds free space in its own subset at td_alloc.
  txn_descriptor txn_descs_[TXN_TABLE_SIZE];
  percore_lazy<unsigned int> next_descs_;

  inline unsigned int tds_per_core()
  {
    return TXN_TABLE_SIZE / coreid::num_cpus_online();
  }

  inline txn_descriptor* my_table()
  {
    return txn_descs_ + coreid::core_id() * tds_per_core();
  }

private:
  txn_table()
  {
    memset(txn_descs_, 0, sizeof(txn_descriptor) * TXN_TABLE_SIZE);
  }

public:
  txn_descriptor* td_alloc();
  void td_free(txn_descriptor *td, txn_state ts);
  static txn_table instance;
};
#endif
