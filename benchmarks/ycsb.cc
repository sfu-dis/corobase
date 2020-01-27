/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include "bench.h"
#include "ycsb.h"

#ifndef ADV_COROUTINE

extern uint g_reps_per_tx;
extern uint g_rmw_additional_reads;
extern int g_amac_txn_read;
extern int g_coro_txn_read;
extern YcsbWorkload ycsb_workload;

class ycsb_sequential_worker : public ycsb_base_worker {
 public:
  ycsb_sequential_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                         const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                         spin_barrier *barrier_a, spin_barrier *barrier_b)
    : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
  }

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;
    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent() || ycsb_workload.scan_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    if (ycsb_workload.read_percent()) {
      if (g_amac_txn_read) {
        w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnReadAMACMultiGet));
      } else if (g_coro_txn_read) {
        w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnReadSimpleCoroMultiGet));
      } else {
        w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnRead));
      }
    }

    if (ycsb_workload.rmw_percent()) {
      w.push_back(workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, TxnRMW));
    }

    return w;
  }

  static rc_t TxnRead(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read(); }
  static rc_t TxnReadAMACMultiGet(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read_amac_multiget(); }
  static rc_t TxnReadSimpleCoroMultiGet(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read_simple_coro_multiget(); }
  static rc_t TxnRMW(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_rmw(); }

  rc_t txn_read() {
    ermia::transaction *txn = nullptr;
    if (!ermia::config::index_probe_only) {
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    }

    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(txn);
      ermia::varstr &v = str((ermia::config::index_probe_only) ? 0 : sizeof(YcsbRecord));
      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char*)v.data() == 'a');
#endif
      if (!ermia::config::index_probe_only) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(YcsbRecord));
      }
    }
    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }
    return {RC_TRUE};
  }

  rc_t txn_read_amac_multiget() {
    ermia::transaction *txn = nullptr;
    if (!ermia::config::index_probe_only) {
      values.clear();
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        if (ermia::config::index_probe_only) {
          values.push_back(&str(0));
        } else {
          values.push_back(&str(sizeof(YcsbRecord)));
        }
      }
    }

    // Prepare states
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(txn);
      if (as.size() < g_reps_per_tx)
        as.emplace_back(&k);
      else
        as[i].reset(&k);
    }

    table_index->amac_MultiGet(txn, as, values);

    if (!ermia::config::index_probe_only) {
      ermia::varstr &v = str(sizeof(YcsbRecord));
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(YcsbRecord));
      }
      ALWAYS_ASSERT(*(char*)v.data() == 'a');
    }

    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }
    return {RC_TRUE};
  }

  rc_t txn_read_simple_coro_multiget() {
    thread_local std::vector<SimpleCoroHandle> handles(g_reps_per_tx);
    keys.clear();
    values.clear();
  
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(nullptr);
      keys.emplace_back(&k);
    }

    table_index->simple_coro_MultiGet(nullptr, keys, values, handles);

    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(YcsbRecord));
      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      ermia::OID oid = ermia::INVALID_OID;
      table_index->GetRecord(txn, rc, k, v, &oid);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      LOG_IF(FATAL, rc._val != RC_TRUE);
      ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      if (!ermia::config::index_probe_only) {
        ALWAYS_ASSERT(v.size() == sizeof(YcsbRecord));
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());
      }

      // Re-initialize the value structure to use my own allocated memory -
      // DoTupleRead will change v.p to the object's data area to avoid memory
      // copy (in the read op we just did).
      new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(YcsbRecord));
      new (v.data()) YcsbRecord('a');
      TryCatch(table_index->UpdateRecord(txn, k, v));  // Modify-write
    }

    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(YcsbRecord));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif
      if (!ermia::config::index_probe_only) {
        ALWAYS_ASSERT(v.size() == sizeof(YcsbRecord));
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());
      }

    }
    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

 private:
  std::vector<ermia::ConcurrentMasstree::AMACState> as;
  std::vector<ermia::varstr *> keys;
  std::vector<ermia::varstr *> values;
};

void ycsb_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_sequential_worker> r(db);
  r.run();
}

#endif  // ADV_COROUTINE
