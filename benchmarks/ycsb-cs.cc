/* 
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include "bench.h"
#include "ycsb.h"

extern uint g_reps_per_tx;
extern ReadTransactionType g_read_txn_type;
extern YcsbWorkload ycsb_workload;

class ycsb_cs_worker : public ycsb_base_worker {
public:
  ycsb_cs_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
    transactions = (ermia::transaction*)malloc(sizeof(ermia::transaction) * ermia::config::coro_batch_size);
    arenas = (ermia::str_arena*)malloc(sizeof(ermia::str_arena) * ermia::config::coro_batch_size);
    for (auto i = 0; i < ermia::config::coro_batch_size; ++i) {
      new (arenas + i) ermia::str_arena(ermia::config::arena_size_mb);
    }
  }

  // Essentially a coroutine scheduler that switches between active transactions
  virtual void MyWork(char *) override {
    // No replication support
    ALWAYS_ASSERT(is_worker);
    workload = get_workload();
    txn_counts.resize(workload.size());

    const size_t batch_size = ermia::config::coro_batch_size;
    std::vector<CoroTxnHandle> handles(batch_size);
    std::vector<uint32_t> workload_idxs(batch_size);

    barrier_a->count_down();
    barrier_b->wait_for();
    util::timer t;
    while (running) {
      ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();

      for(uint32_t i = 0; i < batch_size; i++) {
        uint32_t workload_idx = fetch_workload();
        workload_idxs[i] = workload_idx;
        handles[i] = workload[workload_idx].coro_fn(this, i, begin_epoch).get_handle();
      }

      uint32_t todo_size = batch_size;
      while (todo_size) {
        for(uint32_t i = 0; i < batch_size; i++) {
          if (handles[i]) {
            if (handles[i].done()) {
              finish_workload(handles[i].promise().get_return_value(), workload_idxs[i], t);
              handles[i].destroy();
              handles[i] = nullptr;
              todo_size--;
            } else if (handles[i].promise().callee_coro.done()) {
              handles[i].resume();
            } else {
              handles[i].promise().callee_coro.resume();
            }
          }
        }
      }

      const unsigned long old_seed = r.get_seed();
      r.set_seed(old_seed);
      // TODO: epoch exit correctly
      ermia::MM::epoch_exit(0, begin_epoch);

      if (ermia::config::index_probe_only)
        arena->reset(); // GenerateKey(nullptr) uses global arena
    }
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::SimpleCoro) << "Read txn type must be simple-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      w.push_back(workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, nullptr, TxnRMW));
    }

    if (ycsb_workload.scan_percent()) {
      w.push_back(workload_desc("RMW", double(ycsb_workload.scan_percent()) / 100.0, nullptr, TxnScan));
    }
    return w;
  }

  static ermia::dia::generator<rc_t> TxnRead(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_read(idx, begin_epoch);
  }

  static ermia::dia::generator<rc_t> TxnRMW(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_rmw(idx, begin_epoch);
  }

  static ermia::dia::generator<rc_t> TxnScan(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_scan(idx, begin_epoch);
  }

  // Read transaction with context-switch using simple coroutine
  ermia::dia::generator<rc_t> txn_read(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;
    if (ermia::config::index_probe_only) {
      arenas[idx].reset();
    } else {
      txn = db->NewTransaction(
          ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY,
          arenas[idx],
          &transactions[idx]);
      ermia::TXN::xid_context *xc = txn->GetXIDContext();
      xc->begin_epoch = begin_epoch;
    }

    for (int i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};

      if (ermia::config::index_probe_only) {
        ermia::ConcurrentMasstree::threadinfo ti(begin_epoch);
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        ermia::OID oid = ermia::INVALID_OID;
        rc._val = (co_await table_index->GetMasstree().search_coro(k, oid, ti, &sinfo)) ? RC_TRUE : RC_FALSE;
      } else {
        rc = co_await table_index->coro_GetRecord(txn, k, v);
      }
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char*)v.data() == 'a');
#endif
      if (!ermia::config::index_probe_only)
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
    }

    if (!ermia::config::index_probe_only) {
        TryCatchCoro(db->Commit(txn));
    }
    co_return {RC_TRUE};
  }

  // Read-modify-write transaction with context-switch using simple coroutine
  ermia::dia::generator<rc_t> txn_rmw(uint32_t idx, ermia::epoch_num begin_epoch) {
    auto *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[idx], &transactions[idx]);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    for (int i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};

      rc = co_await table_index->coro_GetRecord(txn, k, v);

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // Under SI this must succeed
      LOG_IF(FATAL, rc._val != RC_TRUE);
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      ASSERT(v.size() == sizeof(ycsb_kv::value));
      memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());

      // Re-initialize the value structure to use my own allocated memory -
      // DoTupleRead will change v.p to the object's data area to avoid memory
      // copy (in the read op we just did).
      new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
      new (v.data()) ycsb_kv::value("a");
      rc = co_await table_index->coro_UpdateRecord(txn, k, v);  // Modify-write

      TryCatchCoro(rc);
    }

    TryCatchCoro(db->Commit(txn));
    co_return {RC_TRUE};
  }

  ermia::dia::generator<rc_t> txn_scan(uint32_t idx, ermia::epoch_num begin_epoch) {
    auto *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[idx], &transactions[idx]);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    rc_t rc = rc_t{RC_INVALID};
    for (int i = 0; i < g_reps_per_tx; ++i) {
      ScanRange range = GenerateScanRange(txn);
      if (ermia::config::index_probe_only) {
        LOG(FATAL) << "Not implemented";
      } else {
        ycsb_scan_callback callback;
        rc = co_await table_index->coro_Scan(txn, range.start_key,
                                             &range.end_key, callback);
      }
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // TODO(lujc): sometimes return RC_FALSE, no value?
      // ALWAYS_ASSERT(rc._val == RC_TRUE);
#endif
    }
    TryCatchCoro(db->Commit(txn));
    co_return {RC_TRUE};
  }

private:
  ermia::transaction *transactions;
  ermia::str_arena *arenas;
};

void ycsb_cs_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_cs_worker> r(db);
  r.run();
}
