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
      arena->reset();

      for(uint32_t i = 0; i < batch_size; i++) {
        uint32_t workload_idx = fetch_workload();
        workload_idxs[i] = workload_idx;
        handles[i] = workload[workload_idx].coro_fn(this, i, begin_epoch);
      }

      uint32_t todo_size = batch_size;
      while (todo_size) {
        ermia::dia::query_scheduler.run();
        for(uint32_t j = 0; j < batch_size; j++) {
          if (handles[j]) {
            handles[j].resume();
            if (handles[j].done()) {
              finish_workload(handles[j].promise().current_value, workload_idxs[j], t);
              handles[j].destroy();
              handles[j] = nullptr;
              todo_size--;
            }
          }
        }
      }

      const unsigned long old_seed = r.get_seed();
      r.set_seed(old_seed);
      // TODO: epoch exit correctly
      ermia::MM::epoch_exit(0, begin_epoch);
    }
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent() || ycsb_workload.scan_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::SimpleCoro) << "Read txn type must be simple-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      w.push_back(workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, nullptr, TxnRMW));
    }
    return w;
  }

  static CoroTxnHandle TxnRead(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_read(idx, begin_epoch).get_handle();
  }

  static CoroTxnHandle TxnRMW(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_rmw(idx, begin_epoch).get_handle();
  }

  // Read transaction with context-switch using simple coroutine
  ermia::dia::generator<rc_t> txn_read(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;
    if (!ermia::config::index_probe_only) {
        txn = &transactions[idx];
        new (txn) ermia::transaction(
          ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, *arena);
        ermia::TXN::xid_context *xc = txn->GetXIDContext();
        xc->begin_epoch = begin_epoch;
    }

    for (int i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
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
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char*)v.data() == 'a');
#endif
      if (!ermia::config::index_probe_only)
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
    }

    if (!ermia::config::index_probe_only) {
      rc_t rc = db->Commit(txn);
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
    }
    co_return {RC_TRUE};
  }

  // Read-modify-write transaction with context-switch using simple coroutine
  ermia::dia::generator<rc_t> txn_rmw(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = &transactions[idx];
    new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    for (int i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};

      rc = co_await table_index->coro_GetRecord(txn, k, v);

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
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

      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
    }

    rc_t rc = db->Commit(txn);
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
    co_return {RC_TRUE};
  }

private:
  ermia::transaction *transactions;
};

void ycsb_cs_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_cs_worker> r(db);
  r.run();
}
