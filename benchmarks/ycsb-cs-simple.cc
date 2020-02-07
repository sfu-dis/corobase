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

      for(uint32_t i = 0; i < g_reps_per_tx; i++) {
        ermia::dia::query_scheduler.run();
        for(auto &h : handles)
          h.resume();
      }

      for(uint32_t i = 0; i < batch_size; i++) {
        ALWAYS_ASSERT(handles[i].done());
        finish_workload(handles[i].promise().current_value, workload_idxs[i], t);
        handles[i].destroy();
      }

      const unsigned long old_seed = r.get_seed();
      r.set_seed(old_seed);
      ermia::MM::epoch_exit(0, begin_epoch);
    }
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent() ||
        ycsb_workload.scan_percent() || ycsb_workload.rmw_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    if (ycsb_workload.read_percent()) {
      LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::SimpleCoro) << "Read txn type must be simple-coro";
      w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, TxnRead));
    }

    return w;
  }

  static CoroTxnHandle TxnRead(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_read(idx, begin_epoch).get_handle();
  }

  // Read transaction with context-switch using simple coroutine
  ermia::dia::generator<rc_t> txn_read(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;
    ermia::ConcurrentMasstree::threadinfo ti(begin_epoch);
    ermia::ConcurrentMasstree::versioned_node_t sinfo;

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
      ermia::OID oid = ermia::INVALID_OID;

      co_await table_index->GetMasstree().search_coro(k, oid, ti, &sinfo);
      rc._val = (oid != ermia::INVALID_OID) ? RC_TRUE : RC_FALSE;

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      if (rc.IsAbort()) {
        db->Abort(txn);
        co_return rc;
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
        co_return rc;
      }
    }
    co_return {RC_TRUE};
  }

private:
  ermia::transaction *transactions;
};

void ycsb_cs_simple_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_cs_worker> r(db);
  r.run();
}
