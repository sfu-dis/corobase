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
    tx_arena = (ermia::str_arena *)malloc(sizeof(ermia::str_arena) * ermia::config::coro_batch_size);
    for (uint32_t i = 0; i < ermia::config::coro_batch_size; ++i) {
      new (&tx_arena[i]) ermia::str_arena(ermia::config::arena_size_mb);;
    }
  }

  // Essentially a coroutine scheduler that switches between active transactions
  virtual void MyWork(char *) override {
    // No replication support
    ALWAYS_ASSERT(is_worker);
    workload = get_workload();
    txn_counts.resize(workload.size());

    std::vector<std::experimental::coroutine_handle<ermia::dia::generator<bool>::promise_type>> handles(ermia::config::coro_batch_size);

    barrier_a->count_down();
    barrier_b->wait_for();
    util::timer t;
    while (running) {
      // Keep looking at the in-flight transactions (handles) and add more when we
      // finish a transaction
      for (uint32_t i = 0; i < handles.size(); ++i) {
        if (handles[i]) {
          if (handles[i].done()) {
            handles[i].destroy();
            handles[i] = nullptr;
            // FIXME(tzwang): get proper stats
            finish_workload(rc_t{RC_TRUE}, 0, t);
          } else {
            handles[i].resume();
          }
        }

        // Note: don't change this to 'else...' - we may change h in the previous if (h)
        if (!handles[i] && running) {
          double d = r.next_uniform();
          for (size_t j = 0; j < workload.size(); j++) {
            if ((j + 1) == workload.size() || d < workload[j].frequency) {
              const unsigned long old_seed = r.get_seed();
              handles[i] = workload[j].coro_fn(this, i);
              handles[i].resume();
              break;
            }
            d -= workload[j].frequency;
          }
        }
      }
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

  static SimpleCoroHandle TxnRead(bench_worker *w, uint32_t idx) {
    return static_cast<ycsb_cs_worker *>(w)->txn_read(idx);
  }

  // Read transaction with context-switch using simple coroutine
  SimpleCoroHandle txn_read(uint32_t idx) {
    thread_local ermia::transaction *tx_buffers = nullptr;
    if (!tx_buffers) {
      tx_buffers = (ermia::transaction *)malloc(sizeof(ermia::transaction) * ermia::config::coro_batch_size);
      keys = (std::vector<ermia::varstr *> *)malloc(
        sizeof(std::vector<ermia::varstr *>) * ermia::config::coro_batch_size);
      for (uint32_t i = 0; i < ermia::config::coro_batch_size; ++i) {
        new (&keys[i]) std::vector<ermia::varstr *>;
      }
    }

    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();

    ermia::transaction *txn = &tx_buffers[idx];
    new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH |
                                 ermia::transaction::TXN_FLAG_READ_ONLY, tx_arena[idx]);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    keys[idx].clear();
    for (int j = 0; j < g_reps_per_tx; ++j) {
      auto &k = GenerateKey(txn);
      keys[idx].push_back(&k);
    }
    ermia::MM::epoch_exit(0, begin_epoch);

    ermia::ConcurrentMasstree::threadinfo ti(xc->begin_epoch);
    return table_index->GetMasstree().ycsb_read_coro(txn, keys[idx], ti, nullptr).get_handle();
  }

private:
  std::vector<ermia::varstr *> *keys;
  std::vector<ermia::varstr *> values;
  ermia::str_arena *tx_arena;
};

void ycsb_cs_simple_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_cs_worker> r(db);
  r.run();
}
