
/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#ifdef ADV_COROUTINE

#include "../str_arena.h"
#include "../dbcore/rcu.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-coroutine.h"
#include "../third-party/foedus/zipfian_random.hpp"
#include "bench.h"
#include "ycsb.h"

extern uint g_reps_per_tx;
extern ReadTransactionType g_read_txn_type;

extern YcsbWorkload ycsb_workload;

template<typename T>
using task = ermia::dia::task<T>;

class ycsb_cs_adv_worker : public ycsb_base_worker {
public:
  ycsb_cs_adv_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
    transactions = (ermia::transaction*)malloc(sizeof(ermia::transaction) * ermia::config::coro_batch_size);
  }

  virtual void MyWork(char *) override {
    if (g_read_txn_type != ReadTransactionType::AdvCoro) {
      ycsb_base_worker::MyWork(nullptr);
      return;
    }
    ALWAYS_ASSERT(is_worker);
    workload = get_workload();
    txn_counts.resize(workload.size());

    const size_t batch_size = ermia::config::coro_batch_size;
    std::vector<task<rc_t>> task_queue(batch_size);
    std::vector<uint32_t> task_workload_idxs(batch_size);

    barrier_a->count_down();
    barrier_b->wait_for();
    util::timer t;

    while (running) {
      ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
      arena->reset();

      for(uint32_t i = 0; i < batch_size; i++) {
        task<rc_t> & coro_task = task_queue[i];
        ASSERT(!coro_task.valid());
        uint32_t workload_idx = fetch_workload();

        task_workload_idxs[i] = workload_idx;

        ASSERT(workload[workload_idx].task_fn);
        coro_task = workload[workload_idx].task_fn(this, i, begin_epoch);
        coro_task.start();
      }

      bool batch_completed = false;
      while (!batch_completed) {
        batch_completed = true;
        for(uint32_t i = 0; i < batch_size; i++) {
          task<rc_t> & coro_task = task_queue[i];
          if (!coro_task.valid()) {
            continue;
          }

          if (!coro_task.done()) {
            coro_task.resume();
            batch_completed = false;
          } else {
            finish_workload(coro_task.get_return_value(), task_workload_idxs[i], t);
            coro_task = task<rc_t>(nullptr);
          }
        }
      }

      ermia::MM::epoch_exit(0, begin_epoch);
    }
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent() 
       || ycsb_workload.rmw_percent()) {
    }

    if (ycsb_workload.read_percent()) {
      if (g_read_txn_type == ReadTransactionType::AdvCoro) {
        w.push_back(workload_desc(
            "Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, nullptr, TxnRead));
      } else if (g_read_txn_type == ReadTransactionType::AdvCoroMultiGet) {
        w.push_back(workload_desc(
            "Read", double(ycsb_workload.read_percent()) / 100.0, TxnReadAdvCoroMultiGet));
      } else {
        LOG(FATAL) << "Wrong read transaction type. Supported: adv-coro and multiget-adv-coro";
      }
    }

    if (ycsb_workload.scan_percent()) {
      if (g_read_txn_type == ReadTransactionType::AdvCoro) {
        w.push_back(workload_desc("Scan", double(ycsb_workload.scan_percent()) / 100.0,
                    nullptr, nullptr,TxnScan));
      } else {
        LOG(FATAL) << "Scan txn type must be adv-coro";
      }
    }

    return w;
  }

  static task<rc_t> TxnRead(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_adv_worker *>(w)->txn_read(idx, begin_epoch);
  }

  static rc_t TxnReadAdvCoroMultiGet(bench_worker *w) {
    return static_cast<ycsb_cs_adv_worker *>(w)->txn_read_adv_coro_multi_get();
  }

  static task<rc_t> TxnScan(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
      return static_cast<ycsb_cs_adv_worker *>(w)->txn_scan(idx, begin_epoch);
  }

private:
  task<rc_t> txn_read(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;

    if (!ermia::config::index_probe_only) {
        txn = &transactions[idx];
        new (txn) ermia::transaction(
          ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, *arena);
          ermia::TXN::xid_context * xc = txn->GetXIDContext();
          xc->begin_epoch = begin_epoch;
    }

    for (int j = 0; j < g_reps_per_tx; ++j) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      if (!ermia::config::index_probe_only) {
        AWAIT table_index->GetRecord(txn, rc, k, v);  // Read
      } else {
        ermia::OID oid = 0;
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        rc = (AWAIT table_index->GetMasstree().search(k, oid, begin_epoch, &sinfo)) ? RC_TRUE : RC_FALSE;
      }

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char*)v.data() == 'a');
#endif

      if (!ermia::config::index_probe_only) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
        ALWAYS_ASSERT(*(char*)v.data() == 'a');
      }
    }

    if (!ermia::config::index_probe_only) {
        TryCatchCoro(db->Commit(txn));
    }

    co_return {RC_TRUE};
  }

  task<rc_t> txn_scan(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;

    txn = &transactions[idx];
    new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH |
                                     ermia::transaction::TXN_FLAG_READ_ONLY,
                                 *arena);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    for (int j = 0; j < g_reps_per_tx; ++j) {
        rc_t rc = rc_t{RC_INVALID};
        ScanRange range = GenerateScanRange(txn);

        if (ermia::config::index_probe_only) {
            ycsb_scan_oid_callback callback;
            co_await table_index->ScanOID(txn, range.start_key, &range.end_key,
                                          rc, callback);
        } else {
            ycsb_scan_callback callback;
            rc = co_await table_index->Scan(txn, range.start_key,
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

  // Multiget using advanced coroutine
  // FIXME(tzwang): this really should not be here (it should be in ycsb.cc). The
  // only reason it's here is it requires ADV_COROUTINE which is mutually
  // exclusive with other non-adv-coroutine variants.
  rc_t txn_read_adv_coro_multi_get() {
    arena->reset();
    ermia::transaction *txn = nullptr;

    thread_local std::vector<ermia::varstr *> keys;
    thread_local std::vector<ermia::varstr *> values;
    thread_local std::vector<ermia::dia::task<bool>> index_probe_tasks(g_reps_per_tx);
    thread_local std::vector<ermia::dia::task<void>> get_record_tasks(g_reps_per_tx);
    keys.clear();

    if (ermia::config::index_probe_only) {
      arena->reset();
    } else {
      values.clear();
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        if (ermia::config::index_probe_only) {
          values.push_back(&str(0));
        } else {
          values.push_back(&str(sizeof(ycsb_kv::value)));
        }
      }
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    }

    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(txn);
      keys.emplace_back(&k);
    }

    table_index->adv_coro_MultiGet(txn, keys, values, index_probe_tasks, get_record_tasks);

    if (!ermia::config::index_probe_only) {
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)values[i]->data(), sizeof(ycsb_kv::value));
      }
      ALWAYS_ASSERT(*(char*)v.data() == 'a');
    }

    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }

    return {RC_TRUE};
  }

private:
  ermia::transaction *transactions;
};

void ycsb_cs_advance_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_cs_adv_worker> r(db);
  r.run();
}
#endif  // ADV_COROUTINE
