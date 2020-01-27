
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

    ermia::dia::coro_task_private::memory_pool memory_pool;

    const size_t batch_size = ermia::config::coro_batch_size;
    std::vector<task<rc_t>> task_queue(batch_size);
    std::vector<ermia::dia::coro_task_private::coro_stack> call_stacks(batch_size);
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
        call_stacks[i].reserve(20);

        ASSERT(workload[workload_idx].task_fn);
        coro_task = workload[workload_idx].task_fn(this, i, begin_epoch);
        coro_task.set_call_stack(&call_stacks[i]);
      }

      bool batch_completed = false;
      while (!batch_completed && running) {
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

    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent() ||
        ycsb_workload.scan_percent() || ycsb_workload.rmw_percent()) {
      LOG(FATAL) << "Not implemented";
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

    return w;
  }

  static task<rc_t> TxnRead(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_adv_worker *>(w)->txn_read_coro_2_layer(idx, begin_epoch);
  }

  static rc_t TxnReadAdvCoroMultiGet(bench_worker *w) {
    return static_cast<ycsb_cs_adv_worker *>(w)->txn_read_adv_coro_multi_get();
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
      if (rc.IsAbort()) {
        db->Abort(txn);
        co_return rc;
      }
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
        rc_t rc = db->Commit(txn);
        if (rc.IsAbort()) {
            db->Abort(txn);
            co_return rc;
        }
    }

    co_return {RC_TRUE};
  }

  task<rc_t> txn_read_coro_2_layer(uint32_t idx, ermia::epoch_num begin_epoch) {
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
      ermia::OID oid = 0;
      ermia::ConcurrentMasstree::versioned_node_t sinfo;
      rc_t rc = rc_t{RC_INVALID};

      if (!ermia::config::index_probe_only) txn->ensure_active();
      bool found = AWAIT table_index->GetMasstree().search_coro_1_layer(k, oid, begin_epoch, &sinfo);
      if (!ermia::config::index_probe_only) {
        ermia::dbtuple *tuple = nullptr;
        if (found) {
          tuple = AWAIT ermia::oidmgr->oid_get_version(table_index->GetMasstree().get_table_descriptor()->GetTupleArray(),
                                                          oid, txn->GetXIDContext());
          if (!tuple) found = false;
        }

        if (found)
          ermia::volatile_write(rc._val, txn->DoTupleRead(tuple, &v)._val);
        else
          ermia::volatile_write(rc._val, RC_FALSE);
      } else {
        rc = found ? RC_TRUE : RC_FALSE;
      }

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

      if (!ermia::config::index_probe_only) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
        ALWAYS_ASSERT(*(char*)v.data() == 'a');
      }
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
    thread_local std::vector<ermia::dia::task<ermia::dbtuple*>> value_fetch_tasks(g_reps_per_tx);
    thread_local std::vector<ermia::dia::coro_task_private::coro_stack> coro_stacks(g_reps_per_tx);
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

    table_index->adv_coro_MultiGet(txn, keys, values, index_probe_tasks, value_fetch_tasks, coro_stacks);

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
