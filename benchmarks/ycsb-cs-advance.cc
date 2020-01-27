
/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <getopt.h>
#include <numa.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef ADV_COROUTINE

#include "../str_arena.h"
#include "../dbcore/rcu.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-coroutine.h"
#include "../third-party/foedus/zipfian_random.hpp"
#include "bench.h"
#include "ycsb.h"

extern uint g_reps_per_tx;
extern uint g_rmw_additional_reads;
extern char g_workload;
extern uint g_initial_table_size;
extern int g_zipfian_rng;
extern double g_zipfian_theta; // zipfian constant, [0, 1), more skewed as it
                               // approaches 1.
extern int g_adv_coro_txn_read;

// { insert, read, update, scan, rmw }
extern YcsbWorkload YcsbWorkloadA;
extern YcsbWorkload YcsbWorkloadB;
extern YcsbWorkload YcsbWorkloadC;
extern YcsbWorkload YcsbWorkloadD;
extern YcsbWorkload YcsbWorkloadE;

// Combine reps_per_tx and rmw_additional_reads to have "10R+10RMW" style
// transactions.
extern YcsbWorkload YcsbWorkloadF;

// Extra workloads (not in spec)
extern YcsbWorkload YcsbWorkloadG;
extern YcsbWorkload YcsbWorkloadH;

extern YcsbWorkload ycsb_workload;

template<typename T>
using task = ermia::dia::task<T>;

class ycsb_cs_adv_worker : public bench_worker {
public:
  ycsb_cs_adv_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a,
                     barrier_b),
        tbl((ermia::ConcurrentMasstreeIndex*)open_tables.at("USERTABLE")),
        uniform_rng(1237 + worker_id) {
    if (g_zipfian_rng) {
      zipfian_rng.init(g_initial_table_size, g_zipfian_theta, 1237 + worker_id);
    }

    transactions = static_cast<ermia::transaction*>(malloc(sizeof(ermia::transaction) * ermia::config::coro_batch_size));
  }

  virtual void MyWork(char *) override {
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

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const override {
    LOG(FATAL) << "Not applicable";
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent())
      w.push_back(workload_desc(
          "Insert", double(ycsb_workload.insert_percent()) / 100.0, nullptr, nullptr, nullptr));
    if (ycsb_workload.read_percent()) {
      if (g_adv_coro_txn_read) {
        w.push_back(workload_desc(
            "Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, nullptr, TxnRead));
      } else {
        w.push_back(workload_desc(
            "Read", double(ycsb_workload.read_percent()) / 100.0, TxnReadAdvCoroMultiGet));
      }
    }
    if (ycsb_workload.update_percent())
      w.push_back(workload_desc(
          "Update", double(ycsb_workload.update_percent()) / 100.0, nullptr, nullptr, nullptr));
    if (ycsb_workload.scan_percent())
      w.push_back(workload_desc(
          "Scan", double(ycsb_workload.scan_percent()) / 100.0, nullptr, nullptr, nullptr));
    if (ycsb_workload.rmw_percent())
      w.push_back(workload_desc(
          "RMW", double(ycsb_workload.rmw_percent()) / 100.0, nullptr, nullptr, nullptr));

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
      ermia::varstr &k = GenerateKey();
      ermia::varstr &v = str(sizeof(YcsbRecord));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      if (!ermia::config::index_probe_only) {
        AWAIT tbl->GetRecord(txn, rc, k, v);  // Read
      } else {
        ermia::OID oid = 0;
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        rc = (AWAIT tbl->GetMasstree().search(k, oid, begin_epoch, &sinfo)) ? RC_TRUE : RC_FALSE;
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
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(YcsbRecord));
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
      ermia::varstr &k = GenerateKey();
      ermia::varstr &v = str(sizeof(YcsbRecord));

      // TODO(tzwang): add read/write_all_fields knobs
      ermia::OID oid = 0;
      ermia::ConcurrentMasstree::versioned_node_t sinfo;
      rc_t rc = rc_t{RC_INVALID};

      if (!ermia::config::index_probe_only) txn->ensure_active();
      bool found = AWAIT tbl->GetMasstree().search_coro_1_layer(k, oid, begin_epoch, &sinfo);
      if (!ermia::config::index_probe_only) {
        ermia::dbtuple *tuple = nullptr;
        if (found) {
          tuple = AWAIT ermia::oidmgr->oid_get_version(tbl->GetMasstree().get_table_descriptor()->GetTupleArray(),
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
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(YcsbRecord));
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

  rc_t txn_read_adv_coro_multi_get() {
    arena->reset();
    ermia::transaction *txn = nullptr;

    thread_local std::vector<ermia::varstr *> keys;
    thread_local std::vector<ermia::varstr *> values;
    thread_local std::vector<ermia::dia::task<bool>> index_probe_tasks(g_reps_per_tx);
    thread_local std::vector<ermia::dia::task<ermia::dbtuple*>> value_fetch_tasks(g_reps_per_tx);
    thread_local std::vector<ermia::dia::coro_task_private::coro_stack> coro_stacks(g_reps_per_tx);
    keys.clear();

    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey();
      keys.emplace_back(&k);
    }

    if (!ermia::config::index_probe_only) {
      values.clear();
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        if (ermia::config::index_probe_only) {
          values.push_back(&str(0));
        } else {
          values.push_back(&str(sizeof(YcsbRecord)));
        }
      }
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    }

    tbl->adv_coro_MultiGet(txn, keys, values,
        index_probe_tasks, value_fetch_tasks, coro_stacks);

    if (!ermia::config::index_probe_only) {
      ermia::varstr &v = str(sizeof(YcsbRecord));
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)values[i]->data(), sizeof(YcsbRecord));
      }
      ALWAYS_ASSERT(*(char*)v.data() == 'a');
    }

    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }

    return {RC_TRUE};
  }

protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }
  ermia::varstr &GenerateKey() {
    uint64_t r = 0;
    if (g_zipfian_rng) {
      r = zipfian_rng.next();
    } else {
      r = uniform_rng.uniform_within(0, g_initial_table_size - 1);
    }

    ermia::varstr &k = str(sizeof(uint64_t));  // 8-byte key
    new (&k) ermia::varstr((char *)&k + sizeof(ermia::varstr), sizeof(uint64_t));
    ::BuildKey(r, k);
    return k;
  }

private:
  ermia::ConcurrentMasstreeIndex *tbl;
  foedus::assorted::UniformRandom uniform_rng;
  foedus::assorted::ZipfianRandom zipfian_rng;
  ermia::transaction *transactions;
};

class ycsb_cs_adv_bench_runner : public bench_runner {
public:
  ycsb_cs_adv_bench_runner(ermia::Engine *db) : bench_runner(db) {
    ycsb_create_db(db);
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::TableDescriptor::GetPrimaryIndex("USERTABLE");
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    uint64_t requested = g_initial_table_size;
    uint64_t records_per_thread = std::max<uint64_t>(1, g_initial_table_size / ermia::config::worker_threads);
    g_initial_table_size = records_per_thread * ermia::config::worker_threads;

    if (ermia::config::verbose) {
      std::cerr << "[INFO] requested for " << requested
           << " records, will load "
           << records_per_thread *ermia::config::worker_threads << std::endl;
    }

    std::vector<bench_loader *> ret;
    for (uint32_t i = 0; i < ermia::config::worker_threads; ++i) {
      ret.push_back(new ycsb_usertable_loader(0, db, open_tables, i));
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_cmdlog_redoers() {
    // Not implemented
    LOG(FATAL) << "Not applicable";
    std::vector<bench_worker *> ret;
    return ret;
  }

  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(8544290);
    std::vector<bench_worker *> ret;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      ret.push_back(new ycsb_cs_adv_worker(i, r.next(), db, open_tables, &barrier_a,
                                           &barrier_b));
    }
    return ret;
  }
};

void ycsb_cs_advance_do_test(ermia::Engine *db, int argc, char **argv) {
  // parse options
  optind = 1;
  while (1) {
    static struct option long_options[] = {
        {"reps-per-tx", required_argument, 0, 'r'},
        {"max-inflight-tx", required_argument, 0, 'i'},
        {"rmw-additional-reads", required_argument, 0, 'a'},
        {"workload", required_argument, 0, 'w'},
        {"initial-table-size", required_argument, 0, 's'},
        {"zipfian", no_argument, &g_zipfian_rng, 1},
        {"zipfian-theta", required_argument, 0, 'z'},
        {"adv-coro-txn-read", no_argument, &g_adv_coro_txn_read, 1},
        {0, 0, 0, 0}};

    int option_index = 0;
    int c = getopt_long(argc, argv, "r:a:w:s:", long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;

    case 'r':
      g_reps_per_tx = strtoul(optarg, NULL, 10);
      break;

    case 'a':
      g_rmw_additional_reads = strtoul(optarg, NULL, 10);
      break;

    case 's':
      g_initial_table_size = strtoul(optarg, NULL, 10);
      break;

    case 'w':
      g_workload = optarg[0];
      if (g_workload == 'A')
        ycsb_workload = YcsbWorkloadA;
      else if (g_workload == 'B')
        ycsb_workload = YcsbWorkloadB;
      else if (g_workload == 'C')
        ycsb_workload = YcsbWorkloadC;
      else if (g_workload == 'D')
        ycsb_workload = YcsbWorkloadD;
      else if (g_workload == 'E')
        ycsb_workload = YcsbWorkloadE;
      else if (g_workload == 'F')
        ycsb_workload = YcsbWorkloadF;
      else if (g_workload == 'G')
        ycsb_workload = YcsbWorkloadG;
      else if (g_workload == 'H')
        ycsb_workload = YcsbWorkloadH;
      else {
        std::cerr << "Wrong workload type: " << g_workload << std::endl;
        abort();
      }
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      abort();
    }
  }

  ALWAYS_ASSERT(g_initial_table_size);

  if (ermia::config::verbose) {
    std::cerr << "ycsb settings:" << std::endl
              << "  workload:                   " << g_workload << std::endl
              << "  initial user table size:    " << g_initial_table_size
              << std::endl
              << "  operations per transaction: " << g_reps_per_tx << std::endl
              << "  additional reads after RMW: " << g_rmw_additional_reads
              << std::endl
              << "  distribution:               "
              << (g_zipfian_rng ? "zipfian" : "uniform")
              << "  adv_coro_txn_read:          " << g_adv_coro_txn_read << std::endl
              << std::endl;

    if (g_zipfian_rng) {
      std::cerr << "  zipfian theta:              " << g_zipfian_theta << std::endl;
    }
  }

  ycsb_cs_adv_bench_runner r(db);
  r.run();
}
#endif  // ADV_COROUTINE
