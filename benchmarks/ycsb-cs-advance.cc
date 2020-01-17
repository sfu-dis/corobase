
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
      ermia::RCU::rcu_enter();

      arena.reset();

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

      ermia::RCU::rcu_exit();
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
    if (ycsb_workload.read_percent())
      w.push_back(workload_desc(
          "Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, nullptr, TxnRead));
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

private:
  task<rc_t> txn_read(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;

    if (!ermia::config::index_probe_only) {
        txn = &transactions[idx];
        new (txn) ermia::transaction(
          ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arena);
          ermia::TXN::xid_context * xc = txn->GetXIDContext();
          xc->begin_epoch = begin_epoch;
    }

    for (int j = 0; j < g_reps_per_tx; ++j) {
      ermia::varstr &k = GenerateKey();
      ermia::varstr &v = str(sizeof(YcsbRecord));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      if (!ermia::config::index_probe_only) {
        co_await tbl->Get(txn, rc, k, v);  // Read
      } else {
        ermia::OID oid = 0;
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        rc = (co_await tbl->GetMasstree().search(k, oid, begin_epoch, &sinfo)) ? RC_TRUE : RC_FALSE;
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
          ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arena);
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
      bool found = co_await tbl->GetMasstree().search_coro_1_layer(k, oid, begin_epoch, &sinfo);
      if (!ermia::config::index_probe_only) {
        ermia::dbtuple *tuple = nullptr;
        if (found) {
          tuple = co_await ermia::oidmgr->oid_get_version(tbl->GetMasstree().get_descriptor()->GetTupleArray(),
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

protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena.next(size); }
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

class ycsb_cs_adv_usertable_loader : public bench_loader {
public:
  ycsb_cs_adv_usertable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      uint32_t loader_id)
      : bench_loader(seed, db, open_tables), loader_id(loader_id) {}

private:
  uint32_t loader_id;

protected:
  // XXX(lujc): for now this is serial
  void load() {
    ermia::dia::coro_task_private::memory_pool memory_pool;

    ermia::OrderedIndex *tbl = open_tables.at("USERTABLE");
    int64_t to_insert = g_initial_table_size / ermia::config::worker_threads;
    uint64_t start_key = loader_id * to_insert;

    for (uint64_t i = 0; i < to_insert; ++i) {
      arena.reset();
      ermia::varstr &k = str(sizeof(uint64_t));
      BuildKey(start_key + i, k);

      ermia::varstr &v = str(sizeof(YcsbRecord));
      new (&v)
          ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(YcsbRecord));
      *(char *)v.p = 'a';

      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      TryVerifyStrict(sync_wait_coro(tbl->Insert(txn, k, v)));
      TryVerifyStrict(db->Commit(txn));
    }

    // Verify inserted values
    for (uint64_t i = 0; i < to_insert; ++i) {
      arena.reset();
      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      rc_t rc = rc_t{RC_INVALID};
      ermia::OID oid = 0;
      ermia::varstr &k = str(sizeof(uint64_t));
      BuildKey(start_key + i, k);
      ermia::varstr &v = str(0);

      sync_wait_coro(tbl->Get(txn, rc, k, v, &oid));
      ALWAYS_ASSERT(*(char *)v.data() == 'a');
      TryVerifyStrict(rc);
      TryVerifyStrict(db->Commit(txn));
    }

    if (ermia::config::verbose) {
      std::cerr << "[INFO] loader " << loader_id << " loaded " << to_insert
                << " keys in USERTABLE" << std::endl;
    }
  }
};

class ycsb_cs_adv_bench_runner : public bench_runner {
public:
  ycsb_cs_adv_bench_runner(ermia::Engine *db) : bench_runner(db) {
    db->CreateMasstreeTable("USERTABLE", false);
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::IndexDescriptor::GetIndex("USERTABLE");
  }

protected:
  virtual std::vector<bench_loader *> make_loaders() {
    uint64_t requested = g_initial_table_size;
    uint64_t records_per_thread = std::max<uint64_t>(
        1, g_initial_table_size / ermia::config::worker_threads);
    g_initial_table_size = records_per_thread * ermia::config::worker_threads;

    if (ermia::config::verbose) {
      std::cerr << "[INFO] requested for " << requested
                << " records, will load "
                << records_per_thread * ermia::config::worker_threads
                << std::endl;
    }

    std::vector<bench_loader *> ret;
    for (uint32_t i = 0; i < ermia::config::worker_threads; ++i) {
      ret.push_back(new ycsb_cs_adv_usertable_loader(0, db, open_tables, i));
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
              << std::endl;

    if (g_zipfian_rng) {
      std::cerr << "  zipfian theta:              " << g_zipfian_theta
                << std::endl;
    }
  }

  ycsb_cs_adv_bench_runner r(db);
  r.run();
}
