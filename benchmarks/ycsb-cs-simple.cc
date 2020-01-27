/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <numa.h>
#include <stdlib.h>
#include <unistd.h>

#include "../dbcore/sm-log.h"
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

class ycsb_cs_worker : public bench_worker {
public:
  ycsb_cs_worker(
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
    tx_arena = (ermia::str_arena *)malloc(sizeof(ermia::str_arena) * ermia::config::coro_batch_size);
    for (uint32_t i = 0; i < ermia::config::coro_batch_size; ++i) {
      new (&tx_arena[i]) ermia::str_arena(ermia::config::arena_size_mb);;
    }
  }

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

        // Note: don't change this to 'else...' - we may change h in the prevous if (h)
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

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const override {
    LOG(FATAL) << "Not applicable";
  }
  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent())
      w.push_back(workload_desc(
          "Insert", double(ycsb_workload.insert_percent()) / 100.0, nullptr));
    if (ycsb_workload.read_percent())
      w.push_back(workload_desc(
          "Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, TxnRead));
    if (ycsb_workload.update_percent())
      w.push_back(workload_desc(
          "Update", double(ycsb_workload.update_percent()) / 100.0, nullptr));
    if (ycsb_workload.scan_percent())
      w.push_back(workload_desc(
          "Scan", double(ycsb_workload.scan_percent()) / 100.0, nullptr));
    if (ycsb_workload.rmw_percent())
      w.push_back(workload_desc(
          "RMW", double(ycsb_workload.rmw_percent()) / 100.0, nullptr, TxnRMW));

    return w;
  }

  static SimpleCoroHandle TxnRead(bench_worker *w, uint32_t idx) {
    return static_cast<ycsb_cs_worker *>(w)->txn_read(idx);
  }

  static SimpleCoroHandle TxnRMW(bench_worker *w, uint32_t) {
    return static_cast<ycsb_cs_worker *>(w)->txn_rmw();
  }

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
    return tbl->GetMasstree().ycsb_read_coro(txn, keys[idx], ti, nullptr).get_handle();
  }

  SimpleCoroHandle txn_rmw() {
    return nullptr;
  }

protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }

  ermia::varstr &GenerateKey(ermia::transaction *t) {
    uint64_t r = 0;
    if (g_zipfian_rng) {
      r = zipfian_rng.next();
    } else {
      r = uniform_rng.uniform_within(0, g_initial_table_size - 1);
    }

    ermia::varstr &k = *t->string_allocator().next(sizeof(uint64_t)); // 8-byte key
    new (&k)
        ermia::varstr((char *)&k + sizeof(ermia::varstr), sizeof(uint64_t));
    ::BuildKey(r, k);
    return k;
  }

private:
  ermia::ConcurrentMasstreeIndex *tbl;
  foedus::assorted::UniformRandom uniform_rng;
  foedus::assorted::ZipfianRandom zipfian_rng;
  std::vector<ermia::varstr *> *keys;
  std::vector<ermia::varstr *> values;
  ermia::str_arena *tx_arena;
};

class ycsb_cs_bench_runner : public bench_runner {
public:
  ycsb_cs_bench_runner(ermia::Engine *db) : bench_runner(db) {
    ycsb_create_db(db);
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::TableDescriptor::GetIndex("USERTABLE");
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
      ret.push_back(new ycsb_usertable_loader(0, db, open_tables, i));
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_cmdlog_redoers() {
    // Not implemented
    std::vector<bench_worker *> ret;
    return ret;
  }
  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(8544290);
    std::vector<bench_worker *> ret;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      ret.push_back(new ycsb_cs_worker(i, r.next(), db, open_tables, &barrier_a,
                                       &barrier_b));
    }
    return ret;
  }
};

void ycsb_cs_simple_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_cs_bench_runner r(db);
  r.run();
}
