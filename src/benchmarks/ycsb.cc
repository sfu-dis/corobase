/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <set>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <numa.h>

#include "../macros.h"
#include "../util.h"
#include "../spinbarrier.h"

#include "bench.h"
#include "ycsb.h"

using namespace std;
using namespace util;

YcsbRecord::YcsbRecord(char value) {
  memset(data_, value, kFields * kFieldLength * sizeof(char));
}

void YcsbRecord::initialize_field(char *field) {
  memset(field, 'a', kFieldLength);
}

uint64_t local_key_counter[config::MAX_THREADS];

uint g_reps_per_tx = 1;
uint g_rmw_additional_reads = 0;
char g_workload = 'F';
uint g_initial_table_size = 10000;
int g_sort_load_keys = 0;

// { insert, read, update, scan, rmw }
YcsbWorkload YcsbWorkloadA('A', 0, 50U, 100U, 0,
                           0);  // Workload A - 50% read, 50% update
YcsbWorkload YcsbWorkloadB('B', 0, 95U, 100U, 0,
                           0);  // Workload B - 95% read, 5% update
YcsbWorkload YcsbWorkloadC('C', 0, 100U, 0, 0, 0);  // Workload C - 100% read
YcsbWorkload YcsbWorkloadD('D', 5U, 100U, 0, 0,
                           0);  // Workload D - 95% read, 5% insert
YcsbWorkload YcsbWorkloadE('E', 5U, 0, 0, 100U,
                           0);  // Workload E - 5% insert, 95% scan

// Combine reps_per_tx and rmw_additional_reads to have "10R+10RMW" style
// transactions.
YcsbWorkload YcsbWorkloadF('F', 0, 0, 0, 0, 100U);  // Workload F - 100% RMW

// Extra workloads (not in spec)
YcsbWorkload YcsbWorkloadG('G', 0, 0, 5U, 100U,
                           0);  // Workload G - 5% update, 95% scan
YcsbWorkload YcsbWorkloadH('H', 0, 0, 0, 100U, 0);  // Workload H - 100% scan

YcsbWorkload workload = YcsbWorkloadF;

fast_random rnd_record_select(477377);

YcsbKey key_arena;
YcsbKey &build_rmw_key(int worker_id) {
  uint64_t key_seq = rnd_record_select.next_uniform() * g_initial_table_size;
  auto cnt = local_key_counter[worker_id];
  if (cnt == 0) {
    cnt = local_key_counter[0];
  }
  auto hi = key_seq / cnt;
  auto lo = key_seq % cnt;
  key_arena.build(hi, lo);
  return key_arena;
}

class ycsb_worker : public bench_worker {
 public:
  ycsb_worker(unsigned int worker_id, unsigned long seed, ndb_wrapper *db,
              const map<string, OrderedIndex *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b),
        tbl(open_tables.at("USERTABLE")) {}

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;
    if (workload.insert_percent())
      w.push_back(workload_desc(
          "Insert", double(workload.insert_percent()) / 100.0, TxnInsert));
    if (workload.read_percent())
      w.push_back(workload_desc("Read", double(workload.read_percent()) / 100.0,
                                TxnRead));
    if (workload.update_percent())
      w.push_back(workload_desc(
          "Update", double(workload.update_percent()) / 100.0, TxnUpdate));
    if (workload.scan_percent())
      w.push_back(workload_desc("Scan", double(workload.scan_percent()) / 100.0,
                                TxnScan));
    if (workload.rmw_percent())
      w.push_back(
          workload_desc("RMW", double(workload.rmw_percent()) / 100.0, TxnRMW));
    return w;
  }

  static rc_t TxnInsert(bench_worker *w) { return {RC_TRUE}; }

  static rc_t TxnRead(bench_worker *w) { return {RC_TRUE}; }

  static rc_t TxnUpdate(bench_worker *w) { return {RC_TRUE}; }

  static rc_t TxnScan(bench_worker *w) { return {RC_TRUE}; }

  static rc_t TxnRMW(bench_worker *w) {
    return static_cast<ycsb_worker *>(w)->txn_rmw();
  }

  rc_t txn_rmw() {
    transaction *txn = db->new_txn(0, arena, txn_buf());
    arena.reset();
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &key = build_rmw_key(worker_id);
      varstr k((char *)&key.data_, sizeof(key));
      varstr v = str(sizeof(YcsbRecord));
      // TODO(tzwang): add read/write_all_fields knobs
      try_catch(tbl->get(txn, k, v));  // Read
      memset(v.data(), 'a', v.size());
      ASSERT(v.size() == sizeof(YcsbRecord));
      try_catch(tbl->put(txn, k, v));  // Modify-write
    }

    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      auto &key = build_rmw_key(worker_id);
      varstr k((char *)&key.data_, sizeof(key));
      varstr v = str(sizeof(YcsbRecord));
      // TODO(tzwang): add read/write_all_fields knobs
      try_catch(tbl->get(txn, k, v));  // Read
    }
    try_catch(db->commit_txn(txn));
    return {RC_TRUE};
  }

 protected:
  inline ALWAYS_INLINE varstr &str(uint64_t size) { return *arena.next(size); }

 private:
  OrderedIndex *tbl;
};

class ycsb_usertable_loader : public bench_loader {
 public:
  ycsb_usertable_loader(unsigned long seed, ndb_wrapper *db,
                        const map<string, OrderedIndex *> &open_tables)
      : bench_loader(seed, db, open_tables) {}

 protected:
  // XXX(tzwang): for now this is serial
  void load() {
    OrderedIndex *tbl = open_tables.at("USERTABLE");
    std::vector<YcsbKey> keys;
    uint64_t records_per_thread = g_initial_table_size / config::worker_threads;
    bool spread = true;
    if (records_per_thread == 0) {
      // Let one thread load all the keys if we don't have at least one record
      // per **worker** thread
      records_per_thread = g_initial_table_size;
      spread = false;
    } else {
      g_initial_table_size = records_per_thread * config::worker_threads;
    }

    if (config::verbose) {
      cerr << "[INFO] requested for " << g_initial_table_size
           << " records, will load "
           << records_per_thread *config::worker_threads << endl;
    }

    // insert an equal number of records on behalf of each worker
    YcsbKey key;
    uint64_t inserted = 0;
    for (uint16_t worker_id = 0; worker_id < config::worker_threads;
         worker_id++) {
      local_key_counter[worker_id] = 0;
      auto remaining_inserts = records_per_thread;
      uint32_t high = worker_id, low = 0;
      while (true) {
        key.build(high, low++);
        keys.push_back(key);
        inserted++;
        local_key_counter[worker_id]++;
        if (--remaining_inserts == 0) break;
      }
      if (not spread)  // do it on behalf of only one worker
        break;
    }

    ALWAYS_ASSERT(keys.size());
    if (g_sort_load_keys) std::sort(keys.begin(), keys.end());

    // start a transaction and insert all the records
    for (auto &key : keys) {
      YcsbRecord r('a');
      varstr k((char *)&key.data_, sizeof(key));
      varstr v(r.data_, sizeof(r));
      transaction *txn = db->new_txn(0, arena, txn_buf());
      arena.reset();
      try_verify_strict(tbl->insert(txn, k, v));
      try_verify_strict(db->commit_txn(txn));
    }

    if (config::verbose)
      cerr << "[INFO] loaded " << inserted << " kyes in USERTABLE" << endl;
  }
};

class ycsb_bench_runner : public bench_runner {
 public:
  ycsb_bench_runner(ndb_wrapper *db) : bench_runner(db) {
    IndexDescriptor::New("USERTABLE");
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = IndexDescriptor::GetIndex("USERTABLE");
  }

 protected:
  virtual vector<bench_loader *> make_loaders() {
    vector<bench_loader *> ret;
    ret.push_back(new ycsb_usertable_loader(0, db, open_tables));
    return ret;
  }

  virtual vector<bench_worker *> make_workers() {
    fast_random r(8544290);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < config::worker_threads; i++) {
      ret.push_back(new ycsb_worker(i, r.next(), db, open_tables, &barrier_a,
                                    &barrier_b));
    }
    return ret;
  }
};

void ycsb_do_test(ndb_wrapper *db, int argc, char **argv) {
  // parse options
  optind = 1;
  while (1) {
    static struct option long_options[] = {
        {"reps-per-tx", required_argument, 0, 'r'},
        {"rmw-additional-reads", required_argument, 0, 'a'},
        {"workload", required_argument, 0, 'w'},
        {"initial-table-size", required_argument, 0, 's'},
        {"sort-load-keys", no_argument, &g_sort_load_keys, 1},
        {0, 0, 0, 0}};

    int option_index = 0;
    int c = getopt_long(argc, argv, "r:a:w:s:", long_options, &option_index);
    if (c == -1) break;
    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
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
          workload = YcsbWorkloadA;
        else if (g_workload == 'B')
          workload = YcsbWorkloadB;
        else if (g_workload == 'C')
          workload = YcsbWorkloadC;
        else if (g_workload == 'D')
          workload = YcsbWorkloadD;
        else if (g_workload == 'E')
          workload = YcsbWorkloadE;
        else if (g_workload == 'F')
          workload = YcsbWorkloadF;
        else if (g_workload == 'G')
          workload = YcsbWorkloadG;
        else if (g_workload == 'H')
          workload = YcsbWorkloadH;
        else {
          cerr << "Wrong workload type: " << g_workload << endl;
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

  if (config::verbose) {
    cerr << "ycsb settings:" << endl
         << "  workload:                   " << g_workload << endl
         << "  initial user table size:    " << g_initial_table_size << endl
         << "  operations per transaction: " << g_reps_per_tx << endl
         << "  additional reads after RMW: " << g_rmw_additional_reads << endl
         << "  sort load keys:             " << g_sort_load_keys << endl;
  }

  ycsb_bench_runner r(db);
  r.run();
}
