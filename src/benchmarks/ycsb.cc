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

#include "bench.h"
#include "ycsb.h"

YcsbRecord::YcsbRecord(char value) {
  memset(data_, value, kFields * kFieldLength * sizeof(char));
}

void YcsbRecord::initialize_field(char *field) {
  memset(field, 'a', kFieldLength);
}

uint64_t local_key_counter[ermia::config::MAX_THREADS];

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

YcsbWorkload ycsb_workload = YcsbWorkloadF;

util::fast_random rnd_record_select(477377);

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
  ycsb_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
              const std::map<std::string, ermia::OrderedIndex *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        tbl(open_tables.at("USERTABLE")) {}

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const {
    LOG(FATAL) << "Not applicable";
  }
  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;
    if (ycsb_workload.insert_percent())
      w.push_back(workload_desc(
          "Insert", double(ycsb_workload.insert_percent()) / 100.0, TxnInsert));
    if (ycsb_workload.read_percent())
      w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0,
                                TxnRead));
    if (ycsb_workload.update_percent())
      w.push_back(workload_desc(
          "Update", double(ycsb_workload.update_percent()) / 100.0, TxnUpdate));
    if (ycsb_workload.scan_percent())
      w.push_back(workload_desc("Scan", double(ycsb_workload.scan_percent()) / 100.0,
                                TxnScan));
    if (ycsb_workload.rmw_percent())
      w.push_back(
          workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, TxnRMW));
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
    ermia::transaction *txn = db->new_txn(0, arena, txn_buf());
    arena.reset();
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &key = build_rmw_key(worker_id);
      ermia::varstr k((char *)&key.data_, sizeof(key));
      ermia::varstr v = str(sizeof(YcsbRecord));
      // TODO(tzwang): add read/write_all_fields knobs
      try_catch(tbl->Get(txn, k, v));  // Read
      memset(v.data(), 'a', v.size());
      ASSERT(v.size() == sizeof(YcsbRecord));
      try_catch(tbl->Put(txn, k, v));  // Modify-write
    }

    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      auto &key = build_rmw_key(worker_id);
      ermia::varstr k((char *)&key.data_, sizeof(key));
      ermia::varstr v = str(sizeof(YcsbRecord));
      // TODO(tzwang): add read/write_all_fields knobs
      try_catch(tbl->Get(txn, k, v));  // Read
    }
    try_catch(db->commit_txn(txn));
    return {RC_TRUE};
  }

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena.next(size); }

 private:
  ermia::OrderedIndex *tbl;
};

class ycsb_usertable_loader : public bench_loader {
 public:
  ycsb_usertable_loader(unsigned long seed, ermia::Engine *db,
                        const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : bench_loader(seed, db, open_tables) {}

 protected:
  // XXX(tzwang): for now this is serial
  void load() {
    ermia::OrderedIndex *tbl = open_tables.at("USERTABLE");
    std::vector<YcsbKey> keys;
    uint64_t records_per_thread = g_initial_table_size / ermia::config::worker_threads;
    bool spread = true;
    if (records_per_thread == 0) {
      // Let one thread load all the keys if we don't have at least one record
      // per **worker** thread
      records_per_thread = g_initial_table_size;
      spread = false;
    } else {
      g_initial_table_size = records_per_thread * ermia::config::worker_threads;
    }

    if (ermia::config::verbose) {
      std::cerr << "[INFO] requested for " << g_initial_table_size
           << " records, will load "
           << records_per_thread *ermia::config::worker_threads << std::endl;
    }

    // insert an equal number of records on behalf of each worker
    YcsbKey key;
    uint64_t inserted = 0;
    for (uint16_t worker_id = 0; worker_id < ermia::config::worker_threads;
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
      ermia::varstr k((char *)&key.data_, sizeof(key));
      ermia::varstr v(r.data_, sizeof(r));
      ermia::transaction *txn = db->new_txn(0, arena, txn_buf());
      arena.reset();
      try_verify_strict(tbl->Insert(txn, k, v));
      try_verify_strict(db->commit_txn(txn));
    }

    if (ermia::config::verbose)
      std::cerr << "[INFO] loaded " << inserted << " kyes in USERTABLE" << std::endl;
  }
};

class ycsb_bench_runner : public bench_runner {
 public:
  ycsb_bench_runner(ermia::Engine *db) : bench_runner(db) {
    db->CreateMasstreeTable("USERTABLE");
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::IndexDescriptor::GetIndex("USERTABLE");
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    std::vector<bench_loader *> ret;
    ret.push_back(new ycsb_usertable_loader(0, db, open_tables));
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
      ret.push_back(new ycsb_worker(i, r.next(), db, open_tables, &barrier_a,
                                    &barrier_b));
    }
    return ret;
  }
};

void ycsb_do_test(ermia::Engine *db, int argc, char **argv) {
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
         << "  initial user table size:    " << g_initial_table_size << std::endl
         << "  operations per transaction: " << g_reps_per_tx << std::endl
         << "  additional reads after RMW: " << g_rmw_additional_reads << std::endl
         << "  sort load keys:             " << g_sort_load_keys << std::endl;
  }

  ycsb_bench_runner r(db);
  r.run();
}
