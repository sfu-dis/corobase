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

extern uint64_t local_key_counter[ermia::config::MAX_THREADS];
extern uint g_reps_per_tx;
extern uint g_rmw_additional_reads;
extern char g_workload;
extern uint g_initial_table_size;
extern int g_sort_load_keys;

extern YcsbKey *key_arena;

extern util::fast_random rnd_record_select;

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

class ycsb_dia_worker : public bench_worker {
 public:
  ycsb_dia_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
              const std::map<std::string, ermia::OrderedIndex *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        tbl((ermia::DecoupledMasstreeIndex*)open_tables.at("USERTABLE")) {}

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

  static rc_t TxnInsert(bench_worker *w) { MARK_REFERENCED(w); return {RC_TRUE}; }

  static rc_t TxnRead(bench_worker *w) {
    return static_cast<ycsb_dia_worker *>(w)->txn_read();
  }

  static rc_t TxnUpdate(bench_worker *w) { MARK_REFERENCED(w); return {RC_TRUE}; }

  static rc_t TxnScan(bench_worker *w) { MARK_REFERENCED(w); return {RC_TRUE}; }

  static rc_t TxnRMW(bench_worker *w) {
    return static_cast<ycsb_dia_worker *>(w)->txn_rmw();
  }

  // Request result placeholders
  void PrepareForDIA(rc_t **out_rcs, ermia::OID **out_oids) {
    static __thread rc_t *rcs = nullptr;
    static __thread ermia::OID *oids = nullptr;
    uint32_t n = std::max<uint32_t>(g_reps_per_tx, g_rmw_additional_reads);
    if (!rcs) {
      rcs = (rc_t *)malloc(sizeof(rc_t) * n);
      oids = (ermia::OID *)malloc(sizeof(ermia::OID) * n);
    }
    for (uint32_t i = 0; i < n; ++i) {
      rcs[i] = rc_t{RC_INVALID};
      oids[i] = 0;
    }
    *out_rcs = rcs;
    *out_oids = oids;
  }

  rc_t txn_read() {
    ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
    arena.reset();

    static __thread std::vector<ermia::varstr *> *values;
    static __thread std::vector<YcsbKey *> *keys;

    values = new std::vector<ermia::varstr *>();
    keys = new std::vector<YcsbKey *>();

    rc_t *rcs = nullptr;
    ermia::OID *oids = nullptr;
    PrepareForDIA(&rcs, &oids);

    for (uint i = 0; i < g_reps_per_tx; ++i) {
      keys->push_back(&build_rmw_key(worker_id));
      values->push_back(&str(sizeof(YcsbRecord)));
      // TODO(tzwang): add read/write_all_fields knobs
      // FIXME(tzwang): DIA may need to copy the key?
      tbl->SendGet(txn, rcs[i], *(*keys)[i], &oids[i]);  // Send out async Get request
    }

    for (uint32_t i = 0; i < g_reps_per_tx; ++i) {
      tbl->RecvGet(txn, rcs[i], oids[i], *(*values)[i]);
      ALWAYS_ASSERT(*(char*)(*values)[i]->data() == 'a');
      TryCatch(rcs[i]);
      // TODO(tzwang): if we abort here (e.g. because the return value rc says
      // so), it might be beneficial to rescind the subsequent requests that have
      // been sent.
    }
    TryCatch(db->Commit(txn));

    delete values;
    delete keys;
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
    arena.reset();

    static __thread std::vector<ermia::varstr *> *values;
    static __thread std::vector<YcsbKey *> *keys;
    values = new std::vector<ermia::varstr *>();
    keys = new std::vector<YcsbKey *>();

    rc_t *rcs = nullptr;
    ermia::OID *oids = nullptr;
    PrepareForDIA(&rcs, &oids);

    // Issue all reads
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      keys->push_back(&build_rmw_key(worker_id));
      values->push_back(&str(sizeof(ermia::varstr)));
      // TODO(tzwang): add read/write_all_fields knobs
      tbl->SendGet(txn, rcs[i], *(*keys)[i], &oids[i]);
    }

    static __thread std::vector<ermia::varstr *> *new_values;
    new_values = new std::vector<ermia::varstr *>();

    for (uint32_t i = 0; i < g_reps_per_tx; ++i) {
      // Barrier to ensure data is read in
      tbl->RecvGet(txn, rcs[i], oids[i], *(*values)[i]);
      ALWAYS_ASSERT(*(char*)(*values)[i]->data() == 'a');
      TryCatch(rcs[i]);

      // Reset the return value placeholders
      rcs[i]._val = RC_INVALID;
      oids[i] = 0;

      // Copy to user space and do the write
      new_values->push_back(&str(sizeof(ermia::varstr) + sizeof(YcsbRecord)));
      (*new_values)[i]->p = (const uint8_t *)(*new_values)[i] + sizeof(ermia::varstr);
      (*new_values)[i]->l = sizeof(YcsbRecord);
      ASSERT((*values)[i]->size() == sizeof(YcsbRecord));

      memcpy((*new_values)[i]->data(), (*values)[i]->data(), (*values)[i]->size());
      memset((*new_values)[i]->data(), 'a', (*values)[i]->size());
      tbl->SendPut(txn, rcs[i], *(*keys)[i], &oids[i]);  // Modify-write
      // TODO(tzwang): similar to read-only case, see if we can rescind
      // subsequent requests for better performance
    }

    // Wait for writes to finish
    for (uint32_t i = 0; i < g_reps_per_tx; ++i) {
      tbl->RecvPut(txn, rcs[i], oids[i], *(*keys)[i], *(*new_values)[i]);
    }

    PrepareForDIA(&rcs, &oids);
    (*keys).clear();
    (*values).clear();
    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      keys->push_back(&build_rmw_key(worker_id));
      values->push_back(&str(sizeof(YcsbRecord)));
      tbl->SendGet(txn, rcs[i], *(*keys)[i], &oids[i]);
    }

    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      tbl->RecvGet(txn, rcs[i], oids[i], *(*values)[i]);
      TryCatch(rcs[i]);
    }
    TryCatch(db->Commit(txn));

    delete values;
    delete keys;
    delete new_values;
    return {RC_TRUE};
  }

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena.next(size); }

 private:
  ermia::DecoupledMasstreeIndex *tbl;
};

class ycsb_dia_usertable_loader : public bench_loader {
 public:
  ycsb_dia_usertable_loader(unsigned long seed, ermia::Engine *db,
                        const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : bench_loader(seed, db, open_tables) {}

 protected:
  // XXX(tzwang): for now this is serial
  void load() {
    ermia::DecoupledMasstreeIndex *tbl = (ermia::DecoupledMasstreeIndex*)open_tables.at("USERTABLE");
    std::vector<YcsbKey*> keys;
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
    uint64_t inserted = 0;
    for (uint16_t worker_id = 0; worker_id < ermia::config::worker_threads;
         worker_id++) {
      local_key_counter[worker_id] = 0;
      auto remaining_inserts = records_per_thread;
      uint32_t high = worker_id, low = 0;
      while (true) {
        YcsbKey *key = (YcsbKey *)malloc(sizeof(YcsbKey) + sizeof(uint64_t));
        new (key) YcsbKey();
        key->build(high, low++);
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
      ermia::varstr *v = (ermia::varstr *)malloc(sizeof(ermia::varstr) + sizeof(uint64_t));
      new (v) ermia::varstr(r.data_, sizeof(r));
      //ermia::varstr v(r.data_, sizeof(r));
      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      arena.reset();

      //TryVerifyStrict(tbl->Insert(txn, *key, v));
      rc_t rc = rc_t{RC_INVALID};
      ermia::OID oid = 0;
      ermia::dbtuple *tuple = nullptr;
      tbl->SendInsert(txn, rc, *key, *v, &oid, &tuple);
      ASSERT(tuple);
      tbl->RecvInsert(txn, rc, oid, *key, *v, tuple);
      TryVerifyStrict(rc);

      TryVerifyStrict(db->Commit(txn));
      free(key);
      free(v);
    }

    if (ermia::config::verbose)
      std::cerr << "[INFO] loaded " << inserted << " kyes in USERTABLE" << std::endl;
  }
};

class ycsb_dia_bench_runner : public bench_runner {
 public:
  ycsb_dia_bench_runner(ermia::Engine *db) : bench_runner(db) {
    db->CreateMasstreeTable("USERTABLE", true /* decoupled index access */);
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::IndexDescriptor::GetIndex("USERTABLE");
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    std::vector<bench_loader *> ret;
    ret.push_back(new ycsb_dia_usertable_loader(0, db, open_tables));
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
      ret.push_back(new ycsb_dia_worker(i, r.next(), db, open_tables, &barrier_a,
                                    &barrier_b));
    }
    return ret;
  }
};

void ycsb_dia_do_test(ermia::Engine *db, int argc, char **argv) {
  // varstr header followed by the actual key data (uint64_t)
  key_arena = (YcsbKey *)malloc(sizeof(YcsbKey) + sizeof(uint64_t));
  new (key_arena) YcsbKey();
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

  ycsb_dia_bench_runner r(db);
  r.run();
}
