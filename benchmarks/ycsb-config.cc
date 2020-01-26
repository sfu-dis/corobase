#include "../ermia.h"
#include "bench.h"
#include "ycsb.h"

uint64_t global_key_counter = 0;
uint g_reps_per_tx = 1;
uint g_rmw_additional_reads = 0;
char g_workload = 'C';
uint g_initial_table_size = 30000000;
int g_amac_txn_read = 0;
int g_coro_txn_read = 0;
int g_adv_coro_txn_read = 0;
int g_zipfian_rng = 0;
double g_zipfian_theta = 0.99;  // zipfian constant, [0, 1), more skewed as it approaches 1.
int g_distinct_keys = 0;

// { insert, read, update, scan, rmw }
YcsbWorkload YcsbWorkloadA('A', 0, 50U, 100U, 0, 0);  // Workload A - 50% read, 50% update
YcsbWorkload YcsbWorkloadB('B', 0, 95U, 100U, 0, 0);  // Workload B - 95% read, 5% update
YcsbWorkload YcsbWorkloadC('C', 0, 100U, 0, 0, 0);  // Workload C - 100% read
YcsbWorkload YcsbWorkloadD('D', 5U, 100U, 0, 0, 0);  // Workload D - 95% read, 5% insert
YcsbWorkload YcsbWorkloadE('E', 5U, 0, 0, 100U, 0);  // Workload E - 5% insert, 95% scan

// Combine reps_per_tx and rmw_additional_reads to have "10R+10RMW" style
// transactions.
YcsbWorkload YcsbWorkloadF('F', 0, 0, 0, 0, 100U);  // Workload F - 100% RMW

// Extra workloads (not in spec)
YcsbWorkload YcsbWorkloadG('G', 0, 0, 5U, 100U, 0);  // Workload G - 5% update, 95% scan
YcsbWorkload YcsbWorkloadH('H', 0, 0, 0, 100U, 0);  // Workload H - 100% scan

YcsbWorkload ycsb_workload = YcsbWorkloadC;

void ycsb_create_db(ermia::Engine *db) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(true);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    db->CreateTable("USERTABLE");
    db->CreateMasstreePrimaryIndex("USERTABLE", std::string("USERTABLE"));
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

void ycsb_usertable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("USERTABLE");
  int64_t to_insert = g_initial_table_size / ermia::config::worker_threads;
  uint64_t start_key = loader_id * to_insert;
  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    ermia::varstr &k = str(sizeof(uint64_t));
    BuildKey(start_key + i, k);

    ermia::varstr &v = str(sizeof(YcsbRecord));
    new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(YcsbRecord));
    *(char*)v.p = 'a';

#ifdef USE_STATIC_COROUTINE
    TryVerifyStrict(sync_wait_coro(tbl->InsertRecord(txn, k, v)));
#else
    TryVerifyStrict(tbl->InsertRecord(txn, k, v));
#endif
    TryVerifyStrict(db->Commit(txn));
  }

  // Verify inserted values
  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = 0;
    ermia::varstr &k = str(sizeof(uint64_t));
    BuildKey(start_key + i, k);
    ermia::varstr &v = str(0);
    tbl->GetRecord(txn, rc, k, v, &oid);
    ALWAYS_ASSERT(*(char*)v.data() == 'a');
    TryVerifyStrict(rc);
    TryVerifyStrict(db->Commit(txn));
  }

  if (ermia::config::verbose) {
    std::cerr << "[INFO] loader " << loader_id <<  " loaded "
              << to_insert << " keys in USERTABLE" << std::endl;
  }
}
