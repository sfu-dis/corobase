#include <getopt.h>
#include "../ermia.h"
#include "bench.h"
#include "ycsb.h"

uint64_t global_key_counter = 0;
uint g_reps_per_tx = 1;
uint g_rmw_additional_reads = 0;
char g_workload = 'C';
uint g_initial_table_size = 30000000;
int g_zipfian_rng = 0;
double g_zipfian_theta = 0.99;  // zipfian constant, [0, 1), more skewed as it approaches 1.

ReadTransactionType g_read_txn_type = ReadTransactionType::Sequential;

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
  ermia::dia::coro_task_private::memory_pool memory_pool;
  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    ermia::varstr &k = str(sizeof(uint64_t));
    BuildKey(start_key + i, k);

    ermia::varstr &v = str(sizeof(ycsb_kv::value));
    new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
    *(char*)v.p = 'a';

#ifdef ADV_COROUTINE
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
#ifdef ADV_COROUTINE
    sync_wait_coro(tbl->GetRecord(txn, rc, k, v, &oid));
#else
    tbl->GetRecord(txn, rc, k, v, &oid);
#endif
    ALWAYS_ASSERT(*(char*)v.data() == 'a');
    TryVerifyStrict(rc);
    TryVerifyStrict(db->Commit(txn));
  }

  if (ermia::config::verbose) {
    std::cerr << "[INFO] loader " << loader_id <<  " loaded "
              << to_insert << " keys in USERTABLE" << std::endl;
  }
}

void ycsb_parse_options(int argc, char **argv) {
  // parse options
  optind = 1;
  while (1) {
    static struct option long_options[] = {
        {"reps-per-tx", required_argument, 0, 'r'},
        {"rmw-additional-reads", required_argument, 0, 'a'},
        {"workload", required_argument, 0, 'w'},
        {"initial-table-size", required_argument, 0, 's'},
        {"zipfian", no_argument, &g_zipfian_rng, 1},
        {"zipfian-theta", required_argument, 0, 'z'},
        {"read-tx-type", required_argument, 0, 't'},
        {0, 0, 0, 0}};

    int option_index = 0;
    int c = getopt_long(argc, argv, "r:a:w:s:z:t:", long_options, &option_index);
    if (c == -1) break;
    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
        break;

      case 't':
        if (std::string(optarg) == "sequential") {
          g_read_txn_type = ReadTransactionType::Sequential;
        } else if (std::string(optarg) == "adv-coro") {
          g_read_txn_type = ReadTransactionType::AdvCoro;
        } else if (std::string(optarg) == "simple-coro") {
          g_read_txn_type = ReadTransactionType::SimpleCoro;
        } else if (std::string(optarg) == "multiget-simple-coro") {
          g_read_txn_type = ReadTransactionType::SimpleCoroMultiGet;
        } else if (std::string(optarg) == "multiget-adv-coro") {
          g_read_txn_type = ReadTransactionType::AdvCoroMultiGet;
        } else if (std::string(optarg) == "multiget-amac") {
          g_read_txn_type = ReadTransactionType::AMACMultiGet;
        } else {
          LOG(FATAL) << "Wrong read transaction type " << std::string(optarg);
        }

      case 'z':
        g_zipfian_theta = strtod(optarg, NULL);
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
         << "  distribution:               " << (g_zipfian_rng ? "zipfian" : "uniform") << std::endl;

    if (g_read_txn_type == ReadTransactionType::Sequential) {
      std::cerr << "  read transaction type:      sequential" << std::endl;
    } else if (g_read_txn_type == ReadTransactionType::AMACMultiGet) {
      std::cerr << "  read transaction type:      amac multi-get" << std::endl;
    } else if (g_read_txn_type == ReadTransactionType::SimpleCoroMultiGet) {
      std::cerr << "  read transaction type:      simple coroutine multi-get" << std::endl;
    } else if (g_read_txn_type == ReadTransactionType::AdvCoroMultiGet) {
      std::cerr << "  read transaction type:      advanced coroutine multi-get" << std::endl;
    } else if (g_read_txn_type == ReadTransactionType::AdvCoro) {
      std::cerr << "  read transaction type:      advanced coroutine" << std::endl;
    } else if (g_read_txn_type == ReadTransactionType::SimpleCoro) {
      std::cerr << "  read transaction type:      simple coroutine" << std::endl;
    } else {
      abort();
    }

    if (g_zipfian_rng) {
      std::cerr << "  zipfian theta:              " << g_zipfian_theta << std::endl;
    }
  }
}
