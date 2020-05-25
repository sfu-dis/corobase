#include <getopt.h>
#include "../ermia.h"
#include "bench.h"
#include "ycsb.h"

uint g_reps_per_tx = 1;
uint g_rmw_additional_reads = 0;
char g_workload = 'C';
uint g_initial_table_size = 30000000;
int g_zipfian_rng = 0;
double g_zipfian_theta = 0.99;  // zipfian constant, [0, 1), more skewed as it approaches 1.


// TODO: support scan_min length, current zipfain rng does not support min bound.
const int g_scan_min_length = 1;
int g_scan_max_length = 1000;
int g_scan_length_zipfain_rng = 0;
double g_scan_length_zipfain_theta = 0.99;

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
  uint32_t nloaders = std::thread::hardware_concurrency() / (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;
  int64_t to_insert = g_initial_table_size / nloaders;
  uint64_t start_key = loader_id * to_insert;
  uint64_t kBatchSize = 50;

  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(start_key + i, k);

    ermia::varstr &v = str(sizeof(ycsb_kv::value));
    new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
    *(char*)v.p = 'a';

#ifdef ADV_COROUTINE
    TryVerifyStrict(sync_wait_coro(tbl->InsertRecord(txn, k, v)));
#else
    TryVerifyStrict(tbl->InsertRecord(txn, k, v));
#endif

    if ((i + 1) % kBatchSize == 0 || i == to_insert - 1) {
      TryVerifyStrict(db->Commit(txn));
      if (i != to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }

  // Verify inserted values
  txn = db->NewTransaction(0, *arena, txn_buf());
  for (uint64_t i = 0; i < to_insert; ++i) {
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(start_key + i, k);
    ermia::varstr &v = str(0);
#ifdef ADV_COROUTINE
    sync_wait_coro(tbl->GetRecord(txn, rc, k, v));
#else
    tbl->GetRecord(txn, rc, k, v);
#endif
    ALWAYS_ASSERT(*(char*)v.data() == 'a');
    TryVerifyStrict(rc);

    if ((i + 1) % kBatchSize == 0 || i == to_insert - 1) {
      TryVerifyStrict(db->Commit(txn));
      if (i != to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
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
        {"scan-range", required_argument, 0, 'g'},
        {0, 0, 0, 0}};

    int option_index = 0;
    int c = getopt_long(argc, argv, "r:a:w:s:z:t:g:", long_options, &option_index);
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
        break;

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

      case 'g':
        g_scan_max_length = strtoul(optarg, NULL, 10);
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
    if (ycsb_workload.scan_percent() > 0) {
      if (g_scan_max_length <= g_scan_min_length || g_scan_min_length < 1) {
         std::cerr << "  invalid scan range:      " << std::endl;
         std::cerr << "  min :                    " << g_scan_min_length << std::endl;
         std::cerr << "  max :                    " << g_scan_max_length << std::endl;
      }
      std::cerr << "  scan maximal range:         " << g_scan_max_length << std::endl;
    }
  }
}
