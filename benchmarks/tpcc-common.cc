#ifndef ADV_COROUTINE

#include <getopt.h>
#include "bench.h"
#include "tpcc-common.h"

// configuration flags
int g_disable_xpartition_txn = 0;
int g_enable_separate_tree_per_partition = 0;
int g_new_order_remote_item_pct = 1;
int g_new_order_fast_id_gen = 0;
int g_uniform_item_dist = 0;
int g_order_status_scan_hack = 0;
int g_wh_temperature = 0;
uint g_microbench_rows = 10;  // this many rows
// can't have both ratio and rows at the same time
int g_microbench_wr_rows = 0;  // this number of rows to write
int g_nr_suppliers = 100;

// TPC-C workload mix
// 0: NewOrder
// 1: Payment
// 2: CreditCheck
// 3: Delivery
// 4: OrderStatus
// 5: StockLevel
// 6: TPC-CH query 2 variant - original query 2, but /w marginal stock table update
// 7: Microbenchmark-random - same as Microbenchmark, but uses random read-set range
unsigned g_txn_workload_mix[8] = {
    45, 43, 0, 4, 4, 4, 0, 0};  // default TPC-C workload mix

// how much % of time a worker should use a random home wh
// 0 - always use home wh
// 50 - 50% of time use random wh
// 100 - always use a random wh
double g_wh_spread = 100;

util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids = nullptr;

SuppStockMap supp_stock_map(10000);  // value ranges 0 ~ 9999 ( modulo by 10k )

const Nation nations[] = {{48, "ALGERIA", 0},
                          {49, "ARGENTINA", 1},
                          {50, "BRAZIL", 1},
                          {51, "CANADA", 1},
                          {52, "EGYPT", 4},
                          {53, "ETHIOPIA", 0},
                          {54, "FRANCE", 3},
                          {55, "GERMANY", 3},
                          {56, "INDIA", 2},
                          {57, "INDONESIA", 2},
                          {65, "IRAN", 4},
                          {66, "IRAQ", 4},
                          {67, "JAPAN", 2},
                          {68, "JORDAN", 4},
                          {69, "KENYA", 0},
                          {70, "MOROCCO", 0},
                          {71, "MOZAMBIQUE", 0},
                          {72, "PERU", 1},
                          {73, "CHINA", 2},
                          {74, "ROMANIA", 3},
                          {75, "SAUDI ARABIA", 4},
                          {76, "VIETNAM", 2},
                          {77, "RUSSIA", 3},
                          {78, "UNITED KINGDOM", 3},
                          {79, "UNITED STATES", 1},
                          {80, "CHINA", 2},
                          {81, "PAKISTAN", 2},
                          {82, "BANGLADESH", 2},
                          {83, "MEXICO", 1},
                          {84, "PHILIPPINES", 2},
                          {85, "THAILAND", 2},
                          {86, "ITALY", 3},
                          {87, "SOUTH AFRICA", 0},
                          {88, "SOUTH KOREA", 2},
                          {89, "COLOMBIA", 1},
                          {90, "SPAIN", 3},
                          {97, "UKRAINE", 3},
                          {98, "POLAND", 3},
                          {99, "SUDAN", 0},
                          {100, "UZBEKISTAN", 2},
                          {101, "MALAYSIA", 2},
                          {102, "VENEZUELA", 1},
                          {103, "NEPAL", 2},
                          {104, "AFGHANISTAN", 2},
                          {105, "NORTH KOREA", 2},
                          {106, "TAIWAN", 2},
                          {107, "GHANA", 0},
                          {108, "IVORY COAST", 0},
                          {109, "SYRIA", 4},
                          {110, "MADAGASCAR", 0},
                          {111, "CAMEROON", 0},
                          {112, "SRI LANKA", 2},
                          {113, "ROMANIA", 3},
                          {114, "NETHERLANDS", 3},
                          {115, "CAMBODIA", 2},
                          {116, "BELGIUM", 3},
                          {117, "GREECE", 3},
                          {118, "PORTUGAL", 3},
                          {119, "ISRAEL", 4},
                          {120, "FINLAND", 3},
                          {121, "SINGAPORE", 2},
                          {122, "NORWAY", 3}};

const char *regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

std::string tpcc_worker_mixin::NameTokens[] = {
    std::string("BAR"),   std::string("OUGHT"), std::string("ABLE"), std::string("PRI"),
    std::string("PRES"),  std::string("ESE"),   std::string("ANTI"), std::string("CALLY"),
    std::string("ATION"), std::string("EING"),
};

std::vector<uint> tpcc_worker_mixin::hot_whs;
std::vector<uint> tpcc_worker_mixin::cold_whs;

template <class WorkerType>
class tpcc_bench_runner : public bench_runner {
 private:
  static bool IsTableReadOnly(const char *name) {
    return strcmp("item", name) == 0;
  }

  static bool IsTableAppendOnly(const char *name) {
    return strcmp("history", name) == 0 || strcmp("oorder_c_id_idx", name) == 0;
  }

  static std::vector<ermia::OrderedIndex *> OpenIndexes(const char *name) {
    const bool is_read_only = IsTableReadOnly(name);
    const bool is_append_only = IsTableAppendOnly(name);
    const std::string s_name(name);
    std::vector<ermia::OrderedIndex *> ret(NumWarehouses());
    if (g_enable_separate_tree_per_partition && !is_read_only) {
      if (NumWarehouses() <= ermia::config::worker_threads) {
        for (size_t i = 0; i < NumWarehouses(); i++) {
          ret[i] = ermia::TableDescriptor::GetIndex(s_name + "_" + std::to_string(i));
          ALWAYS_ASSERT(ret[i]);
        }
      } else {
        const unsigned nwhse_per_partition =
            NumWarehouses() / ermia::config::worker_threads;
        for (size_t partid = 0; partid < ermia::config::worker_threads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend = (partid + 1 == ermia::config::worker_threads)
                                    ? NumWarehouses()
                                    : (partid + 1) * nwhse_per_partition;
          ermia::OrderedIndex *idx =
              ermia::TableDescriptor::GetIndex(s_name + "_" + std::to_string(partid));
          ALWAYS_ASSERT(idx);
          for (size_t i = wstart; i < wend; i++) {
            ret[i] = idx;
          }
        }
      }
    } else {
      ermia::OrderedIndex *idx = ermia::TableDescriptor::GetIndex(s_name);
      ALWAYS_ASSERT(idx);
      for (size_t i = 0; i < NumWarehouses(); i++) {
        ret[i] = idx;
      }
    }
    return ret;
  }

  // Create table and primary index (same name) or a secondary index if
  // primary_idx_name isn't nullptr
  static void RegisterIndex(ermia::Engine *db, const char *table_name,
                            const char *index_name, bool is_primary) {
    const bool is_read_only = IsTableReadOnly(index_name);

    // A labmda function to be executed by an sm-thread
    auto register_index = [=](char *) {
      if (g_enable_separate_tree_per_partition && !is_read_only) {
        if (ermia::config::is_backup_srv() ||
            NumWarehouses() <= ermia::config::worker_threads) {
          for (size_t i = 0; i < NumWarehouses(); i++) {
            if (!is_primary) {
              // Secondary index
              db->CreateMasstreeSecondaryIndex(table_name, std::string(index_name));
            } else {
              db->CreateTable(table_name);
              db->CreateMasstreePrimaryIndex(table_name, std::string(index_name));
            }
          }
        } else {
          const unsigned nwhse_per_partition =
              NumWarehouses() / ermia::config::worker_threads;
          for (size_t partid = 0; partid < ermia::config::worker_threads; partid++) {
            const unsigned wstart = partid * nwhse_per_partition;
            const unsigned wend = (partid + 1 == ermia::config::worker_threads)
                                      ? NumWarehouses()
                                      : (partid + 1) * nwhse_per_partition;
            if (!is_primary) {
              auto s_primary_name = std::string(table_name) + "_" + std::to_string(partid);
              db->CreateMasstreeSecondaryIndex(table_name, s_primary_name);
            } else {
              db->CreateTable(table_name);
              auto ss_name = std::string(table_name) + std::string("_") + std::to_string(partid);
              db->CreateMasstreePrimaryIndex(table_name, ss_name);
            }
          }
        }
      } else {
        if (!is_primary) {
          db->CreateMasstreeSecondaryIndex(table_name, index_name);
        } else {
          db->CreateTable(table_name);
          db->CreateMasstreePrimaryIndex(table_name, std::string(index_name));
        }
      }
    };

    ermia::thread::Thread *thread = ermia::thread::GetThread(true);
    ALWAYS_ASSERT(thread);
    thread->StartTask(register_index);
    thread->Join();
    ermia::thread::PutThread(thread);
  }

 public:
  tpcc_bench_runner(ermia::Engine *db) : bench_runner(db) {
    // Register all tables and indexes with the engine
    RegisterIndex(db, "customer",   "customer",         true);
    RegisterIndex(db, "customer",   "customer_name_idx",         false);
    RegisterIndex(db, "district",   "district",         true);
    RegisterIndex(db, "history",    "history",          true);
    RegisterIndex(db, "item",       "item",             true);
    RegisterIndex(db, "new_order",  "new_order",        true);
    RegisterIndex(db, "oorder",     "oorder",           true);
    RegisterIndex(db, "oorder",     "oorder_c_id_idx",  false);
    RegisterIndex(db, "order_line", "order_line",       true);
    RegisterIndex(db, "stock",      "stock",            true);
    RegisterIndex(db, "stock_data", "stock_data",       true);
    RegisterIndex(db, "nation",     "nation",           true);
    RegisterIndex(db, "region",     "region",           true);
    RegisterIndex(db, "supplier",   "supplier",         true);
    RegisterIndex(db, "warehouse",  "warehouse",        true);
  }

  virtual void prepare(char *) {
#define OPEN_TABLESPACE_X(x) partitions[#x] = OpenIndexes(#x);

    TPCC_TABLE_LIST(OPEN_TABLESPACE_X);

#undef OPEN_TABLESPACE_X

    for (auto &t : partitions) {
      auto v = unique_filter(t.second);
      for (size_t i = 0; i < v.size(); i++)
        open_tables[t.first + "_" + std::to_string(i)] = v[i];
    }

    if (g_new_order_fast_id_gen) {
      void *const px = memalign(
          CACHELINE_SIZE, sizeof(util::aligned_padded_elem<std::atomic<uint64_t>>) *
                              NumWarehouses() * NumDistrictsPerWarehouse());
      g_district_ids =
          reinterpret_cast<util::aligned_padded_elem<std::atomic<uint64_t>> *>(px);
      for (size_t i = 0; i < NumWarehouses() * NumDistrictsPerWarehouse(); i++)
        new (&g_district_ids[i]) std::atomic<uint64_t>(3001);
    }
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    std::vector<bench_loader *> ret;
    ret.push_back(new tpcc_warehouse_loader(9324, db, open_tables, partitions));
    ret.push_back(new tpcc_nation_loader(1512, db, open_tables, partitions));
    ret.push_back(new tpcc_region_loader(789121, db, open_tables, partitions));
    ret.push_back(
        new tpcc_supplier_loader(51271928, db, open_tables, partitions));
    ret.push_back(new tpcc_item_loader(235443, db, open_tables, partitions));
    if (ermia::config::parallel_loading) {
      util::fast_random r(89785943);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.push_back(
            new tpcc_stock_loader(r.next(), db, open_tables, partitions, i));
    } else {
      ret.push_back(
          new tpcc_stock_loader(89785943, db, open_tables, partitions, -1));
    }
    ret.push_back(
        new tpcc_district_loader(129856349, db, open_tables, partitions));
    if (ermia::config::parallel_loading) {
      util::fast_random r(923587856425);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.push_back(
            new tpcc_customer_loader(r.next(), db, open_tables, partitions, i));
    } else {
      ret.push_back(new tpcc_customer_loader(923587856425, db, open_tables,
                                             partitions, -1));
    }
    if (ermia::config::parallel_loading) {
      util::fast_random r(2343352);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.push_back(
            new tpcc_order_loader(r.next(), db, open_tables, partitions, i));
    } else {
      ret.push_back(
          new tpcc_order_loader(2343352, db, open_tables, partitions, -1));
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(23984543);
    std::vector<bench_worker *> ret;
    if (NumWarehouses() <= ermia::config::worker_threads) {
      for (size_t i = 0; i < ermia::config::worker_threads; i++)
        ret.push_back(new WorkerType(i, r.next(), db, open_tables, partitions,
                                    &barrier_a, &barrier_b,
                                    (i % NumWarehouses()) + 1));
    } else {
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        ret.push_back(new WorkerType(i, r.next(), db, open_tables, partitions,
                                     &barrier_a, &barrier_b, i + 1));
      }
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_cmdlog_redoers() {
    std::vector<bench_worker *> ret;
    /*
    ALWAYS_ASSERT(ermia::config::is_backup_srv() && ermia::config::command_log);
    util::fast_random r(23984543);
    for (size_t i = 0; i < ermia::config::replay_threads; i++) {
      ret.push_back(new tpcc_cmdlog_redoer(i, r.next(), db, open_tables, partitions));
    }
    */
    return ret;
  }

 private:
  std::map<std::string, std::vector<ermia::OrderedIndex *>> partitions;
};




void tpcc_do_test(ermia::Engine *db, int argc, char **argv) {
  // parse options
  optind = 1;
  bool did_spec_remote_pct = false;
  while (1) {
    static struct option long_options[] = {
        {"disable-cross-partition-transactions", no_argument,
         &g_disable_xpartition_txn, 1},
        {"enable-separate-tree-per-partition", no_argument,
         &g_enable_separate_tree_per_partition, 1},
        {"new-order-remote-item-pct", required_argument, 0, 'r'},
        {"new-order-fast-id-gen", no_argument, &g_new_order_fast_id_gen, 1},
        {"uniform-item-dist", no_argument, &g_uniform_item_dist, 1},
        {"order-status-scan-hack", no_argument, &g_order_status_scan_hack, 1},
        {"workload-mix", required_argument, 0, 'w'},
        {"warehouse-spread", required_argument, 0, 's'},
        {"80-20-dist", no_argument, &g_wh_temperature, 't'},
        {"microbench-rows", required_argument, 0, 'n'},
        {"microbench-wr-ratio", required_argument, 0, 'p'},
        {"microbench-wr-rows", required_argument, 0, 'q'},
        {"suppliers", required_argument, 0, 'z'},
        {0, 0, 0, 0}};
    int option_index = 0;
    int c =
        getopt_long(argc, argv, "r:w:s:t:n:p:q:z", long_options, &option_index);
    if (c == -1) break;
    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
        break;

      case 's':
        g_wh_spread = strtoul(optarg, NULL, 10) / 100.00;
        break;

      case 'n':
        g_microbench_rows = strtoul(optarg, NULL, 10);
        ALWAYS_ASSERT(g_microbench_rows > 0);
        break;

      case 'q':
        g_microbench_wr_rows = strtoul(optarg, NULL, 10);
        break;

      case 'r':
        g_new_order_remote_item_pct = strtoul(optarg, NULL, 10);
        ALWAYS_ASSERT(g_new_order_remote_item_pct >= 0 &&
                      g_new_order_remote_item_pct <= 100);
        did_spec_remote_pct = true;
        break;

      case 'w': {
        const std::vector<std::string> toks = util::split(optarg, ',');
        ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
        unsigned s = 0;
        for (size_t i = 0; i < toks.size(); i++) {
          unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
          ALWAYS_ASSERT(p >= 0 && p <= 100);
          s += p;
          g_txn_workload_mix[i] = p;
        }
        ALWAYS_ASSERT(s == 100);
      } break;
      case 'z':
        g_nr_suppliers = strtoul(optarg, NULL, 10);
        ALWAYS_ASSERT(g_nr_suppliers > 0);
        break;

      case '?':
        /* getopt_long already printed an error message. */
        exit(1);

      default:
        abort();
    }
  }

  if (did_spec_remote_pct && g_disable_xpartition_txn) {
    std::cerr << "WARNING: --new-order-remote-item-pct given with "
            "--disable-cross-partition-transactions" << std::endl;
    std::cerr << "  --new-order-remote-item-pct will have no effect" << std::endl;
  }

  if (g_wh_temperature) {
    // set up hot and cold WHs
    ALWAYS_ASSERT(NumWarehouses() * 0.2 >= 1);
    uint num_hot_whs = NumWarehouses() * 0.2;
    util::fast_random r(23984543);
    for (uint i = 1; i <= num_hot_whs; i++) {
    try_push:
      uint w = r.next() % NumWarehouses() + 1;
      if (find(tpcc_worker::hot_whs.begin(), tpcc_worker::hot_whs.end(), w) ==
          tpcc_worker::hot_whs.end())
        tpcc_worker::hot_whs.push_back(w);
      else
        goto try_push;
    }

    for (uint i = 1; i <= NumWarehouses(); i++) {
      if (find(tpcc_worker::hot_whs.begin(), tpcc_worker::hot_whs.end(), i) ==
          tpcc_worker::hot_whs.end())
        tpcc_worker::cold_whs.push_back(i);
    }
    ALWAYS_ASSERT(tpcc_worker::cold_whs.size() + tpcc_worker::hot_whs.size() ==
                  NumWarehouses());
  }

  if (ermia::config::verbose) {
    std::cerr << "tpcc settings:" << std::endl;
    if (g_wh_temperature) {
      std::cerr << "  hot whs for 80% accesses     :";
      for (uint i = 0; i < tpcc_worker::hot_whs.size(); i++)
        std::cerr << " " << tpcc_worker::hot_whs[i];
      std::cerr << std::endl;
    } else {
      std::cerr << "  random home warehouse (%)    : " << g_wh_spread * 100 << std::endl;
    }
    std::cerr << "  cross_partition_transactions : " << !g_disable_xpartition_txn
         << std::endl;
    std::cerr << "  separate_tree_per_partition  : "
         << g_enable_separate_tree_per_partition << std::endl;
    std::cerr << "  new_order_remote_item_pct    : " << g_new_order_remote_item_pct
         << std::endl;
    std::cerr << "  new_order_fast_id_gen        : " << g_new_order_fast_id_gen
         << std::endl;
    std::cerr << "  uniform_item_dist            : " << g_uniform_item_dist << std::endl;
    std::cerr << "  order_status_scan_hack       : " << g_order_status_scan_hack
         << std::endl;
    std::cerr << "  microbench rows            : " << g_microbench_rows << std::endl;
    std::cerr << "  microbench wr ratio (%)    : "
         << g_microbench_wr_rows / g_microbench_rows << std::endl;
    std::cerr << "  microbench wr rows         : " << g_microbench_wr_rows << std::endl;
    std::cerr << "  number of suppliers : " << g_nr_suppliers << std::endl;
    std::cerr << "  workload_mix                 : "
         << util::format_list(g_txn_workload_mix,
                        g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix))
         << std::endl;
  }

  if (ermia::config::coro_tx) {
    tpcc_bench_runner<tpcc_cs_worker> r(db);
    r.run();
  } else {
    tpcc_bench_runner<tpcc_worker> r(db);
    r.run();
  }
}
#endif // ADV_COROUTINE
