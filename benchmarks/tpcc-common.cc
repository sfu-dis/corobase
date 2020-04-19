#ifndef ADV_COROUTINE

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
int g_nr_suppliers = 10000;

// how much % of time a worker should use a random home wh
// 0 - always use home wh
// 50 - 50% of time use random wh
// 100 - always use a random wh
double g_wh_spread = 0;

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

#endif // ADV_COROUTINE
