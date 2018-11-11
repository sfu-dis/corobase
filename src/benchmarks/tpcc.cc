/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>

#include "../dbcore/sm-cmd-log.h"

#include "bench.h"
#include "tpcc.h"

typedef std::vector<std::vector<std::pair<int32_t, int32_t>>> SuppStockMap;
SuppStockMap supp_stock_map(10000);  // value ranges 0 ~ 9999 ( modulo by 10k )

struct eqstr {
  bool operator()(const char *s1, const char *s2) const {
    return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
  }
};

#define TPCC_TABLE_LIST(x)                                                     \
  x(customer) x(customer_name_idx) x(district) x(history) x(item) x(new_order) \
      x(oorder) x(oorder_c_id_idx) x(order_line) x(stock) x(stock_data)        \
          x(nation) x(region) x(supplier) x(warehouse)

struct Nation {
  int id;
  std::string name;
  int rId;
};

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

class tpcc_table_scanner : public ermia::OrderedIndex::ScanCallback {
 public:
  tpcc_table_scanner(ermia::str_arena *arena) : _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    ermia::varstr *const k = _arena->next(keylen);
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k, &value);
    return true;
  }

  void clear() { output.clear(); }
  std::vector<std::pair<ermia::varstr *, const ermia::varstr *>> output;
  ermia::str_arena *_arena;
};

static ALWAYS_INLINE size_t NumWarehouses() {
  return (size_t)ermia::config::benchmark_scale_factor;
}

// config constants

static constexpr ALWAYS_INLINE size_t NumItems() { return 100000; }

static constexpr ALWAYS_INLINE size_t NumDistrictsPerWarehouse() {
  return 10;
}

static constexpr ALWAYS_INLINE size_t NumCustomersPerDistrict() {
  return 3000;
}

// configuration flags
static int g_disable_xpartition_txn = 0;
static int g_enable_separate_tree_per_partition = 0;
static int g_new_order_remote_item_pct = 1;
static int g_new_order_fast_id_gen = 0;
static int g_uniform_item_dist = 0;
static int g_order_status_scan_hack = 0;
static int g_wh_temperature = 0;
static uint g_microbench_rows = 100000;  // this many rows
// can't have both ratio and rows at the same time
static int g_microbench_wr_rows = 0;  // this number of rows to write
static int g_nr_suppliers = 10000;

// how much % of time a worker should use a random home wh
// 0 - always use home wh
// 50 - 50% of time use random wh
// 100 - always use a random wh
static double g_wh_spread = 0;

// TPC-C workload mix
// 0: NewOrder
// 1: Payment
// 2: CreditCheck
// 3: Delivery
// 4: OrderStatus
// 5: StockLevel
// 6: TPC-CH query 2 variant - original query 2, but /w marginal stock table
// update
// 7: Microbenchmark-random - same as Microbenchmark, but uses random read-set
// range
static unsigned g_txn_workload_mix[] = {
    45, 43, 0, 4, 4, 4, 0, 0};  // default TPC-C workload mix

static util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids = nullptr;

static inline std::atomic<uint64_t> &NewOrderIdHolder(unsigned warehouse,
                                                 unsigned district) {
  ASSERT(warehouse >= 1 && warehouse <= NumWarehouses());
  ASSERT(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
      (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t FastNewOrderIdGen(unsigned warehouse,
                                         unsigned district) {
  return NewOrderIdHolder(warehouse, district)
      .fetch_add(1, std::memory_order_acq_rel);
}

#ifndef NDEBUG
struct checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static ALWAYS_INLINE void SanityCheckCustomer(
      const customer::key *k, const customer::value *v) {
    ASSERT(v->c_credit == "BC" || v->c_credit == "GC");
    ASSERT(v->c_middle == "OE");
    ASSERT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    ASSERT(k->c_d_id >= 1 &&
           static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(k->c_id >= 1 &&
           static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
  }

  static ALWAYS_INLINE void SanityCheckWarehouse(
      const warehouse::key *k, const warehouse::value *v) {
    ASSERT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
    ASSERT(v->w_state.size() == 2);
    ASSERT(v->w_zip == "123456789");
  }

  static ALWAYS_INLINE void SanityCheckDistrict(
      const district::key *k, const district::value *v) {
    ASSERT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    ASSERT(k->d_id >= 1 &&
           static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->d_next_o_id >= 3001);
    ASSERT(v->d_state.size() == 2);
    ASSERT(v->d_zip == "123456789");
  }

  static ALWAYS_INLINE void SanityCheckItem(const item::key *k,
                                                   const item::value *v) {
    ASSERT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
    ASSERT(v->i_price >= 1.0 && v->i_price <= 100.0);
  }

  static ALWAYS_INLINE void SanityCheckStock(const stock::key *k) {
    ASSERT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    ASSERT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
  }

  static ALWAYS_INLINE void SanityCheckNewOrder(const new_order::key *k) {
    ASSERT(k->no_w_id >= 1 &&
           static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    ASSERT(k->no_d_id >= 1 &&
           static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
  }

  static ALWAYS_INLINE void SanityCheckOOrder(const oorder::key *k,
                                                     const oorder::value *v) {
    ASSERT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    ASSERT(k->o_d_id >= 1 &&
           static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->o_c_id >= 1 &&
           static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    ASSERT(v->o_carrier_id >= 0 &&
           static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static ALWAYS_INLINE void SanityCheckOrderLine(
      const order_line::key *k, const order_line::value *v) {
    ASSERT(k->ol_w_id >= 1 &&
           static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    ASSERT(k->ol_d_id >= 1 &&
           static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(k->ol_number >= 1 && k->ol_number <= 15);
    ASSERT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
  }
};
#endif

struct _dummy {};  // exists so we can inherit from it, so we can use a macro in
                   // an init list...

class tpcc_worker_mixin : private _dummy {
#define DEFN_TBL_INIT_X(name) , tbl_##name##_vec(partitions.at(#name))

 public:
  tpcc_worker_mixin(const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : _dummy()  // so hacky...
        TPCC_TABLE_LIST(DEFN_TBL_INIT_X) {
    ALWAYS_ASSERT(NumWarehouses() >= 1);
  }

#undef DEFN_TBL_INIT_X

 protected:
#define DEFN_TBL_ACCESSOR_X(name)                                   \
 private:                                                           \
  std::vector<ermia::OrderedIndex *> tbl_##name##_vec;                     \
                                                                    \
 protected:                                                         \
  ALWAYS_INLINE ermia::OrderedIndex *tbl_##name(unsigned int wid) { \
    ASSERT(wid >= 1 && wid <= NumWarehouses());                     \
    ASSERT(tbl_##name##_vec.size() == NumWarehouses());             \
    return tbl_##name##_vec[wid - 1];                               \
  }

  TPCC_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X

 public:
  static inline uint32_t GetCurrentTimeMillis() {
    // struct timeval tv;
    // ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    // return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number

    static thread_local uint32_t tl_hack = 0;
    return tl_hack++;
  }

  // utils for generating random #s and strings

  static ALWAYS_INLINE int CheckBetweenInclusive(int v, int lower,
                                                        int upper) {
    MARK_REFERENCED(lower);
    MARK_REFERENCED(upper);
    ASSERT(v >= lower);
    ASSERT(v <= upper);
    return v;
  }

  static ALWAYS_INLINE int RandomNumber(util::fast_random &r, int min,
                                               int max) {
    return CheckBetweenInclusive(
        (int)(r.next_uniform() * (max - min + 1) + min), min, max);
  }

  static ALWAYS_INLINE int NonUniformRandom(util::fast_random &r, int A, int C,
                                                   int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) %
            (max - min + 1)) +
           min;
  }

  static ALWAYS_INLINE int GetItemId(util::fast_random &r) {
    return CheckBetweenInclusive(
        g_uniform_item_dist ? RandomNumber(r, 1, NumItems())
                            : NonUniformRandom(r, 8191, 7911, 1, NumItems()),
        1, NumItems());
  }

  static ALWAYS_INLINE int GetCustomerId(util::fast_random &r) {
    return CheckBetweenInclusive(
        NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1,
        NumCustomersPerDistrict());
  }

  static std::string NameTokens[];

  // all tokens are at most 5 chars long
  static const size_t CustomerLastNameMaxSize = 5 * 3;

  static inline size_t GetCustomerLastName(uint8_t *buf, int num) {
    const std::string &s0 = NameTokens[num / 100];
    const std::string &s1 = NameTokens[(num / 10) % 10];
    const std::string &s2 = NameTokens[num % 10];
    uint8_t *const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    memcpy(buf, s0.data(), s0_sz);
    buf += s0_sz;
    memcpy(buf, s1.data(), s1_sz);
    buf += s1_sz;
    memcpy(buf, s2.data(), s2_sz);
    buf += s2_sz;
    return buf - begin;
  }

  static inline std::string GetCustomerLastName(int num) {
    std::string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t *)&ret[0], num));
    return ret;
  }

  static ALWAYS_INLINE std::string
  GetNonUniformCustomerLastNameLoad(util::fast_random &r) {
    return GetCustomerLastName(NonUniformRandom(r, 255, 157, 0, 999));
  }

  static ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(uint8_t *buf, util::fast_random &r) {
    return GetCustomerLastName(buf, NonUniformRandom(r, 255, 223, 0, 999));
  }

  static ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(char *buf, util::fast_random &r) {
    return GetNonUniformCustomerLastNameRun((uint8_t *)buf, r);
  }

  static ALWAYS_INLINE std::string
  GetNonUniformCustomerLastNameRun(util::fast_random &r) {
    return GetCustomerLastName(NonUniformRandom(r, 255, 223, 0, 999));
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline std::string RandomStr(util::fast_random &r, uint len) {
    // this is a property of the oltpbench implementation...
    if (!len) return "";

    uint i = 0;
    std::string buf(len - 1, 0);
    while (i < (len - 1)) {
      const char c = (char)r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c)) continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a std::string of length len
  static inline std::string RandomNStr(util::fast_random &r, uint len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint i = 0; i < len; i++) buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }
};

std::string tpcc_worker_mixin::NameTokens[] = {
    std::string("BAR"),   std::string("OUGHT"), std::string("ABLE"), std::string("PRI"),
    std::string("PRES"),  std::string("ESE"),   std::string("ANTI"), std::string("CALLY"),
    std::string("ATION"), std::string("EING"),
};

class tpcc_cmdlog_redoer: public bench_worker, public tpcc_worker_mixin {
 public:
  tpcc_cmdlog_redoer(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
              const std::map<std::string, ermia::OrderedIndex *> &open_tables,
              const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_worker(worker_id, false, seed, db, open_tables),
        tpcc_worker_mixin(partitions) {
    memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
  }

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;

  rc_t txn_new_order(uint wh);
  static rc_t TxnNewOrder(bench_worker *w, void *param) {
    uint64_t wh = (uint64_t)param;
    return static_cast<tpcc_cmdlog_redoer *>(w)->txn_new_order((uint)wh);
  }

  rc_t txn_delivery(uint32_t warehouse_id);
  static rc_t TxnDelivery(bench_worker *w, void *param) {
    uint64_t wh = (uint64_t)param;
    return static_cast<tpcc_cmdlog_redoer *>(w)->txn_delivery((uint)wh);
  }

  rc_t txn_payment(uint32_t warehouse_id);
  static rc_t TxnPayment(bench_worker *w, void *param) {
    uint64_t wh = (uint64_t)param;
    return static_cast<tpcc_cmdlog_redoer *>(w)->txn_payment((uint)wh);
  }

  virtual workload_desc_vec get_workload() const {
    LOG(FATAL) << "Not applicable";
  }

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const {
    cmdlog_redo_workload_desc_vec w;
    w.push_back(cmdlog_redo_workload_desc("NewOrder", TxnNewOrder));
    w.push_back(cmdlog_redo_workload_desc("Payment", TxnPayment));
    w.push_back(cmdlog_redo_workload_desc("Delivery", TxnDelivery));
    return w;
  }

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena.next(size); }

 private:
  int32_t last_no_o_ids[10];  // XXX(stephentu): hack
};

class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
 public:
  tpcc_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
              const std::map<std::string, ermia::OrderedIndex *> &open_tables,
              const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint home_warehouse_id)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        tpcc_worker_mixin(partitions),
        home_warehouse_id(home_warehouse_id) {
    ASSERT(home_warehouse_id >= 1 and home_warehouse_id <= NumWarehouses() + 1);
    memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
  }

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;

  rc_t txn_new_order();

  static rc_t TxnNewOrder(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_new_order();
  }

  rc_t txn_delivery();

  static rc_t TxnDelivery(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_delivery();
  }

  rc_t txn_credit_check();
  static rc_t TxnCreditCheck(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_credit_check();
  }

  rc_t txn_payment();

  static rc_t TxnPayment(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_payment();
  }

  rc_t txn_order_status();

  static rc_t TxnOrderStatus(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_order_status();
  }

  rc_t txn_stock_level();

  static rc_t TxnStockLevel(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_stock_level();
  }

  rc_t txn_microbench_random();

  static rc_t TxnMicroBenchRandom(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_microbench_random();
  }

  rc_t txn_query2();

  static rc_t TxnQuery2(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_query2();
  }

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const {
    LOG(FATAL) << "Not applicable";
  }

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;
    // numbers from sigmod.csail.mit.edu:
    // w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
    // w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
    // w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k
    // ops/sec
    // w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k
    // ops/sec
    // w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k
    // ops/sec
    unsigned m = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);
    if (g_txn_workload_mix[0])
      w.push_back(workload_desc(
          "NewOrder", double(g_txn_workload_mix[0]) / 100.0, TxnNewOrder));
    if (g_txn_workload_mix[1])
      w.push_back(workload_desc(
          "Payment", double(g_txn_workload_mix[1]) / 100.0, TxnPayment));
    if (g_txn_workload_mix[2])
      w.push_back(workload_desc("CreditCheck",
                                double(g_txn_workload_mix[2]) / 100.0,
                                TxnCreditCheck));
    if (g_txn_workload_mix[3])
      w.push_back(workload_desc(
          "Delivery", double(g_txn_workload_mix[3]) / 100.0, TxnDelivery));
    if (g_txn_workload_mix[4])
      w.push_back(workload_desc("OrderStatus",
                                double(g_txn_workload_mix[4]) / 100.0,
                                TxnOrderStatus));
    if (g_txn_workload_mix[5])
      w.push_back(workload_desc(
          "StockLevel", double(g_txn_workload_mix[5]) / 100.0, TxnStockLevel));
    if (g_txn_workload_mix[6])
      w.push_back(workload_desc("Query2", double(g_txn_workload_mix[6]) / 100.0,
                                TxnQuery2));
    if (g_txn_workload_mix[7])
      w.push_back(workload_desc("MicroBenchRandom",
                                double(g_txn_workload_mix[7]) / 100.0,
                                TxnMicroBenchRandom));
    return w;
  }

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena.next(size); }

 private:
  ALWAYS_INLINE unsigned pick_wh(util::fast_random &r) {
    if (g_wh_temperature) {  // do it 80/20 way
      uint w = 0;
      if (r.next_uniform() >= 0.2)  // 80% access
        w = hot_whs[r.next() % hot_whs.size()];
      else
        w = cold_whs[r.next() % cold_whs.size()];
      LOG_IF(FATAL, w < 1 || w > NumWarehouses());
      return w;
    } else {
      ASSERT(g_wh_spread >= 0 and g_wh_spread <= 1);
      // wh_spread = 0: always use home wh
      // wh_spread = 1: always use random wh
      if (ermia::config::command_log || g_wh_spread == 0 || r.next_uniform() >= g_wh_spread)
        return home_warehouse_id;
      return r.next() % NumWarehouses() + 1;
    }
  }

 public:
  // 80/20 access: 80% of all accesses touch 20% of WHs (randmonly
  // choose one from hot_whs), while the 20% of accesses touch the
  // remaining 80% of WHs.
  static std::vector<uint> hot_whs;
  static std::vector<uint> cold_whs;

 private:
  const uint home_warehouse_id;
  int32_t last_no_o_ids[10];  // XXX(stephentu): hack
};

std::vector<uint> tpcc_worker::hot_whs;
std::vector<uint> tpcc_worker::cold_whs;

class tpcc_nation_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_nation_loader(unsigned long seed, ermia::Engine *db,
                     const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                     const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
    uint i;
    for (i = 0; i < 62; i++) {
      const nation::key k(nations[i].id);
      nation::value v;

      const std::string n_comment = RandomStr(r, RandomNumber(r, 10, 20));
      v.n_name = std::string(nations[i].name);
      v.n_regionkey = nations[i].rId;
      v.n_comment.assign(n_comment);
      TryVerifyStrict(tbl_nation(1)->Insert(txn, Encode(str(Size(k)), k),
                                              Encode(str(Size(v)), v)));
    }
    TryVerifyStrict(db->Commit(txn));
  }
};

class tpcc_region_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_region_loader(unsigned long seed, ermia::Engine *db,
                     const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                     const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
    for (uint i = 0; i < 5; i++) {
      const region::key k(i);
      region::value v;

      v.r_name = std::string(regions[i]);
      const std::string r_comment = RandomStr(r, RandomNumber(r, 10, 20));
      v.r_comment.assign(r_comment);
      TryVerifyStrict(tbl_region(1)->Insert(txn, Encode(str(Size(k)), k),
                                              Encode(str(Size(v)), v)));
    }
    TryVerifyStrict(db->Commit(txn));
  }
};

class tpcc_supplier_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_supplier_loader(unsigned long seed, ermia::Engine *db,
                       const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                       const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    for (uint i = 0; i < 10000; i++) {
      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      const supplier::key k(i);
      supplier::value v;

      v.su_name = std::string("Supplier#") + std::string("000000000") + std::to_string(i);
      v.su_address = RandomStr(r, RandomNumber(r, 10, 40));

      auto rand = 0;
      while (rand == 0 || (rand > '9' && rand < 'A') ||
             (rand > 'Z' && rand < 'a'))
        rand = RandomNumber(r, '0', 'z');
      v.su_nationkey = rand;
      //		  v.su_phone = std::string("911");			//
      //XXX. nobody wants this field
      //		  v.su_acctbal = 0;
      //		  v.su_comment = RandomStr(r, RandomNumber(r,10,39));
      //// XXX. Q16 uses this. fix this if needed.

      TryVerifyStrict(tbl_supplier(1)->Insert(txn, Encode(str(Size(k)), k),
                                                Encode(str(Size(v)), v)));

      TryVerifyStrict(db->Commit(txn));
    }
  }
};

class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_warehouse_loader(unsigned long seed, ermia::Engine *db,
                        const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                        const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    std::vector<warehouse::value> warehouses;
    for (uint i = 1; i <= NumWarehouses(); i++) {
      arena.reset();
      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      const warehouse::key k(i);

      const std::string w_name = RandomStr(r, RandomNumber(r, 6, 10));
      const std::string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_city = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_state = RandomStr(r, 3);
      const std::string w_zip = "123456789";

      warehouse::value v;
      v.w_ytd = 300000;
      v.w_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
      v.w_name.assign(w_name);
      v.w_street_1.assign(w_street_1);
      v.w_street_2.assign(w_street_2);
      v.w_city.assign(w_city);
      v.w_state.assign(w_state);
      v.w_zip.assign(w_zip);

#ifndef NDEBUG
      checker::SanityCheckWarehouse(&k, &v);
#endif
      const size_t sz = Size(v);
      warehouse_total_sz += sz;
      n_warehouses++;
      TryVerifyStrict(tbl_warehouse(i)->Insert(txn, Encode(str(Size(k)), k),
                                                 Encode(str(sz), v)));

      warehouses.push_back(v);
      TryVerifyStrict(db->Commit(txn));
    }
    for (uint i = 1; i <= NumWarehouses(); i++) {
      arena.reset();
      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      const warehouse::key k(i);
      warehouse::value warehouse_temp;
      ermia::varstr warehouse_v = str(Size(warehouse_temp));
      TryVerifyStrict(
          tbl_warehouse(i)->Get(txn, Encode(str(Size(k)), k), warehouse_v));
      const warehouse::value *v = Decode(warehouse_v, warehouse_temp);
      ALWAYS_ASSERT(warehouses[i - 1] == *v);

#ifndef NDEBUG
      checker::SanityCheckWarehouse(&k, v);
#endif
      TryVerifyStrict(db->Commit(txn));
    }

    // pre-build supp-stock mapping table to boost tpc-ch queries
    for (uint w = 1; w <= NumWarehouses(); w++)
      for (uint i = 1; i <= NumItems(); i++)
        supp_stock_map[w * i % 10000].push_back(std::make_pair(w, i));

    if (ermia::config::verbose) {
      std::cerr << "[INFO] finished loading warehouse" << std::endl;
      std::cerr << "[INFO]   * average warehouse record length: "
           << (double(warehouse_total_sz) / double(n_warehouses)) << " bytes"
           << std::endl;
    }
  }
};

class tpcc_item_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_item_loader(unsigned long seed, ermia::Engine *db,
                   const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                   const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    uint64_t total_sz = 0;
    for (uint i = 1; i <= NumItems(); i++) {
      arena.reset();
      ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
      // items don't "belong" to a certain warehouse, so no pinning
      const item::key k(i);

      item::value v;
      const std::string i_name = RandomStr(r, RandomNumber(r, 14, 24));
      v.i_name.assign(i_name);
      v.i_price = (float)RandomNumber(r, 100, 10000) / 100.0;
      const int len = RandomNumber(r, 26, 50);
      if (RandomNumber(r, 1, 100) > 10) {
        const std::string i_data = RandomStr(r, len);
        v.i_data.assign(i_data);
      } else {
        const int startOriginal = RandomNumber(r, 2, (len - 8));
        const std::string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                              RandomStr(r, len - startOriginal - 7);
        v.i_data.assign(i_data);
      }
      v.i_im_id = RandomNumber(r, 1, 10000);

#ifndef NDEBUG
      checker::SanityCheckItem(&k, &v);
#endif
      const size_t sz = Size(v);
      total_sz += sz;
      TryVerifyStrict(tbl_item(1)->Insert(
          txn, Encode(str(Size(k)), k),
          Encode(str(sz), v)));  // this table is shared, so any partition is OK
      TryVerifyStrict(db->Commit(txn));
    }
    if (ermia::config::verbose) {
      std::cerr << "[INFO] finished loading item" << std::endl;
      std::cerr << "[INFO]   * average item record length: "
           << (double(total_sz) / double(NumItems())) << " bytes" << std::endl;
    }
  }
};

class tpcc_stock_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_stock_loader(unsigned long seed, ermia::Engine *db,
                    const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                    const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                    ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  virtual void load() {
    std::string obj_buf, obj_buf1;

    uint64_t stock_total_sz = 0, n_stocks = 0;
    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);

    for (uint w = w_start; w <= w_end; w++) {
      const size_t batchsize = 10;
      for (size_t i = 0; i < NumItems();) {
        size_t iend = std::min(i + batchsize, NumItems());
        ermia::scoped_str_arena s_arena(arena);
        for (uint j = i + 1; j <= iend; j++) {
          arena.reset();
          ermia::transaction *const txn = db->NewTransaction(0, arena, txn_buf());
          const stock::key k(w, j);
          const stock_data::key k_data(w, j);

          stock::value v;
          v.s_quantity = RandomNumber(r, 10, 100);
          v.s_ytd = 0;
          v.s_order_cnt = 0;
          v.s_remote_cnt = 0;

          stock_data::value v_data;
          const int len = RandomNumber(r, 26, 50);
          if (RandomNumber(r, 1, 100) > 10) {
            const std::string s_data = RandomStr(r, len);
            v_data.s_data.assign(s_data);
          } else {
            const int startOriginal = RandomNumber(r, 2, (len - 8));
            const std::string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                                  RandomStr(r, len - startOriginal - 7);
            v_data.s_data.assign(s_data);
          }
          v_data.s_dist_01.assign(RandomStr(r, 24));
          v_data.s_dist_02.assign(RandomStr(r, 24));
          v_data.s_dist_03.assign(RandomStr(r, 24));
          v_data.s_dist_04.assign(RandomStr(r, 24));
          v_data.s_dist_05.assign(RandomStr(r, 24));
          v_data.s_dist_06.assign(RandomStr(r, 24));
          v_data.s_dist_07.assign(RandomStr(r, 24));
          v_data.s_dist_08.assign(RandomStr(r, 24));
          v_data.s_dist_09.assign(RandomStr(r, 24));
          v_data.s_dist_10.assign(RandomStr(r, 24));

#ifndef NDEBUG
          checker::SanityCheckStock(&k);
#endif
          const size_t sz = Size(v);
          stock_total_sz += sz;
          n_stocks++;
          TryVerifyStrict(tbl_stock(w)->Insert(txn, Encode(str(Size(k)), k),
                                                 Encode(str(sz), v)));
          TryVerifyStrict(
              tbl_stock_data(w)->Insert(txn, Encode(str(Size(k_data)), k_data),
                                        Encode(str(Size(v_data)), v_data)));
          TryVerifyStrict(db->Commit(txn));
        }

        // loop update
        i = iend;
      }
    }
    if (ermia::config::verbose) {
      if (warehouse_id == -1) {
        std::cerr << "[INFO] finished loading stock" << std::endl;
        std::cerr << "[INFO]   * average stock record length: "
             << (double(stock_total_sz) / double(n_stocks)) << " bytes" << std::endl;
      } else {
        std::cerr << "[INFO] finished loading stock (w=" << warehouse_id << ")"
             << std::endl;
      }
    }
  }

 private:
  ssize_t warehouse_id;
};

class tpcc_district_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_district_loader(unsigned long seed, ermia::Engine *db,
                       const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                       const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;

    const ssize_t bsize = 10;
    uint64_t district_total_sz = 0, n_districts = 0;
    uint cnt = 0;
    for (uint w = 1; w <= NumWarehouses(); w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
        arena.reset();
        ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
        const district::key k(w, d);

        district::value v;
        v.d_ytd = 30000;
        v.d_tax = (float)(RandomNumber(r, 0, 2000) / 10000.0);
        v.d_next_o_id = 3001;
        v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
        v.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_state.assign(RandomStr(r, 3));
        v.d_zip.assign("123456789");

#ifndef NDEBUG
        checker::SanityCheckDistrict(&k, &v);
#endif
        const size_t sz = Size(v);
        district_total_sz += sz;
        n_districts++;
        TryVerifyStrict(tbl_district(w)->Insert(txn, Encode(str(Size(k)), k),
                                                  Encode(str(sz), v)));

        TryVerifyStrict(db->Commit(txn));
      }
    }
    if (ermia::config::verbose) {
      std::cerr << "[INFO] finished loading district" << std::endl;
      std::cerr << "[INFO]   * average district record length: "
           << (double(district_total_sz) / double(n_districts)) << " bytes"
           << std::endl;
    }
  }
};

class tpcc_customer_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_customer_loader(unsigned long seed, ermia::Engine *db,
                       const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                       const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                       ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  virtual void load() {
    std::string obj_buf;

    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);
    const size_t batchsize = 10;
    const size_t nbatches = (batchsize > NumCustomersPerDistrict())
                                ? 1
                                : (NumCustomersPerDistrict() / batchsize);

    uint64_t total_sz = 0;

    for (uint w = w_start; w <= w_end; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        for (uint batch = 0; batch < nbatches;) {
          const size_t cstart = batch * batchsize;
          const size_t cend =
              std::min((batch + 1) * batchsize, NumCustomersPerDistrict());
          for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
            ermia::scoped_str_arena s_arena(arena);
            arena.reset();
            ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
            const uint c = cidx0 + 1;
            const customer::key k(w, d, c);

            customer::value v;
            v.c_id = c;  // Put the c_id here in the tuple, needed by
                         // order-status later
            v.c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
            if (RandomNumber(r, 1, 100) <= 10)
              v.c_credit.assign("BC");
            else
              v.c_credit.assign("GC");

            if (c <= 1000)
              v.c_last.assign(GetCustomerLastName(c - 1));
            else
              v.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

            v.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
            v.c_credit_lim = 50000;

            v.c_balance = -10;
            v.c_ytd_payment = 10;
            v.c_payment_cnt = 1;
            v.c_delivery_cnt = 0;

            v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_state.assign(RandomStr(r, 3));
            v.c_zip.assign(RandomNStr(r, 4) + "11111");
            v.c_phone.assign(RandomNStr(r, 16));
            v.c_since = GetCurrentTimeMillis();
            v.c_middle.assign("OE");
            v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

#ifndef NDEBUG
            checker::SanityCheckCustomer(&k, &v);
#endif
            const size_t sz = Size(v);
            total_sz += sz;
            ermia::OID c_oid = 0;  // Get the OID and put in customer_name_idx later
            TryVerifyStrict(tbl_customer(w)->Insert(
                txn, Encode(str(Size(k)), k), Encode(str(sz), v), &c_oid));
            TryVerifyStrict(db->Commit(txn));

            // customer name index
            const customer_name_idx::key k_idx(
                k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));

            // index structure is:
            // (c_w_id, c_d_id, c_last, c_first) -> OID

            arena.reset();
            txn = db->NewTransaction(0, arena, txn_buf());
            TryVerifyStrict(tbl_customer_name_idx(w)->Insert(
                txn, Encode(str(Size(k_idx)), k_idx), c_oid));
            TryVerifyStrict(db->Commit(txn));
            arena.reset();

            history::key k_hist;
            k_hist.h_c_id = c;
            k_hist.h_c_d_id = d;
            k_hist.h_c_w_id = w;
            k_hist.h_d_id = d;
            k_hist.h_w_id = w;
            k_hist.h_date = GetCurrentTimeMillis();

            history::value v_hist;
            v_hist.h_amount = 10;
            v_hist.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

            arena.reset();
            txn = db->NewTransaction(0, arena, txn_buf());
            TryVerifyStrict(
                tbl_history(w)->Insert(txn, Encode(str(Size(k_hist)), k_hist),
                                       Encode(str(Size(v_hist)), v_hist)));
            TryVerifyStrict(db->Commit(txn));
          }
          batch++;
        }
      }
    }
    if (ermia::config::verbose) {
      if (warehouse_id == -1) {
        std::cerr << "[INFO] finished loading customer" << std::endl;
        std::cerr << "[INFO]   * average customer record length: "
             << (double(total_sz) /
                 double(NumWarehouses() * NumDistrictsPerWarehouse() *
                        NumCustomersPerDistrict())) << " bytes " << std::endl;
      } else {
        std::cerr << "[INFO] finished loading customer (w=" << warehouse_id << ")"
             << std::endl;
      }
    }
  }

 private:
  ssize_t warehouse_id;
};

class tpcc_order_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_order_loader(unsigned long seed, ermia::Engine *db,
                    const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                    const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                    ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  size_t NumOrderLinesPerCustomer() { return RandomNumber(r, 5, 15); }

  virtual void load() {
    std::string obj_buf;

    uint64_t order_line_total_sz = 0, n_order_lines = 0;
    uint64_t oorder_total_sz = 0, n_oorders = 0;
    uint64_t new_order_total_sz = 0, n_new_orders = 0;

    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);

    for (uint w = w_start; w <= w_end; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        std::set<uint> c_ids_s;
        std::vector<uint> c_ids;
        while (c_ids.size() != NumCustomersPerDistrict()) {
          const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
          if (c_ids_s.count(x)) continue;
          c_ids_s.insert(x);
          c_ids.emplace_back(x);
        }
        for (uint c = 1; c <= NumCustomersPerDistrict();) {
          ermia::scoped_str_arena s_arena(arena);
          arena.reset();
          ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
          const oorder::key k_oo(w, d, c);

          oorder::value v_oo;
          v_oo.o_c_id = c_ids[c - 1];
          if (k_oo.o_id < 2101)
            v_oo.o_carrier_id = RandomNumber(r, 1, 10);
          else
            v_oo.o_carrier_id = 0;
          v_oo.o_ol_cnt = NumOrderLinesPerCustomer();
          v_oo.o_all_local = 1;
          v_oo.o_entry_d = GetCurrentTimeMillis();

#ifndef NDEBUG
          checker::SanityCheckOOrder(&k_oo, &v_oo);
#endif
          const size_t sz = Size(v_oo);
          oorder_total_sz += sz;
          n_oorders++;
          ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
          TryVerifyStrict(
              tbl_oorder(w)->Insert(txn, Encode(str(Size(k_oo)), k_oo),
                                    Encode(str(sz), v_oo), &v_oo_oid));
          TryVerifyStrict(db->Commit(txn));
          arena.reset();
          txn = db->NewTransaction(0, arena, txn_buf());

          const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id,
                                              v_oo.o_c_id, k_oo.o_id);
          TryVerifyStrict(tbl_oorder_c_id_idx(w)->Insert(
              txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
          TryVerifyStrict(db->Commit(txn));

          if (c >= 2101) {
            arena.reset();
            txn = db->NewTransaction(0, arena, txn_buf());
            const new_order::key k_no(w, d, c);
            const new_order::value v_no;

#ifndef NDEBUG
            checker::SanityCheckNewOrder(&k_no);
#endif
            const size_t sz = Size(v_no);
            new_order_total_sz += sz;
            n_new_orders++;
            TryVerifyStrict(tbl_new_order(w)->Insert(
                txn, Encode(str(Size(k_no)), k_no), Encode(str(sz), v_no)));
            TryVerifyStrict(db->Commit(txn));
          }

          for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
            const order_line::key k_ol(w, d, c, l);

            order_line::value v_ol;
            v_ol.ol_i_id = RandomNumber(r, 1, 100000);
            if (k_ol.ol_o_id < 2101) {
              v_ol.ol_delivery_d = v_oo.o_entry_d;
              v_ol.ol_amount = 0;
            } else {
              v_ol.ol_delivery_d = 0;
              // random within [0.01 .. 9,999.99]
              v_ol.ol_amount = (float)(RandomNumber(r, 1, 999999) / 100.0);
            }

            v_ol.ol_supply_w_id = k_ol.ol_w_id;
            v_ol.ol_quantity = 5;
            // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
            // v_ol.ol_dist_info = RandomStr(r, 24);

#ifndef NDEBUG
            checker::SanityCheckOrderLine(&k_ol, &v_ol);
#endif
            const size_t sz = Size(v_ol);
            order_line_total_sz += sz;
            n_order_lines++;
            arena.reset();
            txn = db->NewTransaction(0, arena, txn_buf());
            TryVerifyStrict(tbl_order_line(w)->Insert(
                txn, Encode(str(Size(k_ol)), k_ol), Encode(str(sz), v_ol)));
            TryVerifyStrict(db->Commit(txn));
          }
          c++;
        }
      }
    }

    if (ermia::config::verbose) {
      if (warehouse_id == -1) {
        std::cerr << "[INFO] finished loading order" << std::endl;
        std::cerr << "[INFO]   * average order_line record length: "
             << (double(order_line_total_sz) / double(n_order_lines))
             << " bytes" << std::endl;
        std::cerr << "[INFO]   * average oorder record length: "
             << (double(oorder_total_sz) / double(n_oorders)) << " bytes"
             << std::endl;
        std::cerr << "[INFO]   * average new_order record length: "
             << (double(new_order_total_sz) / double(n_new_orders)) << " bytes"
             << std::endl;
      } else {
        std::cerr << "[INFO] finished loading order (w=" << warehouse_id << ")"
             << std::endl;
      }
    }
  }

 private:
  ssize_t warehouse_id;
};

rc_t tpcc_worker::txn_new_order() {
  const uint warehouse_id = pick_wh(r);
  const uint districtID = RandomNumber(r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(r, 5, 15);
  uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
               RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
        supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }
  ASSERT(!g_disable_xpartition_txn || allLocal);

  // XXX(stephentu): implement rollback
  //
  // worst case txn profile:
  //   1 customer get
  //   1 warehouse get
  //   1 district get
  //   1 new_order insert
  //   1 district put
  //   1 oorder insert
  //   1 oorder_cid_idx insert
  //   15 times:
  //      1 item get
  //      1 stock get
  //      1 stock put
  //      1 order_line insert
  //
  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 0
  //   max_read_set_size : 15
  //   max_write_set_size : 15
  //   num_txn_contexts : 9
  ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  const customer::key k_c(warehouse_id, districtID, customerID);
  customer::value v_c_temp;
  ermia::varstr sv_c_temp = str(Size(v_c_temp));
  TryVerifyRelaxed(tbl_customer(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c_temp));
  const customer::value *v_c = Decode(sv_c_temp, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;
  ermia::varstr sv_w_temp = str(Size(v_w_temp));
  TryVerifyRelaxed(tbl_warehouse(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_w)), k_w), sv_w_temp));
  const warehouse::value *v_w = Decode(sv_w_temp, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr sv_d_temp = str(Size(v_d_temp));
  TryVerifyRelaxed(tbl_district(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_d)), k_d), sv_d_temp));
  const district::value *v_d = Decode(sv_d_temp, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t my_next_o_id =
      g_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID)
                              : v_d->d_next_o_id;

  const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
  const new_order::value v_no;
  const size_t new_order_sz = Size(v_no);
  TryCatch(tbl_new_order(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_no)), k_no),
                         Encode(str(new_order_sz), v_no)));

  if (!g_new_order_fast_id_gen) {
    district::value v_d_new(*v_d);
    v_d_new.d_next_o_id++;
    TryCatch(tbl_district(warehouse_id)
                  ->Put(txn, Encode(str(Size(k_d)), k_d),
                        Encode(str(Size(v_d_new)), v_d_new)));
  }

  const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;  // seems to be ignored
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  const size_t oorder_sz = Size(v_oo);
  ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
  TryCatch(tbl_oorder(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_oo)), k_oo),
                         Encode(str(oorder_sz), v_oo), &v_oo_oid));

  const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID,
                                      k_no.no_o_id);
  TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));

  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];

    const item::key k_i(ol_i_id);
    item::value v_i_temp;
    ermia::varstr sv_i_temp = str(Size(v_i_temp));
    TryVerifyRelaxed(
        tbl_item(1)->Get(txn, Encode(str(Size(k_i)), k_i), sv_i_temp));
    const item::value *v_i = Decode(sv_i_temp, v_i_temp);
#ifndef NDEBUG
    checker::SanityCheckItem(&k_i, v_i);
#endif

    const stock::key k_s(ol_supply_w_id, ol_i_id);
    stock::value v_s_temp;
    ermia::varstr sv_s_temp = str(Size(v_s_temp));
    TryVerifyRelaxed(tbl_stock(ol_supply_w_id)
                         ->Get(txn, Encode(str(Size(k_s)), k_s), sv_s_temp));
    const stock::value *v_s = Decode(sv_s_temp, v_s_temp);
#ifndef NDEBUG
    checker::SanityCheckStock(&k_s);
#endif

    stock::value v_s_new(*v_s);
    if (v_s_new.s_quantity - ol_quantity >= 10)
      v_s_new.s_quantity -= ol_quantity;
    else
      v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
    v_s_new.s_ytd += ol_quantity;
    v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

    TryCatch(tbl_stock(ol_supply_w_id)
                  ->Put(txn, Encode(str(Size(k_s)), k_s),
                        Encode(str(Size(v_s_new)), v_s_new)));

    const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                               ol_number);
    order_line::value v_ol;
    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0;  // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
    v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
    v_ol.ol_quantity = int8_t(ol_quantity);

    const size_t order_line_sz = Size(v_ol);
    TryCatch(tbl_order_line(warehouse_id)
                  ->Insert(txn, Encode(str(Size(k_ol)), k_ol),
                           Encode(str(order_line_sz), v_ol)));
  }

  TryCatch(db->Commit(txn));
  if (ermia::config::command_log && !ermia::config::is_backup_srv()) {
    ermia::CommandLog::cmd_log->Insert(warehouse_id, TPCC_CLID_NEW_ORDER);
  }
  return {RC_TRUE};
}

class new_order_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  new_order_scan_callback() : k_no(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keylen);
    MARK_REFERENCED(value);
    ASSERT(keylen == sizeof(new_order::key));
    ASSERT(value.size() == sizeof(new_order::value));
    k_no = Decode(keyp, k_no_temp);
#ifndef NDEBUG
    new_order::value v_no_temp;
    const new_order::value *v_no = Decode(value, v_no_temp);
    checker::SanityCheckNewOrder(k_no);
#endif
    return false;
  }
  inline const new_order::key *get_key() const { return k_no; }

 private:
  new_order::key k_no_temp;
  const new_order::key *k_no;
};

// explicitly copies keys, because btree::search_range_call() interally
// re-uses a single string to pass keys (so using standard string assignment
// will force a re-allocation b/c of shared ref-counting)
//
// this isn't done for values, because all values are read-only in a
// multi-version
// system. ermia::varstrs for values only point to the real data in the database, but
// still we need to allocate a ermia::varstr header for each value. Internally it's
// just a ermia::varstr in the stack.
template <size_t N>
class static_limit_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  // XXX: push ignore_key into lower layer
  static_limit_callback(ermia::str_arena *arena, bool ignore_key)
      : n(0), arena(arena), ignore_key(ignore_key) {
    static_assert(N > 0, "xx");
    values.reserve(N);
  }

  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    ASSERT(n < N);
    ermia::varstr *pv = arena->next(0);  // header only
    pv->p = value.p;
    pv->l = value.l;
    if (ignore_key) {
      values.emplace_back(nullptr, pv);
    } else {
      ermia::varstr *const s_px = arena->next(keylen);
      ASSERT(s_px);
      s_px->copy_from(keyp, keylen);
      values.emplace_back(s_px, pv);
    }
    return ++n < N;
  }

  inline size_t size() const { return values.size(); }

  typedef std::pair<const ermia::varstr *, const ermia::varstr *> kv_pair;
  typename std::vector<kv_pair> values;

 private:
  size_t n;
  ermia::str_arena *arena;
  bool ignore_key;
};

rc_t tpcc_worker::txn_delivery() {
  const uint warehouse_id = pick_wh(r);
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  // worst case txn profile:
  //   10 times:
  //     1 new_order scan node
  //     1 oorder get
  //     2 order_line scan nodes
  //     15 order_line puts
  //     1 new_order remove
  //     1 oorder put
  //     1 customer get
  //     1 customer put
  //
  // output from counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 21
  //   max_read_set_size : 133
  //   max_write_set_size : 133
  //   num_txn_contexts : 4
  ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
    const new_order::key k_no_1(warehouse_id, d,
                                std::numeric_limits<int32_t>::max());
    new_order_scan_callback new_order_c;
    {
      TryCatch(tbl_new_order(warehouse_id)
                    ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                           &Encode(str(Size(k_no_1)), k_no_1), new_order_c,
                           s_arena.get()));
    }

    const new_order::key *k_no = new_order_c.get_key();
    if (unlikely(!k_no)) continue;
    last_no_o_ids[d - 1] = k_no->no_o_id + 1;  // XXX: update last seen

    const oorder::key k_oo(warehouse_id, d, k_no->no_o_id);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    oorder::value v_oo_temp;
    ermia::varstr sv_oo_temp = str(Size(v_oo_temp));
    TryCatchCondAbort(
        tbl_oorder(warehouse_id)
            ->Get(txn, Encode(str(Size(k_oo)), k_oo), sv_oo_temp));
    const oorder::value *v_oo = Decode(sv_oo_temp, v_oo_temp);
#ifndef NDEBUG
    checker::SanityCheckOOrder(&k_oo, v_oo);
#endif

    static_limit_callback<15> c(
        s_arena.get(), false);  // never more than 15 order_lines per order
    const order_line::key k_oo_0(warehouse_id, d, k_no->no_o_id, 0);
    const order_line::key k_oo_1(warehouse_id, d, k_no->no_o_id,
                                 std::numeric_limits<int32_t>::max());

    // XXX(stephentu): mutable scans would help here
    TryCatch(tbl_order_line(warehouse_id)
                  ->Scan(txn, Encode(str(Size(k_oo_0)), k_oo_0),
                         &Encode(str(Size(k_oo_1)), k_oo_1), c, s_arena.get()));
    float sum = 0.0;
    for (size_t i = 0; i < c.size(); i++) {
      order_line::value v_ol_temp;
      const order_line::value *v_ol = Decode(*c.values[i].second, v_ol_temp);

#ifndef NDEBUG
      order_line::key k_ol_temp;
      const order_line::key *k_ol = Decode(*c.values[i].first, k_ol_temp);
      checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

      sum += v_ol->ol_amount;
      order_line::value v_ol_new(*v_ol);
      v_ol_new.ol_delivery_d = ts;
      ASSERT(s_arena.get()->manages(c.values[i].first));
      TryCatch(tbl_order_line(warehouse_id)
                    ->Put(txn, *c.values[i].first,
                          Encode(str(Size(v_ol_new)), v_ol_new)));
    }

    // delete new order
    TryCatch(tbl_new_order(warehouse_id)
                  ->Remove(txn, Encode(str(Size(*k_no)), *k_no)));

    // update oorder
    oorder::value v_oo_new(*v_oo);
    v_oo_new.o_carrier_id = o_carrier_id;
    TryCatch(tbl_oorder(warehouse_id)
                  ->Put(txn, Encode(str(Size(k_oo)), k_oo),
                        Encode(str(Size(v_oo_new)), v_oo_new)));

    const uint c_id = v_oo->o_c_id;
    const float ol_total = sum;

    // update customer
    const customer::key k_c(warehouse_id, d, c_id);
    customer::value v_c_temp;
    ermia::varstr sv_c_temp = str(Size(v_c_temp));
    TryVerifyRelaxed(tbl_customer(warehouse_id)
                         ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c_temp));

    const customer::value *v_c = Decode(sv_c_temp, v_c_temp);
    customer::value v_c_new(*v_c);
    v_c_new.c_balance += ol_total;
    TryCatch(tbl_customer(warehouse_id)
                  ->Put(txn, Encode(str(Size(k_c)), k_c),
                        Encode(str(Size(v_c_new)), v_c_new)));
  }
  TryCatch(db->Commit(txn));
  if (ermia::config::command_log && !ermia::config::is_backup_srv()) {
    ermia::CommandLog::cmd_log->Insert(warehouse_id, TPCC_CLID_DELIVERY);
  }
  return {RC_TRUE};
}

class credit_check_order_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  credit_check_order_scan_callback(ermia::str_arena *arena) : _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(value);
    ermia::varstr *const k = _arena->next(keylen);
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k);
    return true;
  }
  std::vector<ermia::varstr *> output;
  ermia::str_arena *_arena;
};

class credit_check_order_line_scan_callback
    : public ermia::OrderedIndex::ScanCallback {
 public:
  credit_check_order_line_scan_callback() {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    _v_ol.emplace_back(&value);
    return true;
  }
  std::vector<const ermia::varstr *> _v_ol;
};

rc_t tpcc_worker::txn_credit_check() {
  /*
          Note: Cahill's credit check transaction to introduce SI's anomaly.

          SELECT c_balance, c_credit_lim
          INTO :c_balance, :c_credit_lim
          FROM Customer
          WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id

          SELECT SUM(ol_amount) INTO :neworder_balance
          FROM OrderLine, Orders, NewOrder
          WHERE ol_o_id = o_id AND ol_d_id = :d_id
          AND ol_w_id = :w_id AND o_d_id = :d_id
          AND o_w_id = :w_id AND o_c_id = :c_id
          AND no_o_id = o_id AND no_d_id = :d_id
          AND no_w_id = :w_id

          if (c_balance + neworder_balance > c_credit_lim)
          c_credit = "BC";
          else
          c_credit = "GC";

          SQL UPDATE Customer SET c_credit = :c_credit
          WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id
  */

  const uint warehouse_id = pick_wh(r);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  ASSERT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  // select * from customer with random C_ID
  customer::key k_c;
  customer::value v_c_temp;
  ermia::varstr sv_c_temp = str(Size(v_c_temp));
  const uint customerID = GetCustomerId(r);
  k_c.c_w_id = customerWarehouseID;
  k_c.c_d_id = customerDistrictID;
  k_c.c_id = customerID;
  TryVerifyRelaxed(tbl_customer(customerWarehouseID)
                       ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c_temp));
  const customer::value *v_c = Decode(sv_c_temp, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  // scan order
  //		c_w_id = :w_id;
  //		c_d_id = :d_id;
  //		c_id = :c_id;
  credit_check_order_scan_callback c_no(s_arena.get());
  const new_order::key k_no_0(warehouse_id, districtID, 0);
  const new_order::key k_no_1(warehouse_id, districtID,
                              std::numeric_limits<int32_t>::max());
  TryCatch(tbl_new_order(warehouse_id)
                ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                       &Encode(str(Size(k_no_1)), k_no_1), c_no,
                       s_arena.get()));
  ALWAYS_ASSERT(c_no.output.size());

  double sum = 0;
  for (auto &k : c_no.output) {
    new_order::key k_no_temp;
    const new_order::key *k_no = Decode(*k, k_no_temp);

    const oorder::key k_oo(warehouse_id, districtID, k_no->no_o_id);
    oorder::value v;
    ermia::varstr sv = str(Size(v));
    TryCatchCond(
        tbl_oorder(warehouse_id)->Get(txn, Encode(str(Size(k_oo)), k_oo), sv),
        continue);
    // Order line scan
    //		ol_d_id = :d_id
    //		ol_w_id = :w_id
    //		ol_o_id = o_id
    //		ol_number = 1-15
    static thread_local credit_check_order_line_scan_callback c_ol;
    c_ol._v_ol.clear();
    const order_line::key k_ol_0(warehouse_id, districtID, k_no->no_o_id, 1);
    const order_line::key k_ol_1(warehouse_id, districtID, k_no->no_o_id, 15);
    TryCatch(tbl_order_line(warehouse_id)
                  ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                         &Encode(str(Size(k_ol_1)), k_ol_1), c_ol,
                         s_arena.get()));
    ALWAYS_ASSERT(c_ol._v_ol.size());

    for (auto &v_ol : c_ol._v_ol) {
      order_line::value v_ol_temp;
      const order_line::value *val = Decode(*v_ol, v_ol_temp);

      // Aggregation
      sum += val->ol_amount;
    }
  }

  // c_credit update
  customer::value v_c_new(*v_c);
  if (v_c_new.c_balance + sum >= 5000)  // Threshold = 5K
    v_c_new.c_credit.assign("BC");
  else
    v_c_new.c_credit.assign("GC");
  TryCatch(tbl_customer(customerWarehouseID)
                ->Put(txn, Encode(str(Size(k_c)), k_c),
                      Encode(str(Size(v_c_new)), v_c_new)));

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_payment() {
  const uint warehouse_id = pick_wh(r);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  ASSERT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 10
  //   max_read_set_size : 71
  //   max_write_set_size : 1
  //   num_txn_contexts : 5
  ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;
  ermia::varstr sv_w_temp = str(Size(v_w_temp));
  TryVerifyRelaxed(tbl_warehouse(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_w)), k_w), sv_w_temp));
  const warehouse::value *v_w = Decode(sv_w_temp, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  warehouse::value v_w_new(*v_w);
  v_w_new.w_ytd += paymentAmount;
  TryCatch(tbl_warehouse(warehouse_id)
                ->Put(txn, Encode(str(Size(k_w)), k_w),
                      Encode(str(Size(v_w_new)), v_w_new)));

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr sv_d_temp = str(Size(v_d_temp));
  TryVerifyRelaxed(tbl_district(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_d)), k_d), sv_d_temp));
  const district::value *v_d = Decode(sv_d_temp, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  district::value v_d_new(*v_d);
  v_d_new.d_ytd += paymentAmount;
  TryCatch(tbl_district(warehouse_id)
                ->Put(txn, Encode(str(Size(k_d)), k_d),
                      Encode(str(Size(v_d_new)), v_d_new)));

  customer::key k_c;
  customer::value v_c;
  ermia::varstr sv_c = str(Size(v_c));
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, (char)255);

    customer_name_idx::key k_c_idx_0;
    k_c_idx_0.c_w_id = customerWarehouseID;
    k_c_idx_0.c_d_id = customerDistrictID;
    k_c_idx_0.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);

    customer_name_idx::key k_c_idx_1;
    k_c_idx_1.c_w_id = customerWarehouseID;
    k_c_idx_1.c_d_id = customerDistrictID;
    k_c_idx_1.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);

    static_limit_callback<NMaxCustomerIdxScanElems> c(
        s_arena.get(), true);  // probably a safe bet for now
    TryCatch(tbl_customer_name_idx(customerWarehouseID)
                  ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                         &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c,
                         s_arena.get()));
    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(*c.values[index].second, v_c);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = v_c.c_id;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = customerID;
    TryVerifyRelaxed(tbl_customer(customerWarehouseID)
                         ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c));
    Decode(sv_c, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif
  customer::value v_c_new(v_c);

  v_c_new.c_balance -= paymentAmount;
  v_c_new.c_ytd_payment += paymentAmount;
  v_c_new.c_payment_cnt++;
  if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", k_c.c_id,
                     k_c.c_d_id, k_c.c_w_id, districtID, warehouse_id,
                     paymentAmount, v_c.c_data.c_str());
    v_c_new.c_data.resize_junk(
        std::min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
    memcpy((void *)v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
  }

  TryCatch(tbl_customer(customerWarehouseID)
                ->Put(txn, Encode(str(Size(k_c)), k_c),
                      Encode(str(Size(v_c_new)), v_c_new)));

  const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID,
                         warehouse_id, ts);
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *)v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s", v_w->w_name.c_str(), v_d->d_name.c_str());
  v_h.h_data.resize_junk(
      std::min(static_cast<size_t>(n), v_h.h_data.max_size()));

  TryCatch(tbl_history(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_h)), k_h),
                         Encode(str(Size(v_h)), v_h)));

  TryCatch(db->Commit(txn));
  if (ermia::config::command_log && !ermia::config::is_backup_srv()) {
    ermia::CommandLog::cmd_log->Insert(warehouse_id, TPCC_CLID_PAYMENT);
  }
  return {RC_TRUE};
}

class order_line_nop_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  order_line_nop_callback() : n(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keylen);
    MARK_REFERENCED(keyp);
    ASSERT(keylen == sizeof(order_line::key));
    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);
#ifndef NDEBUG
    order_line::key k_ol_temp;
    const order_line::key *k_ol = Decode(keyp, k_ol_temp);
    checker::SanityCheckOrderLine(k_ol, v_ol);
#endif
    ++n;
    return true;
  }
  size_t n;
};

class latest_key_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  latest_key_callback(ermia::varstr &k, int32_t limit = -1)
      : limit(limit), n(0), k(&k) {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(value);
    ASSERT(limit == -1 || n < limit);
    k->copy_from(keyp, keylen);
    ++n;
    return (limit == -1) || (n < limit);
  }

  inline size_t size() const { return n; }
  inline ermia::varstr &kstr() { return *k; }

 private:
  int32_t limit;
  int32_t n;
  ermia::varstr *k;
};

rc_t tpcc_worker::txn_order_status() {
  const uint warehouse_id = pick_wh(r);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 13
  //   max_read_set_size : 81
  //   max_write_set_size : 0
  //   num_txn_contexts : 4
  const uint64_t read_only_mask =
      ermia::config::enable_safesnap ? ermia::transaction::TXN_FLAG_READ_ONLY : 0;
  ermia::transaction *txn = db->NewTransaction(read_only_mask, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  // NB: since txn_order_status() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)

  customer::key k_c;
  customer::value v_c;
  ermia::varstr sv_c = str(Size(v_c));
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, (char)255);

    customer_name_idx::key k_c_idx_0;
    k_c_idx_0.c_w_id = warehouse_id;
    k_c_idx_0.c_d_id = districtID;
    k_c_idx_0.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);

    customer_name_idx::key k_c_idx_1;
    k_c_idx_1.c_w_id = warehouse_id;
    k_c_idx_1.c_d_id = districtID;
    k_c_idx_1.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);

    static_limit_callback<NMaxCustomerIdxScanElems> c(
        s_arena.get(), true);  // probably a safe bet for now
    TryCatch(tbl_customer_name_idx(warehouse_id)
                  ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                         &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c,
                         s_arena.get()));
    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(*c.values[index].second, v_c);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = v_c.c_id;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = customerID;
    TryVerifyRelaxed(tbl_customer(warehouse_id)
                         ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c));
    Decode(sv_c, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif

  oorder_c_id_idx::value sv;
  ermia::varstr *newest_o_c_id = s_arena.get()->next(Size(sv));
  if (g_order_status_scan_hack) {
    // XXX(stephentu): HACK- we bound the # of elems returned by this scan to
    // 15- this is because we don't have reverse scans. In an ideal system, a
    // reverse scan would only need to read 1 btree node. We could simulate a
    // lookup by only reading the first element- but then we would *always*
    // read the first order by any customer.  To make this more interesting, we
    // randomly select which elem to pick within the 1st or 2nd btree nodes.
    // This is obviously a deviation from TPC-C, but it shouldn't make that
    // much of a difference in terms of performance numbers (in fact we are
    // making it worse for us)
    latest_key_callback c_oorder(*newest_o_c_id, (r.next() % 15) + 1);
    const oorder_c_id_idx::key k_oo_idx_0(warehouse_id, districtID, k_c.c_id,
                                          0);
    const oorder_c_id_idx::key k_oo_idx_1(warehouse_id, districtID, k_c.c_id,
                                          std::numeric_limits<int32_t>::max());
    {
      TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                    ->Scan(txn, Encode(str(Size(k_oo_idx_0)), k_oo_idx_0),
                           &Encode(str(Size(k_oo_idx_1)), k_oo_idx_1), c_oorder,
                           s_arena.get()));
    }
    ALWAYS_ASSERT(c_oorder.size());
  } else {
    latest_key_callback c_oorder(*newest_o_c_id, 1);
    const oorder_c_id_idx::key k_oo_idx_hi(warehouse_id, districtID, k_c.c_id,
                                           std::numeric_limits<int32_t>::max());
    TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                  ->ReverseScan(txn, Encode(str(Size(k_oo_idx_hi)), k_oo_idx_hi),
                                nullptr, c_oorder, s_arena.get()));
    ALWAYS_ASSERT(c_oorder.size() == 1);
  }

  oorder_c_id_idx::key k_oo_idx_temp;
  const oorder_c_id_idx::key *k_oo_idx = Decode(*newest_o_c_id, k_oo_idx_temp);
  const uint o_id = k_oo_idx->o_o_id;

  order_line_nop_callback c_order_line;
  const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, o_id,
                               std::numeric_limits<int32_t>::max());
  TryCatch(tbl_order_line(warehouse_id)
                ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                       &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line,
                       s_arena.get()));
  ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

class order_line_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  order_line_scan_callback() : n(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    ASSERT(keylen == sizeof(order_line::key));
    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);

#ifndef NDEBUG
    order_line::key k_ol_temp;
    const order_line::key *k_ol = Decode(keyp, k_ol_temp);
    checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

    s_i_ids[v_ol->ol_i_id] = 1;
    n++;
    return true;
  }
  size_t n;
  std::unordered_map<uint, bool> s_i_ids;
};

rc_t tpcc_worker::txn_stock_level() {
  const uint warehouse_id = pick_wh(r);
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 19
  //   max_read_set_size : 241
  //   max_write_set_size : 0
  //   n_node_scan_large_instances : 1
  //   n_read_set_large_instances : 2
  //   num_txn_contexts : 3
  const uint64_t read_only_mask =
      ermia::config::enable_safesnap ? ermia::transaction::TXN_FLAG_READ_ONLY : 0;
  ermia::transaction *txn = db->NewTransaction(read_only_mask, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  // NB: since txn_stock_level() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr sv_d_temp = str(Size(v_d_temp));
  TryVerifyRelaxed(tbl_district(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_d)), k_d), sv_d_temp));
  const district::value *v_d = Decode(sv_d_temp, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t cur_next_o_id =
      g_new_order_fast_id_gen
          ? NewOrderIdHolder(warehouse_id, districtID)
                .load(std::memory_order_acquire)
          : v_d->d_next_o_id;

  // manual joins are fun!
  order_line_scan_callback c;
  const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
  const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id, 0);
  {
    TryCatch(tbl_order_line(warehouse_id)
                  ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                         &Encode(str(Size(k_ol_1)), k_ol_1), c, s_arena.get()));
  }
  {
    std::unordered_map<uint, bool> s_i_ids_distinct;
    for (auto &p : c.s_i_ids) {
      const stock::key k_s(warehouse_id, p.first);
      stock::value v_s;
      ermia::varstr sv_s = str(Size(v_s));
      ASSERT(p.first >= 1 && p.first <= NumItems());
      TryVerifyRelaxed(
          tbl_stock(warehouse_id)->Get(txn, Encode(str(Size(k_s)), k_s), sv_s));
      const uint8_t *ptr = (const uint8_t *)sv_s.data();
      int16_t i16tmp;
      ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
      if (i16tmp < int(threshold)) s_i_ids_distinct[p.first] = 1;
    }
    // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
  }
  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_query2() {
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_READ_MOSTLY, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  static thread_local tpcc_table_scanner r_scanner(&arena);
  r_scanner.clear();
  const region::key k_r_0(0);
  const region::key k_r_1(5);
  TryCatch(tbl_region(1)->Scan(txn, Encode(str(sizeof(k_r_0)), k_r_0),
                                &Encode(str(sizeof(k_r_1)), k_r_1), r_scanner,
                                s_arena.get()));
  ALWAYS_ASSERT(r_scanner.output.size() == 5);

  static thread_local tpcc_table_scanner n_scanner(&arena);
  n_scanner.clear();
  const nation::key k_n_0(0);
  const nation::key k_n_1(std::numeric_limits<int32_t>::max());
  TryCatch(tbl_nation(1)->Scan(txn, Encode(str(sizeof(k_n_0)), k_n_0),
                                &Encode(str(sizeof(k_n_1)), k_n_1), n_scanner,
                                s_arena.get()));
  ALWAYS_ASSERT(n_scanner.output.size() == 62);

  // Pick a target region
  auto target_region = RandomNumber(r, 0, 4);
  //	auto target_region = 3;
  ALWAYS_ASSERT(0 <= target_region and target_region <= 4);

  // Scan region
  for (auto &r_r : r_scanner.output) {
    region::key k_r_temp;
    region::value v_r_temp;
    const region::key *k_r = Decode(*r_r.first, k_r_temp);
    const region::value *v_r = Decode(*r_r.second, v_r_temp);

    // filtering region
    if (v_r->r_name != std::string(regions[target_region])) continue;

    // Scan nation
    for (auto &r_n : n_scanner.output) {
      nation::key k_n_temp;
      nation::value v_n_temp;
      const nation::key *k_n = Decode(*r_n.first, k_n_temp);
      const nation::value *v_n = Decode(*r_n.second, v_n_temp);

      // filtering nation
      if (k_r->r_regionkey != v_n->n_regionkey) continue;

      // Scan suppliers
      for (auto i = 0; i < g_nr_suppliers; i++) {
        const supplier::key k_su(i);
        supplier::value v_su_tmp;
        ermia::varstr buf_su = str(Size(v_su_tmp));
        TryVerifyRelaxed(
            tbl_supplier(1)->Get(txn, Encode(str(Size(k_su)), k_su), buf_su));
        const supplier::value *v_su = Decode(buf_su, v_su_tmp);

        // Filtering suppliers
        if (k_n->n_nationkey != v_su->su_nationkey) continue;

        // aggregate - finding a stock tuple having min. stock level
        stock::key min_k_s(0, 0);
        stock::value min_v_s(0, 0, 0, 0);

        int16_t min_qty = std::numeric_limits<int16_t>::max();
        for (auto &it : supp_stock_map
                 [k_su.su_suppkey])  // already know
                                     // "mod((s_w_id*s_i_id),10000)=su_suppkey"
                                     // items
        {
          const stock::key k_s(it.first, it.second);
          stock::value v_s_tmp(0, 0, 0, 0);
          ermia::varstr sv = str(Size(v_s_tmp));
          TryVerifyRelaxed(
              tbl_stock(it.first)->Get(txn, Encode(str(Size(k_s)), k_s), sv));
          const stock::value *v_s = Decode(sv, v_s_tmp);

          ASSERT(k_s.s_w_id * k_s.s_i_id % 10000 == k_su.su_suppkey);
          if (min_qty > v_s->s_quantity) {
            min_k_s.s_w_id = k_s.s_w_id;
            min_k_s.s_i_id = k_s.s_i_id;
            min_v_s.s_quantity = v_s->s_quantity;
            min_v_s.s_ytd = v_s->s_ytd;
            min_v_s.s_order_cnt = v_s->s_order_cnt;
            min_v_s.s_remote_cnt = v_s->s_remote_cnt;
          }
        }

        // fetch the (lowest stock level) item info
        const item::key k_i(min_k_s.s_i_id);
        item::value v_i_temp;
        ermia::varstr sv_i_temp = str(Size(v_i_temp));
        TryVerifyRelaxed(
            tbl_item(1)->Get(txn, Encode(str(Size(k_i)), k_i), sv_i_temp));
        const item::value *v_i = Decode(sv_i_temp, v_i_temp);
#ifndef NDEBUG
        checker::SanityCheckItem(&k_i, v_i);
#endif

        //  filtering item (i_data like '%b')
        auto found = v_i->i_data.str().find('b');
        if (found != std::string::npos) continue;

        // XXX. read-mostly txn: update stock or item here

        if (min_v_s.s_quantity < 15) {
          stock::value new_v_s;
          new_v_s.s_quantity = min_v_s.s_quantity + 50;
          new_v_s.s_ytd = min_v_s.s_ytd;
          new_v_s.s_order_cnt = min_v_s.s_order_cnt;
          new_v_s.s_remote_cnt = min_v_s.s_remote_cnt;
#ifndef NDEBUG
          checker::SanityCheckStock(&min_k_s);
#endif
          TryCatch(tbl_stock(min_k_s.s_w_id)
                        ->Put(txn, Encode(str(Size(min_k_s)), min_k_s),
                              Encode(str(Size(new_v_s)), new_v_s)));
        }

        // TODO. sorting by n_name, su_name, i_id

        /*
        cout << k_su.su_suppkey        << ","
                << v_su->su_name                << ","
                << v_n->n_name                  << ","
                << k_i.i_id                     << ","
                << v_i->i_name                  << std::endl;
                */
      }
    }
  }

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_microbench_random() {
  ermia::transaction *txn = db->NewTransaction(0, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  uint start_w = 0, start_s = 0;
  ASSERT(NumWarehouses() * NumItems() >= g_microbench_rows);

  // pick start row, if it's not enough, later wrap to the first row
  uint w = start_w = RandomNumber(r, 1, NumWarehouses() + 1);
  uint s = start_s = RandomNumber(r, 1, NumItems() + 1);

  // read rows
  stock::value v;
  ermia::varstr sv = str(Size(v));
  for (uint i = 0; i < g_microbench_rows; i++) {
    const stock::key k_s(w, s);
    DLOG(INFO) << "rd " << w << " " << s;
    TryCatch(tbl_stock(w)->Get(txn, Encode(str(Size(k_s)), k_s), sv));

    if (++s > NumItems()) {
      s = 1;
      if (++w > NumWarehouses()) w = 1;
    }
  }

  // now write, in the same read-set
  uint n_write_rows = g_microbench_wr_rows;
  for (uint i = 0; i < n_write_rows; i++) {
    // generate key
    uint row_nr = RandomNumber(
        r, 1, n_write_rows + 1);  // XXX. do we need overlap checking?

    // index starting with 1 is a pain with %, starting with 0 instead:
    // convert row number to (w, s) tuple
    const uint idx =
        (start_w - 1) * NumItems() + (start_s - 1 + row_nr) % NumItems();
    const uint ww = idx / NumItems() + 1;
    const uint ss = idx % NumItems() + 1;

    DLOG(INFO) << (ww - 1) * NumItems() + ss - 1;
    DLOG(INFO) << ((start_w - 1) * NumItems() + start_s - 1 + row_nr) %
                       (NumItems() * (NumWarehouses()));
    ASSERT((ww - 1) * NumItems() + ss - 1 < NumItems() * NumWarehouses());
    ASSERT((ww - 1) * NumItems() + ss - 1 ==
           ((start_w - 1) * NumItems() + (start_s - 1 + row_nr) % NumItems()) %
               (NumItems() * (NumWarehouses())));

    // TODO. more plausible update needed
    const stock::key k_s(ww, ss);
    DLOG(INFO) << "wr " << ww << " " << ss << " row_nr=" << row_nr;

    stock::value v;
    v.s_quantity = RandomNumber(r, 10, 100);
    v.s_ytd = 0;
    v.s_order_cnt = 0;
    v.s_remote_cnt = 0;

#ifndef NDEBUG
    checker::SanityCheckStock(&k_s);
#endif
    TryCatch(tbl_stock(ww)->Put(txn, Encode(str(Size(k_s)), k_s),
                                 Encode(str(Size(v)), v)));
  }

  DLOG(INFO) << "micro-random finished";
#ifndef NDEBUG
  abort();
#endif

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

class tpcc_bench_runner : public bench_runner {
 private:
  static bool IsTableReadOnly(const char *name) {
    return strcmp("item", name) == 0;
  }

  static bool IsTableAppendOnly(const char *name) {
    return strcmp("history", name) == 0 || strcmp("oorder_c_id_idx", name) == 0;
  }

  static std::vector<ermia::OrderedIndex *> OpenTablesForTablespace(const char *name) {
    const bool is_read_only = IsTableReadOnly(name);
    const bool is_append_only = IsTableAppendOnly(name);
    const std::string s_name(name);
    std::vector<ermia::OrderedIndex *> ret(NumWarehouses());
    if (g_enable_separate_tree_per_partition && !is_read_only) {
      if (NumWarehouses() <= ermia::config::worker_threads) {
        for (size_t i = 0; i < NumWarehouses(); i++)
          ret[i] = ermia::IndexDescriptor::GetIndex(s_name + "_" + std::to_string(i));
      } else {
        const unsigned nwhse_per_partition =
            NumWarehouses() / ermia::config::worker_threads;
        for (size_t partid = 0; partid < ermia::config::worker_threads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend = (partid + 1 == ermia::config::worker_threads)
                                    ? NumWarehouses()
                                    : (partid + 1) * nwhse_per_partition;
          ermia::OrderedIndex *idx =
              ermia::IndexDescriptor::GetIndex(s_name + "_" + std::to_string(partid));
          for (size_t i = wstart; i < wend; i++) ret[i] = idx;
        }
      }
    } else {
      ermia::OrderedIndex *idx = ermia::IndexDescriptor::GetIndex(s_name);
      for (size_t i = 0; i < NumWarehouses(); i++) ret[i] = idx;
    }
    return ret;
  }

  static void RegisterTable(ermia::Engine *db, const char *name,
                            const char *primary_idx_name = nullptr) {
    const bool is_read_only = IsTableReadOnly(name);
    std::string s_name(name);
    if (g_enable_separate_tree_per_partition && !is_read_only) {
      if (ermia::config::is_backup_srv() ||
          NumWarehouses() <= ermia::config::worker_threads) {
        for (size_t i = 0; i < NumWarehouses(); i++) {
          std::string s_primary_name("");
          if (primary_idx_name) {
            s_primary_name = std::string(primary_idx_name) + "_" + std::to_string(i);
          }
          auto ss_name = s_name + "_" + std::to_string(i);
          db->CreateMasstreeTable(ss_name.c_str(), s_primary_name.c_str());
        }
      } else {
        const unsigned nwhse_per_partition =
            NumWarehouses() / ermia::config::worker_threads;
        for (size_t partid = 0; partid < ermia::config::worker_threads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend = (partid + 1 == ermia::config::worker_threads)
                                    ? NumWarehouses()
                                    : (partid + 1) * nwhse_per_partition;
          std::string s_primary_name("");
          if (primary_idx_name) {
            s_primary_name = std::string(primary_idx_name) + "_" + std::to_string(partid);
          }
          db->CreateMasstreeTable((s_name + std::string("_") + std::to_string(partid)).c_str(),
                          s_primary_name.c_str());
        }
      }
    } else {
      db->CreateMasstreeTable(name, primary_idx_name);
    }
  }

 public:
  tpcc_bench_runner(ermia::Engine *db) : bench_runner(db) {
    // Register all tables with the engine
    RegisterTable(db, "customer");
    RegisterTable(db, "customer_name_idx", "customer");
    RegisterTable(db, "district");
    RegisterTable(db, "history");
    RegisterTable(db, "item");
    RegisterTable(db, "new_order");
    RegisterTable(db, "oorder");
    RegisterTable(db, "oorder_c_id_idx", "oorder");
    RegisterTable(db, "order_line");
    RegisterTable(db, "stock");
    RegisterTable(db, "stock_data");
    RegisterTable(db, "nation");
    RegisterTable(db, "region");
    RegisterTable(db, "supplier");
    RegisterTable(db, "warehouse");
  }

  virtual void prepare(char *) {
#define OPEN_TABLESPACE_X(x) partitions[#x] = OpenTablesForTablespace(#x);

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
        ret.push_back(new tpcc_worker(i, r.next(), db, open_tables, partitions,
                                      &barrier_a, &barrier_b,
                                      (i % NumWarehouses()) + 1));
    } else {
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        ret.push_back(new tpcc_worker(i, r.next(), db, open_tables, partitions,
                                      &barrier_a, &barrier_b, i + 1));
      }
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_cmdlog_redoers() {
    ALWAYS_ASSERT(ermia::config::is_backup_srv() && ermia::config::command_log);
    util::fast_random r(23984543);
    std::vector<bench_worker *> ret;
    for (size_t i = 0; i < ermia::config::replay_threads; i++) {
      ret.push_back(new tpcc_cmdlog_redoer(i, r.next(), db, open_tables, partitions));
    }
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

  tpcc_bench_runner r(db);
  r.run();
}

rc_t tpcc_cmdlog_redoer::txn_new_order(uint warehouse_id) {
  const uint districtID = RandomNumber(r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(r, 5, 15);
  uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
               RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
        supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }
  ASSERT(!g_disable_xpartition_txn || allLocal);

  // XXX(stephentu): implement rollback
  //
  // worst case txn profile:
  //   1 customer get
  //   1 warehouse get
  //   1 district get
  //   1 new_order insert
  //   1 district put
  //   1 oorder insert
  //   1 oorder_cid_idx insert
  //   15 times:
  //      1 item get
  //      1 stock get
  //      1 stock put
  //      1 order_line insert
  //
  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 0
  //   max_read_set_size : 15
  //   max_write_set_size : 15
  //   num_txn_contexts : 9
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CMD_REDO, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  const customer::key k_c(warehouse_id, districtID, customerID);
  customer::value v_c_temp;
  ermia::varstr sv_c_temp = str(Size(v_c_temp));
  TryVerifyRelaxed(tbl_customer(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c_temp));
  const customer::value *v_c = Decode(sv_c_temp, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;
  ermia::varstr sv_w_temp = str(Size(v_w_temp));
  TryVerifyRelaxed(tbl_warehouse(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_w)), k_w), sv_w_temp));
  const warehouse::value *v_w = Decode(sv_w_temp, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr sv_d_temp = str(Size(v_d_temp));
  TryVerifyRelaxed(tbl_district(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_d)), k_d), sv_d_temp));
  const district::value *v_d = Decode(sv_d_temp, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t my_next_o_id =
      g_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID)
                              : v_d->d_next_o_id;

  const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
  const new_order::value v_no;
  const size_t new_order_sz = Size(v_no);
  TryCatch(tbl_new_order(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_no)), k_no),
                         Encode(str(new_order_sz), v_no)));

  if (!g_new_order_fast_id_gen) {
    district::value v_d_new(*v_d);
    v_d_new.d_next_o_id++;
    TryCatch(tbl_district(warehouse_id)
                  ->Put(txn, Encode(str(Size(k_d)), k_d),
                        Encode(str(Size(v_d_new)), v_d_new)));
  }

  const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;  // seems to be ignored
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  const size_t oorder_sz = Size(v_oo);
  ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
  TryCatch(tbl_oorder(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_oo)), k_oo),
                         Encode(str(oorder_sz), v_oo), &v_oo_oid));

  const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID,
                                      k_no.no_o_id);
  TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));

  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];

    const item::key k_i(ol_i_id);
    item::value v_i_temp;
    ermia::varstr sv_i_temp = str(Size(v_i_temp));
    TryVerifyRelaxed(
        tbl_item(1)->Get(txn, Encode(str(Size(k_i)), k_i), sv_i_temp));
    const item::value *v_i = Decode(sv_i_temp, v_i_temp);
#ifndef NDEBUG
    checker::SanityCheckItem(&k_i, v_i);
#endif

    const stock::key k_s(ol_supply_w_id, ol_i_id);
    stock::value v_s_temp;
    ermia::varstr sv_s_temp = str(Size(v_s_temp));
    TryVerifyRelaxed(tbl_stock(ol_supply_w_id)
                         ->Get(txn, Encode(str(Size(k_s)), k_s), sv_s_temp));
    const stock::value *v_s = Decode(sv_s_temp, v_s_temp);
#ifndef NDEBUG
    checker::SanityCheckStock(&k_s);
#endif

    stock::value v_s_new(*v_s);
    if (v_s_new.s_quantity - ol_quantity >= 10)
      v_s_new.s_quantity -= ol_quantity;
    else
      v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
    v_s_new.s_ytd += ol_quantity;
    v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

    TryCatch(tbl_stock(ol_supply_w_id)
                  ->Put(txn, Encode(str(Size(k_s)), k_s),
                        Encode(str(Size(v_s_new)), v_s_new)));

    const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                               ol_number);
    order_line::value v_ol;
    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0;  // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
    v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
    v_ol.ol_quantity = int8_t(ol_quantity);

    const size_t order_line_sz = Size(v_ol);
    TryCatch(tbl_order_line(warehouse_id)
                  ->Insert(txn, Encode(str(Size(k_ol)), k_ol),
                           Encode(str(order_line_sz), v_ol)));
  }

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_cmdlog_redoer::txn_payment(uint warehouse_id) {
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  ASSERT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 10
  //   max_read_set_size : 71
  //   max_write_set_size : 1
  //   num_txn_contexts : 5
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CMD_REDO, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;
  ermia::varstr sv_w_temp = str(Size(v_w_temp));
  TryVerifyRelaxed(tbl_warehouse(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_w)), k_w), sv_w_temp));
  const warehouse::value *v_w = Decode(sv_w_temp, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  warehouse::value v_w_new(*v_w);
  v_w_new.w_ytd += paymentAmount;
  TryCatch(tbl_warehouse(warehouse_id)
                ->Put(txn, Encode(str(Size(k_w)), k_w),
                      Encode(str(Size(v_w_new)), v_w_new)));

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr sv_d_temp = str(Size(v_d_temp));
  TryVerifyRelaxed(tbl_district(warehouse_id)
                       ->Get(txn, Encode(str(Size(k_d)), k_d), sv_d_temp));
  const district::value *v_d = Decode(sv_d_temp, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  district::value v_d_new(*v_d);
  v_d_new.d_ytd += paymentAmount;
  TryCatch(tbl_district(warehouse_id)
                ->Put(txn, Encode(str(Size(k_d)), k_d),
                      Encode(str(Size(v_d_new)), v_d_new)));

  customer::key k_c;
  customer::value v_c;
  ermia::varstr sv_c = str(Size(v_c));
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, (char)255);

    customer_name_idx::key k_c_idx_0;
    k_c_idx_0.c_w_id = customerWarehouseID;
    k_c_idx_0.c_d_id = customerDistrictID;
    k_c_idx_0.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);

    customer_name_idx::key k_c_idx_1;
    k_c_idx_1.c_w_id = customerWarehouseID;
    k_c_idx_1.c_d_id = customerDistrictID;
    k_c_idx_1.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);

    static_limit_callback<NMaxCustomerIdxScanElems> c(
        s_arena.get(), true);  // probably a safe bet for now
    TryCatch(tbl_customer_name_idx(customerWarehouseID)
                  ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                         &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c,
                         s_arena.get()));
    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(*c.values[index].second, v_c);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = v_c.c_id;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = customerID;
    TryVerifyRelaxed(tbl_customer(customerWarehouseID)
                         ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c));
    Decode(sv_c, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif
  customer::value v_c_new(v_c);

  v_c_new.c_balance -= paymentAmount;
  v_c_new.c_ytd_payment += paymentAmount;
  v_c_new.c_payment_cnt++;
  if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", k_c.c_id,
                     k_c.c_d_id, k_c.c_w_id, districtID, warehouse_id,
                     paymentAmount, v_c.c_data.c_str());
    v_c_new.c_data.resize_junk(
        std::min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
    memcpy((void *)v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
  }

  TryCatch(tbl_customer(customerWarehouseID)
                ->Put(txn, Encode(str(Size(k_c)), k_c),
                      Encode(str(Size(v_c_new)), v_c_new)));

  const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID,
                         warehouse_id, ts);
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *)v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s", v_w->w_name.c_str(), v_d->d_name.c_str());
  v_h.h_data.resize_junk(
      std::min(static_cast<size_t>(n), v_h.h_data.max_size()));

  TryCatch(tbl_history(warehouse_id)
                ->Insert(txn, Encode(str(Size(k_h)), k_h),
                         Encode(str(Size(v_h)), v_h)));

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_cmdlog_redoer::txn_delivery(uint warehouse_id) {
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  // worst case txn profile:
  //   10 times:
  //     1 new_order scan node
  //     1 oorder get
  //     2 order_line scan nodes
  //     15 order_line puts
  //     1 new_order remove
  //     1 oorder put
  //     1 customer get
  //     1 customer put
  //
  // output from counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 21
  //   max_read_set_size : 133
  //   max_write_set_size : 133
  //   num_txn_contexts : 4
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CMD_REDO, arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
    const new_order::key k_no_1(warehouse_id, d,
                                std::numeric_limits<int32_t>::max());
    new_order_scan_callback new_order_c;
    {
      TryCatch(tbl_new_order(warehouse_id)
                    ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                           &Encode(str(Size(k_no_1)), k_no_1), new_order_c,
                           s_arena.get()));
    }

    const new_order::key *k_no = new_order_c.get_key();
    if (unlikely(!k_no)) continue;
    last_no_o_ids[d - 1] = k_no->no_o_id + 1;  // XXX: update last seen

    const oorder::key k_oo(warehouse_id, d, k_no->no_o_id);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    oorder::value v_oo_temp;
    ermia::varstr sv_oo_temp = str(Size(v_oo_temp));
    TryCatchCondAbort(
        tbl_oorder(warehouse_id)
            ->Get(txn, Encode(str(Size(k_oo)), k_oo), sv_oo_temp));
    const oorder::value *v_oo = Decode(sv_oo_temp, v_oo_temp);
#ifndef NDEBUG
    checker::SanityCheckOOrder(&k_oo, v_oo);
#endif

    static_limit_callback<15> c(
        s_arena.get(), false);  // never more than 15 order_lines per order
    const order_line::key k_oo_0(warehouse_id, d, k_no->no_o_id, 0);
    const order_line::key k_oo_1(warehouse_id, d, k_no->no_o_id,
                                 std::numeric_limits<int32_t>::max());

    // XXX(stephentu): mutable scans would help here
    TryCatch(tbl_order_line(warehouse_id)
                  ->Scan(txn, Encode(str(Size(k_oo_0)), k_oo_0),
                         &Encode(str(Size(k_oo_1)), k_oo_1), c, s_arena.get()));
    float sum = 0.0;
    for (size_t i = 0; i < c.size(); i++) {
      order_line::value v_ol_temp;
      const order_line::value *v_ol = Decode(*c.values[i].second, v_ol_temp);

#ifndef NDEBUG
      order_line::key k_ol_temp;
      const order_line::key *k_ol = Decode(*c.values[i].first, k_ol_temp);
      checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

      sum += v_ol->ol_amount;
      order_line::value v_ol_new(*v_ol);
      v_ol_new.ol_delivery_d = ts;
      ASSERT(s_arena.get()->manages(c.values[i].first));
      TryCatch(tbl_order_line(warehouse_id)
                    ->Put(txn, *c.values[i].first,
                          Encode(str(Size(v_ol_new)), v_ol_new)));
    }

    // delete new order
    TryCatch(tbl_new_order(warehouse_id)
                  ->Remove(txn, Encode(str(Size(*k_no)), *k_no)));

    // update oorder
    oorder::value v_oo_new(*v_oo);
    v_oo_new.o_carrier_id = o_carrier_id;
    TryCatch(tbl_oorder(warehouse_id)
                  ->Put(txn, Encode(str(Size(k_oo)), k_oo),
                        Encode(str(Size(v_oo_new)), v_oo_new)));

    const uint c_id = v_oo->o_c_id;
    const float ol_total = sum;

    // update customer
    const customer::key k_c(warehouse_id, d, c_id);
    customer::value v_c_temp;
    ermia::varstr sv_c_temp = str(Size(v_c_temp));
    TryVerifyRelaxed(tbl_customer(warehouse_id)
                         ->Get(txn, Encode(str(Size(k_c)), k_c), sv_c_temp));

    const customer::value *v_c = Decode(sv_c_temp, v_c_temp);
    customer::value v_c_new(*v_c);
    v_c_new.c_balance += ol_total;
    TryCatch(tbl_customer(warehouse_id)
                  ->Put(txn, Encode(str(Size(k_c)), k_c),
                        Encode(str(Size(v_c_new)), v_c_new)));
  }
  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}
