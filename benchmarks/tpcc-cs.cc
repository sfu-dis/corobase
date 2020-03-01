/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#ifndef ADV_COROUTINE

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

#include "tpcc-common.h"

class tpcc_cs_worker : public bench_worker, public tpcc_worker_mixin {
 public:
  tpcc_cs_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                 const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                 const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                 spin_barrier *barrier_a, spin_barrier *barrier_b,
                 uint home_warehouse_id)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        tpcc_worker_mixin(partitions),
        home_warehouse_id(home_warehouse_id) {
    ASSERT(home_warehouse_id >= 1 and home_warehouse_id <= NumWarehouses() + 1);
    memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
    transactions = (ermia::transaction*)malloc(sizeof(ermia::transaction) * ermia::config::coro_batch_size);
  }

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;

  ermia::dia::generator<rc_t> txn_new_order(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnNewOrder(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_new_order(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_delivery(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnDelivery(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_delivery(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_credit_check(uint32_t idx, ermia::epoch_num begin_epoch);
  static CoroTxnHandle TxnCreditCheck(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_credit_check(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_payment(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnPayment(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_payment(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_order_status(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnOrderStatus(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_order_status(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_stock_level(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnStockLevel(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_stock_level(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_query2(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnQuery2(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_query2(idx, begin_epoch).get_handle();
  }

  ermia::dia::generator<rc_t> txn_microbench_random(uint32_t idx, ermia::epoch_num begin_epoch);

  static CoroTxnHandle TxnMicroBenchRandom(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_microbench_random(idx, begin_epoch).get_handle();
  }

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const override {
    LOG(FATAL) << "Not applicable";
  }

  virtual workload_desc_vec get_workload() const override {
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
          "NewOrder", double(g_txn_workload_mix[0]) / 100.0, nullptr, TxnNewOrder));
    if (g_txn_workload_mix[1])
      w.push_back(workload_desc(
          "Payment", double(g_txn_workload_mix[1]) / 100.0, nullptr, TxnPayment));
    if (g_txn_workload_mix[2])
      w.push_back(workload_desc(
          "CreditCheck",double(g_txn_workload_mix[2]) / 100.0, nullptr, TxnCreditCheck));
    if (g_txn_workload_mix[3])
      w.push_back(workload_desc(
          "Delivery", double(g_txn_workload_mix[3]) / 100.0, nullptr, TxnDelivery));
    if (g_txn_workload_mix[4])
      w.push_back(workload_desc(
          "OrderStatus", double(g_txn_workload_mix[4]) / 100.0, nullptr, TxnOrderStatus));
    if (g_txn_workload_mix[5])
      w.push_back(workload_desc(
          "StockLevel", double(g_txn_workload_mix[5]) / 100.0, nullptr, TxnStockLevel));
    if (g_txn_workload_mix[6])
      w.push_back(workload_desc(
          "Query2", double(g_txn_workload_mix[6]) / 100.0, nullptr, TxnQuery2));
    if (g_txn_workload_mix[7])
      w.push_back(workload_desc(
          "MicroBenchRandom", double(g_txn_workload_mix[7]) / 100.0, nullptr, TxnMicroBenchRandom));
    return w;
  }

  // Essentially a coroutine scheduler that switches between active transactions
  virtual void MyWork(char *) override {
    // No replication support
    ALWAYS_ASSERT(is_worker);
    workload = get_workload();
    txn_counts.resize(workload.size());

    const size_t batch_size = ermia::config::coro_batch_size;
    std::vector<CoroTxnHandle> handles(batch_size);
    std::vector<uint32_t> workload_idxs(batch_size);

    barrier_a->count_down();
    barrier_b->wait_for();
    util::timer t;
    while (running) {
      ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
      arena->reset();

      for(uint32_t i = 0; i < batch_size; i++) {
        uint32_t workload_idx = fetch_workload();
        workload_idxs[i] = workload_idx;
        handles[i] = workload[workload_idx].coro_fn(this, i, 0);
      }

      uint32_t todo_size = batch_size;
      while (todo_size) {
        ermia::dia::query_scheduler.run();
        for(uint32_t i = 0; i < batch_size; i++) {
          if (handles[i]) {
            if (handles[i].done()) {
              finish_workload(handles[i].promise().current_value, workload_idxs[i], t);
              handles[i].destroy();
              handles[i] = nullptr;
              todo_size--;
            } else {
              handles[i].resume();
            }
          }
        }
      }

      const unsigned long old_seed = r.get_seed();
      r.set_seed(old_seed);
      // TODO: epoch exit correctly
      ermia::MM::epoch_exit(0, begin_epoch);
    }
  }

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }

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
  ermia::transaction *transactions;
};

std::vector<uint> tpcc_cs_worker::hot_whs;
std::vector<uint> tpcc_cs_worker::cold_whs;

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_new_order(uint32_t idx, ermia::epoch_num begin_epoch) {
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

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
  const customer::key k_c(warehouse_id, districtID, customerID);
  customer::value v_c_temp;
  ermia::varstr valptr;

  rc = rc_t{RC_INVALID};
  tbl_customer(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val;
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const customer::value *v_c = Decode(valptr, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;

  rc = rc_t{RC_INVALID};
  tbl_warehouse(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_w)), k_w), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val; 
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const warehouse::value *v_w = Decode(valptr, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;

  rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val;
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t my_next_o_id =
      g_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID)
                              : v_d->d_next_o_id;

  const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
  const new_order::value v_no;
  const size_t new_order_sz = Size(v_no);
  rc = tbl_new_order(warehouse_id)
           ->InsertRecord(txn, Encode(str(Size(k_no)), k_no),
                          Encode(str(new_order_sz), v_no));
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  if (!g_new_order_fast_id_gen) {
    district::value v_d_new(*v_d);
    v_d_new.d_next_o_id++;
    rc = tbl_district(warehouse_id)
             ->UpdateRecord(txn, Encode(str(Size(k_d)), k_d),
                            Encode(str(Size(v_d_new)), v_d_new));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
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
  rc = tbl_oorder(warehouse_id)
           ->InsertRecord(txn, Encode(str(Size(k_oo)), k_oo),
                          Encode(str(oorder_sz), v_oo), &v_oo_oid);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID,
                                      k_no.no_o_id);
  rc = tbl_oorder_c_id_idx(warehouse_id)
           ->InsertOID(txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];

    const item::key k_i(ol_i_id);
    item::value v_i_temp;

    rc = rc_t{RC_INVALID};
    tbl_item(1)->GetRecord(txn, rc, Encode(str(Size(k_i)), k_i), valptr);
    // TryVerifyRelaxed
    LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
      << "Wrong return value " << rc._val;
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

    const item::value *v_i = Decode(valptr, v_i_temp);
#ifndef NDEBUG
    checker::SanityCheckItem(&k_i, v_i);
#endif

    const stock::key k_s(ol_supply_w_id, ol_i_id);
    stock::value v_s_temp;

    rc = co_await tbl_stock(ol_supply_w_id)->coro_GetRecord(txn, Encode(str(Size(k_s)), k_s), valptr);
    // TryVerifyRelaxed
    LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
      << "Wrong return value " << rc._val;
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

    const stock::value *v_s = Decode(valptr, v_s_temp);
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

    rc = tbl_stock(ol_supply_w_id)
             ->UpdateRecord(txn, Encode(str(Size(k_s)), k_s),
                            Encode(str(Size(v_s_new)), v_s_new));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }


    const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                               ol_number);
    order_line::value v_ol;
    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0;  // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
    v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
    v_ol.ol_quantity = int8_t(ol_quantity);

    const size_t order_line_sz = Size(v_ol);
    rc = tbl_order_line(warehouse_id)
             ->InsertRecord(txn, Encode(str(Size(k_ol)), k_ol),
                            Encode(str(order_line_sz), v_ol));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
  }

  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
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

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_delivery(uint32_t idx, ermia::epoch_num begin_epoch) {
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

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
  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
    const new_order::key k_no_1(warehouse_id, d,
                                std::numeric_limits<int32_t>::max());
    new_order_scan_callback new_order_c;
    {
      rc = tbl_new_order(warehouse_id)
               ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                      &Encode(str(Size(k_no_1)), k_no_1), new_order_c,
                      arena);
      // TryCatch
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
    }

    const new_order::key *k_no = new_order_c.get_key();
    if (unlikely(!k_no)) continue;
    last_no_o_ids[d - 1] = k_no->no_o_id + 1;  // XXX: update last seen

    const oorder::key k_oo(warehouse_id, d, k_no->no_o_id);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    oorder::value v_oo_temp;
    ermia::varstr valptr;
    rc = rc_t{RC_INVALID};
    tbl_oorder(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr);
    // TryCatchCondAbort
    if (rc.IsAbort() or rc._val == RC_FALSE) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

    const oorder::value *v_oo = Decode(valptr, v_oo_temp);
#ifndef NDEBUG
    checker::SanityCheckOOrder(&k_oo, v_oo);
#endif

    static_limit_callback<15> c(arena, false);  // never more than 15 order_lines per order
    const order_line::key k_oo_0(warehouse_id, d, k_no->no_o_id, 0);
    const order_line::key k_oo_1(warehouse_id, d, k_no->no_o_id,
                                 std::numeric_limits<int32_t>::max());

    // XXX(stephentu): mutable scans would help here
    rc = tbl_order_line(warehouse_id)
             ->Scan(txn, Encode(str(Size(k_oo_0)), k_oo_0),
                    &Encode(str(Size(k_oo_1)), k_oo_1), c, arena);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

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
      ASSERT(arena->manages(c.values[i].first));
      rc = co_await tbl_order_line(warehouse_id)
               ->coro_UpdateRecord(txn, *c.values[i].first,
                              Encode(str(Size(v_ol_new)), v_ol_new));
      // TryCatch
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
    }

    // delete new order
    rc = tbl_new_order(warehouse_id)
             ->RemoveRecord(txn, Encode(str(Size(*k_no)), *k_no));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

    // update oorder
    oorder::value v_oo_new(*v_oo);
    v_oo_new.o_carrier_id = o_carrier_id;
    rc =tbl_oorder(warehouse_id)
            ->UpdateRecord(txn, Encode(str(Size(k_oo)), k_oo),
                           Encode(str(Size(v_oo_new)), v_oo_new));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

    const uint c_id = v_oo->o_c_id;
    const float ol_total = sum;

    // update customer
    const customer::key k_c(warehouse_id, d, c_id);
    customer::value v_c_temp;

    rc = rc_t{RC_INVALID};
    tbl_customer(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
    // TryVerifyRelaxed
    LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
      << "Wrong return value " << rc._val;
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

    const customer::value *v_c = Decode(valptr, v_c_temp);
    customer::value v_c_new(*v_c);
    v_c_new.c_balance += ol_total;
    rc = tbl_customer(warehouse_id)
             ->UpdateRecord(txn, Encode(str(Size(k_c)), k_c),
                            Encode(str(Size(v_c_new)), v_c_new));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
  }
  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
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
  credit_check_order_line_scan_callback(ermia::str_arena *arena) : _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    ermia::varstr *pv = _arena->next(0);  // header only
    pv->p = value.p;
    pv->l = value.l;
    _v_ol.emplace_back(pv);
    return true;
  }
  std::vector<const ermia::varstr *> _v_ol;
  ermia::str_arena *_arena;
};

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_credit_check(uint32_t idx, ermia::epoch_num begin_epoch) {
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
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

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

  // select * from customer with random C_ID
  customer::key k_c;
  customer::value v_c_temp;
  ermia::varstr valptr;
  const uint customerID = GetCustomerId(r);
  k_c.c_w_id = customerWarehouseID;
  k_c.c_d_id = customerDistrictID;
  k_c.c_id = customerID;

  tbl_customer(customerWarehouseID)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val; 
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const customer::value *v_c = Decode(valptr, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  // scan order
  //		c_w_id = :w_id;
  //		c_d_id = :d_id;
  //		c_id = :c_id;
  credit_check_order_scan_callback c_no(arena);
  const new_order::key k_no_0(warehouse_id, districtID, 0);
  const new_order::key k_no_1(warehouse_id, districtID,
                              std::numeric_limits<int32_t>::max());
  rc = tbl_new_order(warehouse_id)
           ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                  &Encode(str(Size(k_no_1)), k_no_1), c_no, arena);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  ALWAYS_ASSERT(c_no.output.size());

  double sum = 0;
  for (auto &k : c_no.output) {
    new_order::key k_no_temp;
    const new_order::key *k_no = Decode(*k, k_no_temp);

    const oorder::key k_oo(warehouse_id, districtID, k_no->no_o_id);
    oorder::value v;
    rc = rc_t{RC_INVALID};
    tbl_oorder(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr);
    // TryCatchCond
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
    if (rc._val == RC_FALSE)
      continue;
    // Order line scan
    //		ol_d_id = :d_id
    //		ol_w_id = :w_id
    //		ol_o_id = o_id
    //		ol_number = 1-15
    static thread_local credit_check_order_line_scan_callback c_ol(arena);
    c_ol._v_ol.clear();
    const order_line::key k_ol_0(warehouse_id, districtID, k_no->no_o_id, 1);
    const order_line::key k_ol_1(warehouse_id, districtID, k_no->no_o_id, 15);
    rc = tbl_order_line(warehouse_id)
             ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                    &Encode(str(Size(k_ol_1)), k_ol_1), c_ol, arena);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
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
  rc = tbl_customer(customerWarehouseID)
           ->UpdateRecord(txn, Encode(str(Size(k_c)), k_c),
                          Encode(str(Size(v_c_new)), v_c_new));
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
}

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_payment(uint32_t idx, ermia::epoch_num begin_epoch) {
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

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
  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;
  ermia::varstr valptr;

  tbl_warehouse(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_w)), k_w), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val; 
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const warehouse::value *v_w = Decode(valptr, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  warehouse::value v_w_new(*v_w);
  v_w_new.w_ytd += paymentAmount;
  rc = tbl_warehouse(warehouse_id)
           ->UpdateRecord(txn, Encode(str(Size(k_w)), k_w),
                          Encode(str(Size(v_w_new)), v_w_new));
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;

  rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val; 
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  district::value v_d_new(*v_d);
  v_d_new.d_ytd += paymentAmount;
  rc = tbl_district(warehouse_id)
           ->UpdateRecord(txn, Encode(str(Size(k_d)), k_d),
                          Encode(str(Size(v_d_new)), v_d_new));
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  customer::key k_c;
  customer::value v_c;
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

    static_limit_callback<NMaxCustomerIdxScanElems> c(arena, true);  // probably a safe bet for now
    rc = tbl_customer_name_idx(customerWarehouseID)
             ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                    &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c, arena);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

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
    rc = rc_t{RC_INVALID};
    tbl_customer(customerWarehouseID)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
    // TryVerifyRelaxed
    LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
      << "Wrong return value " << rc._val; 
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
    Decode(valptr, v_c);
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

  rc = tbl_customer(customerWarehouseID)
           ->UpdateRecord(txn, Encode(str(Size(k_c)), k_c),
                          Encode(str(Size(v_c_new)), v_c_new));
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID,
                         warehouse_id, ts);
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *)v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s", v_w->w_name.c_str(), v_d->d_name.c_str());
  v_h.h_data.resize_junk(
      std::min(static_cast<size_t>(n), v_h.h_data.max_size()));

  rc = tbl_history(warehouse_id)
           ->InsertRecord(txn, Encode(str(Size(k_h)), k_h),
                          Encode(str(Size(v_h)), v_h));
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
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

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_order_status(uint32_t idx, ermia::epoch_num begin_epoch) {
  const uint64_t read_only_mask =
      ermia::config::enable_safesnap ? ermia::transaction::TXN_FLAG_READ_ONLY : 0;
  // NB: since txn_order_status() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH | read_only_mask, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

  const uint warehouse_id = pick_wh(r);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 13
  //   max_read_set_size : 81
  //   max_write_set_size : 0
  //   num_txn_contexts : 4
  customer::key k_c;
  customer::value v_c;
  ermia::varstr valptr;
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

    static_limit_callback<NMaxCustomerIdxScanElems> c(arena, true);  // probably a safe bet for now
    rc = tbl_customer_name_idx(warehouse_id)
             ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                    &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c, arena);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
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

    rc_t rc = rc_t{RC_INVALID};
    tbl_customer(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
    // TryVerifyRelaxed
    LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
      << "Wrong return value " << rc._val; 
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
    Decode(valptr, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif

  oorder_c_id_idx::value sv;
  ermia::varstr *newest_o_c_id = arena->next(Size(sv));
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
      rc = tbl_oorder_c_id_idx(warehouse_id)
               ->Scan(txn, Encode(str(Size(k_oo_idx_0)), k_oo_idx_0),
                      &Encode(str(Size(k_oo_idx_1)), k_oo_idx_1), c_oorder, arena);
      // TryCatch
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
    }
    ALWAYS_ASSERT(c_oorder.size());
  } else {
    latest_key_callback c_oorder(*newest_o_c_id, 1);
    const oorder_c_id_idx::key k_oo_idx_hi(warehouse_id, districtID, k_c.c_id,
                                           std::numeric_limits<int32_t>::max());
    rc = tbl_oorder_c_id_idx(warehouse_id)
             ->ReverseScan(txn, Encode(str(Size(k_oo_idx_hi)), k_oo_idx_hi),
                           nullptr, c_oorder, arena);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
    ALWAYS_ASSERT(c_oorder.size() == 1);
  }

  oorder_c_id_idx::key k_oo_idx_temp;
  const oorder_c_id_idx::key *k_oo_idx = Decode(*newest_o_c_id, k_oo_idx_temp);
  const uint o_id = k_oo_idx->o_o_id;

  order_line_nop_callback c_order_line;
  const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, o_id,
                               std::numeric_limits<int32_t>::max());
  rc = tbl_order_line(warehouse_id)
           ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                  &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line, arena);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);

  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
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

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_stock_level(uint32_t idx, ermia::epoch_num begin_epoch) {
  const uint64_t read_only_mask =
      ermia::config::enable_safesnap ? ermia::transaction::TXN_FLAG_READ_ONLY : 0;
  // NB: since txn_stock_level() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH | read_only_mask, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

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
  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr valptr;

  tbl_district(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  // TryVerifyRelaxed
  LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
    << "Wrong return value " << rc._val; 
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

  const district::value *v_d = Decode(valptr, v_d_temp);
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
    rc = tbl_order_line(warehouse_id)
             ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                    &Encode(str(Size(k_ol_1)), k_ol_1), c, arena);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
  }
  {
    std::unordered_map<uint, bool> s_i_ids_distinct;
    for (auto &p : c.s_i_ids) {
      const stock::key k_s(warehouse_id, p.first);
      stock::value v_s;
      ASSERT(p.first >= 1 && p.first <= NumItems());

      rc = co_await tbl_stock(warehouse_id)->coro_GetRecord(txn, Encode(str(Size(k_s)), k_s), valptr);
      // TryVerifyRelaxed
      LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
        << "Wrong return value " << rc._val; 
      if (rc.IsAbort()) {
        db->Abort(txn);
        if (rc.IsAbort())
          co_return rc;
        else
          co_return {RC_ABORT_USER};
      }
      const uint8_t *ptr = (const uint8_t *)valptr.data();
      int16_t i16tmp;
      ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
      if (i16tmp < int(threshold)) s_i_ids_distinct[p.first] = 1;
    }
    // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
  }
  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
}

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_query2(uint32_t idx, ermia::epoch_num begin_epoch) {
  ermia::transaction *txn = &transactions[idx];
  // FIXME(yongjunh): use TXN_FLAG_READ_MOSTLY for SSN
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

  static thread_local tpcc_table_scanner r_scanner(arena);
  r_scanner.clear();
  const region::key k_r_0(0);
  const region::key k_r_1(5);
  rc = tbl_region(1)->Scan(txn, Encode(str(sizeof(k_r_0)), k_r_0),
                           &Encode(str(sizeof(k_r_1)), k_r_1), r_scanner, arena);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  ALWAYS_ASSERT(r_scanner.output.size() == 5);

  static thread_local tpcc_table_scanner n_scanner(arena);
  n_scanner.clear();
  const nation::key k_n_0(0);
  const nation::key k_n_1(std::numeric_limits<int32_t>::max());
  rc = tbl_nation(1)->Scan(txn, Encode(str(sizeof(k_n_0)), k_n_0),
                           &Encode(str(sizeof(k_n_1)), k_n_1), n_scanner, arena);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }

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
        ermia::varstr valptr;

        rc_t rc = rc_t{RC_INVALID};
        tbl_supplier(1)->GetRecord(txn, rc, Encode(str(Size(k_su)), k_su), valptr);
        // TryVerifyRelaxed
        LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
          << "Wrong return value " << rc._val; 
        if (rc.IsAbort()) {
          db->Abort(txn);
          if (rc.IsAbort())
            co_return rc;
          else
            co_return {RC_ABORT_USER};
        }
        const supplier::value *v_su = Decode(valptr, v_su_tmp);

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
          rc = co_await tbl_stock(it.first)->coro_GetRecord(txn, Encode(str(Size(k_s)), k_s), valptr);
          // TryVerifyRelaxed
          LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
            << "Wrong return value " << rc._val; 
          if (rc.IsAbort()) {
            db->Abort(txn);
            if (rc.IsAbort())
              co_return rc;
            else
              co_return {RC_ABORT_USER};
          }
          const stock::value *v_s = Decode(valptr, v_s_tmp);

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
        rc = rc_t{RC_INVALID};
        tbl_item(1)->GetRecord(txn, rc, Encode(str(Size(k_i)), k_i), valptr);
        // TryVerifyRelaxed
        LOG_IF(FATAL, rc._val != RC_TRUE && !rc.IsAbort()) \
          << "Wrong return value " << rc._val; 
        if (rc.IsAbort()) {
          db->Abort(txn);
          if (rc.IsAbort())
            co_return rc;
          else
            co_return {RC_ABORT_USER};
        }
        const item::value *v_i = Decode(valptr, v_i_temp);
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
          rc = tbl_stock(min_k_s.s_w_id)
                   ->UpdateRecord(txn, Encode(str(Size(min_k_s)), min_k_s),
                                  Encode(str(Size(new_v_s)), new_v_s));
          // TryCatch
          if (rc.IsAbort()) {
            db->Abort(txn);
            if (rc.IsAbort())
              co_return rc;
            else
              co_return {RC_ABORT_USER};
          }
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

  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
}

ermia::dia::generator<rc_t> tpcc_cs_worker::txn_microbench_random(uint32_t idx, ermia::epoch_num begin_epoch) {
  ermia::transaction *txn = &transactions[idx];
  new (txn) ermia::transaction(ermia::transaction::TXN_FLAG_CSWITCH, *arena);
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  xc->begin_epoch = begin_epoch;
  rc_t rc = rc_t{RC_INVALID};

  uint start_w = 0, start_s = 0;
  ASSERT(NumWarehouses() * NumItems() >= g_microbench_rows);

  // pick start row, if it's not enough, later wrap to the first row
  uint w = start_w = RandomNumber(r, 1, NumWarehouses());
  uint s = start_s = RandomNumber(r, 1, NumItems());
  
  // read rows
  ermia::varstr sv;
  for (uint i = 0; i < g_microbench_rows; i++) {
    const stock::key k_s(w, s);
    DLOG(INFO) << "rd " << w << " " << s;
    rc = rc_t{RC_INVALID};
    tbl_stock(w)->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), sv);
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }

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
    rc = tbl_stock(ww)->UpdateRecord(txn, Encode(str(Size(k_s)), k_s),
                                     Encode(str(Size(v)), v));
    // TryCatch
    if (rc.IsAbort()) {
      db->Abort(txn);
      if (rc.IsAbort())
        co_return rc;
      else
        co_return {RC_ABORT_USER};
    }
  }

  DLOG(INFO) << "micro-random finished";
#ifndef NDEBUG
  abort();
#endif

  rc = db->Commit(txn);
  // TryCatch
  if (rc.IsAbort()) {
    db->Abort(txn);
    if (rc.IsAbort())
      co_return rc;
    else
      co_return {RC_ABORT_USER};
  }
  co_return {RC_TRUE};
}

class tpcc_cs_bench_runner : public bench_runner {
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
  tpcc_cs_bench_runner(ermia::Engine *db) : bench_runner(db) {
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
        ret.push_back(new tpcc_cs_worker(i, r.next(), db, open_tables, partitions,
                                         &barrier_a, &barrier_b,
                                         (i % NumWarehouses()) + 1));
    } else {
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        ret.push_back(new tpcc_cs_worker(i, r.next(), db, open_tables, partitions,
                                         &barrier_a, &barrier_b, i + 1));
      }
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_cmdlog_redoers() {
    ALWAYS_ASSERT(ermia::config::is_backup_srv() && ermia::config::command_log);
    std::vector<bench_worker *> ret;
    return ret;
  }

 private:
  std::map<std::string, std::vector<ermia::OrderedIndex *>> partitions;
};

void tpcc_cs_do_test(ermia::Engine *db, int argc, char **argv) {
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
      if (find(tpcc_cs_worker::hot_whs.begin(), tpcc_cs_worker::hot_whs.end(), w) ==
          tpcc_cs_worker::hot_whs.end())
        tpcc_cs_worker::hot_whs.push_back(w);
      else
        goto try_push;
    }

    for (uint i = 1; i <= NumWarehouses(); i++) {
      if (find(tpcc_cs_worker::hot_whs.begin(), tpcc_cs_worker::hot_whs.end(), i) ==
          tpcc_cs_worker::hot_whs.end())
        tpcc_cs_worker::cold_whs.push_back(i);
    }
    ALWAYS_ASSERT(tpcc_cs_worker::cold_whs.size() + tpcc_cs_worker::hot_whs.size() ==
                  NumWarehouses());
  }

  if (ermia::config::verbose) {
    std::cerr << "tpcc settings:" << std::endl;
    if (g_wh_temperature) {
      std::cerr << "  hot whs for 80% accesses     :";
      for (uint i = 0; i < tpcc_cs_worker::hot_whs.size(); i++)
        std::cerr << " " << tpcc_cs_worker::hot_whs[i];
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

  tpcc_cs_bench_runner r(db);
  r.run();
}
#endif // ADV_COROUTINE

