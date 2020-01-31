#pragma once

#include "record/encoder.h"
#include "record/inline_str.h"
#include "../macros.h"

// These correspond to the their index in the workload desc vector
#define TPCC_CLID_NEW_ORDER 0
#define TPCC_CLID_PAYMENT   1
#define TPCC_CLID_DELIVERY  2

#define CUSTOMER_KEY_FIELDS(x, y) \
  x(int32_t, c_w_id) y(int32_t, c_d_id) y(int32_t, c_id)
#define CUSTOMER_VALUE_FIELDS(x, y)                                           \
  x(int32_t, c_id) y(float, c_discount) y(inline_str_fixed<2>, c_credit)      \
      y(inline_str_8<16>, c_last) y(inline_str_8<16>, c_first) y(             \
          float, c_credit_lim) y(float, c_balance) y(float, c_ytd_payment)    \
          y(int32_t, c_payment_cnt) y(int32_t, c_delivery_cnt)                \
              y(inline_str_8<20>, c_street_1) y(inline_str_8<20>, c_street_2) \
                  y(inline_str_8<20>, c_city) y(inline_str_fixed<2>, c_state) \
                      y(inline_str_fixed<9>, c_zip)                           \
                          y(inline_str_fixed<16>, c_phone)                    \
                              y(uint32_t, c_since)                            \
                                  y(inline_str_fixed<2>, c_middle)            \
                                      y(inline_str_16<500>, c_data)
DO_STRUCT(customer, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS)

#define CUSTOMER_NAME_IDX_KEY_FIELDS(x, y)                              \
  x(int32_t, c_w_id) y(int32_t, c_d_id) y(inline_str_fixed<16>, c_last) \
      y(inline_str_fixed<16>, c_first)
#define CUSTOMER_NAME_IDX_VALUE_FIELDS(x, y) x(int8_t, dummy)
DO_STRUCT(customer_name_idx, CUSTOMER_NAME_IDX_KEY_FIELDS,
          CUSTOMER_NAME_IDX_VALUE_FIELDS)

#define DISTRICT_KEY_FIELDS(x, y) x(int32_t, d_w_id) y(int32_t, d_id)
#define DISTRICT_VALUE_FIELDS(x, y)                                   \
  x(float, d_ytd) y(float, d_tax) y(int32_t, d_next_o_id)             \
      y(inline_str_8<10>, d_name) y(inline_str_8<20>, d_street_1)     \
          y(inline_str_8<20>, d_street_2) y(inline_str_8<20>, d_city) \
              y(inline_str_fixed<2>, d_state) y(inline_str_fixed<9>, d_zip)
DO_STRUCT(district, DISTRICT_KEY_FIELDS, DISTRICT_VALUE_FIELDS)

#define HISTORY_KEY_FIELDS(x, y)                               \
  x(int32_t, h_c_id) y(int32_t, h_c_d_id) y(int32_t, h_c_w_id) \
      y(int32_t, h_d_id) y(int32_t, h_w_id) y(uint32_t, h_date)
#define HISTORY_VALUE_FIELDS(x, y) \
  x(float, h_amount) y(inline_str_8<24>, h_data)
DO_STRUCT(history, HISTORY_KEY_FIELDS, HISTORY_VALUE_FIELDS)

#define ITEM_KEY_FIELDS(x, y) x(int32_t, i_id)
#define ITEM_VALUE_FIELDS(x, y)                                             \
  x(inline_str_8<24>, i_name) y(float, i_price) y(inline_str_8<50>, i_data) \
      y(int32_t, i_im_id)
DO_STRUCT(item, ITEM_KEY_FIELDS, ITEM_VALUE_FIELDS)

#define NEW_ORDER_KEY_FIELDS(x, y) \
  x(int32_t, no_w_id) y(int32_t, no_d_id) y(int32_t, no_o_id)
// need dummy b/c our btree cannot have empty values.
// we also size value so that it can fit a key
#define NEW_ORDER_VALUE_FIELDS(x, y) x(inline_str_fixed<12>, no_dummy)
DO_STRUCT(new_order, NEW_ORDER_KEY_FIELDS, NEW_ORDER_VALUE_FIELDS)

#define OORDER_KEY_FIELDS(x, y) \
  x(int32_t, o_w_id) y(int32_t, o_d_id) y(int32_t, o_id)
#define OORDER_VALUE_FIELDS(x, y)                                 \
  x(int32_t, o_c_id) y(int32_t, o_carrier_id) y(int8_t, o_ol_cnt) \
      y(bool, o_all_local) y(uint32_t, o_entry_d)
DO_STRUCT(oorder, OORDER_KEY_FIELDS, OORDER_VALUE_FIELDS)

#define OORDER_C_ID_IDX_KEY_FIELDS(x, y) \
  x(int32_t, o_w_id) y(int32_t, o_d_id) y(int32_t, o_c_id) y(int32_t, o_o_id)
#define OORDER_C_ID_IDX_VALUE_FIELDS(x, y) x(uint8_t, dummy)
DO_STRUCT(oorder_c_id_idx, OORDER_C_ID_IDX_KEY_FIELDS,
          OORDER_C_ID_IDX_VALUE_FIELDS)

#define ORDER_LINE_KEY_FIELDS(x, y)                           \
  x(int32_t, ol_w_id) y(int32_t, ol_d_id) y(int32_t, ol_o_id) \
      y(int32_t, ol_number)
#define ORDER_LINE_VALUE_FIELDS(x, y)                                \
  x(int32_t, ol_i_id) y(uint32_t, ol_delivery_d) y(float, ol_amount) \
      y(int32_t, ol_supply_w_id) y(int8_t, ol_quantity)
DO_STRUCT(order_line, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_FIELDS)

#define STOCK_KEY_FIELDS(x, y) x(int32_t, s_w_id) y(int32_t, s_i_id)
#define STOCK_VALUE_FIELDS(x, y)                                 \
  x(int16_t, s_quantity) y(float, s_ytd) y(int32_t, s_order_cnt) \
      y(int32_t, s_remote_cnt)
DO_STRUCT(stock, STOCK_KEY_FIELDS, STOCK_VALUE_FIELDS)

#define STOCK_DATA_KEY_FIELDS(x, y) x(int32_t, s_w_id) y(int32_t, s_i_id)
#define STOCK_DATA_VALUE_FIELDS(x, y)                                       \
  x(inline_str_8<50>, s_data) y(inline_str_fixed<24>, s_dist_01)            \
      y(inline_str_fixed<24>, s_dist_02) y(inline_str_fixed<24>, s_dist_03) \
          y(inline_str_fixed<24>, s_dist_04)                                \
              y(inline_str_fixed<24>, s_dist_05)                            \
                  y(inline_str_fixed<24>, s_dist_06)                        \
                      y(inline_str_fixed<24>, s_dist_07)                    \
                          y(inline_str_fixed<24>, s_dist_08)                \
                              y(inline_str_fixed<24>, s_dist_09)            \
                                  y(inline_str_fixed<24>, s_dist_10)
DO_STRUCT(stock_data, STOCK_DATA_KEY_FIELDS, STOCK_DATA_VALUE_FIELDS)

#define WAREHOUSE_KEY_FIELDS(x, y) x(int32_t, w_id)
#define WAREHOUSE_VALUE_FIELDS(x, y)                                  \
  x(float, w_ytd) y(float, w_tax) y(inline_str_8<10>, w_name)         \
      y(inline_str_8<20>, w_street_1) y(inline_str_8<20>, w_street_2) \
          y(inline_str_8<20>, w_city) y(inline_str_fixed<2>, w_state) \
              y(inline_str_fixed<9>, w_zip)
DO_STRUCT(warehouse, WAREHOUSE_KEY_FIELDS, WAREHOUSE_VALUE_FIELDS)

#define NATION_KEY_FIELDS(x, y) x(int32_t, n_nationkey)
#define NATION_VALUE_FIELDS(x, y)                         \
  x(inline_str_fixed<25>, n_name) y(int32_t, n_regionkey) \
      y(inline_str_fixed<152>, n_comment)
DO_STRUCT(nation, NATION_KEY_FIELDS, NATION_VALUE_FIELDS)

#define REGION_KEY_FIELDS(x, y) x(int32_t, r_regionkey)
#define REGION_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<25>, r_name) y(inline_str_fixed<152>, r_comment)
DO_STRUCT(region, REGION_KEY_FIELDS, REGION_VALUE_FIELDS)

#define SUPPLIER_KEY_FIELDS(x, y) x(int32_t, su_suppkey)
#define SUPPLIER_VALUE_FIELDS(x, y)                                    \
  x(inline_str_fixed<25>, su_name) y(inline_str_fixed<40>, su_address) \
      y(int32_t, su_nationkey) y(inline_str_fixed<15>, su_phone)       \
          y(int32_t, su_acctbal) y(inline_str_fixed<40>, su_comment)
DO_STRUCT(supplier, SUPPLIER_KEY_FIELDS, SUPPLIER_VALUE_FIELDS)

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

static const char *regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

typedef std::vector<std::vector<std::pair<int32_t, int32_t>>> SuppStockMap;
static SuppStockMap supp_stock_map(10000);  // value ranges 0 ~ 9999 ( modulo by 10k )

struct eqstr {
  bool operator()(const char *s1, const char *s2) const {
    return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
  }
};

#define TPCC_TABLE_LIST(x)                                                     \
  x(customer) x(customer_name_idx) x(district) x(history) x(item) x(new_order) \
      x(oorder) x(oorder_c_id_idx) x(order_line) x(stock) x(stock_data)        \
          x(nation) x(region) x(supplier) x(warehouse)

class tpcc_table_scanner : public ermia::OrderedIndex::ScanCallback {
 public:
  tpcc_table_scanner(ermia::str_arena *arena) : _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    ermia::varstr *const k = _arena->next(keylen);
    ermia::varstr *v = _arena->next(0);  // header only
    v->p = value.p;
    v->l = value.l;
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k, v);
    return true;
  }

  void clear() { output.clear(); }
  std::vector<std::pair<ermia::varstr *, const ermia::varstr *>> output;
  ermia::str_arena *_arena;
};

class dia_tpcc_table_scanner : public ermia::OrderedIndex::DiaScanCallback {
public:
  dia_tpcc_table_scanner(ermia::str_arena *arena) : _arena(arena) {}

  virtual bool Invoke(const char *keyp, size_t keylen, ermia::OID oid) {
    ermia::varstr *const k = _arena->atomic_next(keylen);
    ermia::varstr *v = _arena->atomic_next(0); // header only
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k, v);
    oids.emplace_back(oid);
    return true;
  }

  virtual bool Receive(ermia::transaction *t,
                       ermia::TableDescriptor *td) {
    for (int i = 0; i < oids.size(); ++i) {
      ermia::dbtuple *tuple = NULL;
      if (ermia::config::is_backup_srv()) {
        tuple = ermia::oidmgr->BackupGetVersion(
            td->GetTupleArray(),
            td->GetPersistentAddressArray(),
            ermia::volatile_read(oids[i]), t->GetXIDContext());
      } else {
        tuple = ermia::oidmgr->oid_get_version(td->GetTupleArray(),
                                               ermia::volatile_read(oids[i]),
                                               t->GetXIDContext());
      }
      if (tuple) {
        ermia::varstr value;
        if (t->DoTupleRead(tuple, &value)._val == RC_TRUE) {
          (output[i].second)->p = value.p;
          (output[i].second)->l = value.l;
          continue;
        }
      }
      return false;
    }
    return true;
  }

  void clear() {
    output.clear();
    oids.clear();
  }

  std::vector<std::pair<const ermia::varstr *, ermia::varstr *>> output;
  std::vector<ermia::OID> oids;

  private:
    ermia::str_arena *_arena;
};

struct _dummy {};  // exists so we can inherit from it, so we can use a macro in
                   // an init list...

