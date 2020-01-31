#pragma once
#include "ermia.h"

namespace ermia {

// User-facing masstree with decoupled index access
class DecoupledMasstreeIndex : public ConcurrentMasstreeIndex {
  friend class sm_log_recover_impl;
  friend class sm_chkpt_mgr;
  friend class dia::IndexThread;

public:
  DecoupledMasstreeIndex(std::string name, const char *primary);

  inline void SendGet(transaction *t, rc_t &rc, const varstr &key, OID *out_oid,
                      uint32_t idx_no) {
    ASSERT(out_oid);
    ermia::dia::SendGetRequest(t, this, &key, out_oid, &rc, idx_no);
  }
  void RecvGet(transaction *t, rc_t &rc, OID &oid, varstr &value);

  inline void SendInsert(transaction *t, rc_t &rc, const varstr &key,
                         varstr &value, OID *out_oid, dbtuple **out_tuple,
                         uint32_t idx_no) {
    ASSERT(out_oid);
    *out_tuple = nullptr;
    *out_oid = t->PrepareInsert(this, &value, out_tuple);
    ermia::dia::SendInsertRequest(t, this, &key, out_oid, &rc, idx_no);
  }
  void RecvInsert(transaction *t, rc_t &rc, OID oid, varstr &key, varstr &value,
                  dbtuple *tuple);
  // overload SendInsert for secondary index.
  inline void SendInsert(transaction *t, rc_t &rc, const varstr &key,
                         OID *value_oid, uint32_t idx_no) {
    // For secondary index, PrepareInsert just copy value_oid to oid
    // So ignore this step and send value_oid directly
    ermia::dia::SendInsertRequest(t, this, &key, value_oid, &rc, idx_no);
  }
  // overload RecvInsert for secondary index.
  void RecvInsert(transaction *t, rc_t &rc, varstr &key, OID value_oid);

  inline void SendPut(transaction *t, rc_t &rc, const varstr &key, OID *out_oid,
                      uint32_t idx_no) {
    SendGet(t, rc, key, out_oid, idx_no);
  }
  void RecvPut(transaction *t, rc_t &rc, OID &oid, const varstr &key,
               varstr &value);

  inline void SendRemove(transaction *t, rc_t &rc, const varstr &key,
                         OID *out_oid, uint32_t idx_no) {
    SendGet(t, rc, key, out_oid, idx_no);
  }
  void RecvRemove(transaction *t, rc_t &rc, OID &oid, const varstr &key);

  inline void SendScan(transaction *t, rc_t &rc, varstr &start_key,
                       varstr *end_key, DiaScanCallback &dia_callback,
                       uint32_t idx_no) {
    ermia::dia::SendScanRequest(t, this, &start_key, end_key,
                                (OID *)&dia_callback, &rc, idx_no);
  }
  void RecvScan(transaction *t, rc_t &rc, DiaScanCallback &dia_callback);

  inline void SendReverseScan(transaction *t, rc_t &rc, varstr &start_key,
                              varstr *end_key, DiaScanCallback &dia_callback,
                              uint32_t idx_no) {
    ermia::dia::SendReverseScanRequest(t, this, &start_key, end_key,
                                       (OID *)&dia_callback, &rc, idx_no);
  }
  void RecvReverseScan(transaction *t, rc_t &rc, DiaScanCallback &dia_callback);
};

}  // namespace ermia
