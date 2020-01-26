#include "ermia_decouple.h"

DecoupledMasstreeIndex::DecoupledMasstreeIndex(std::string name,
                                               const char *primary)
    : ConcurrentMasstreeIndex(name.c_str(), primary != nullptr) {}

void DecoupledMasstreeIndex::RecvGet(transaction *t, rc_t &rc, OID &oid,
                                     varstr &value) {
  // Wait for the traversal to finish
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    dbtuple *tuple = oidmgr->oid_get_version(
        table_descriptor->GetTupleArray(), volatile_read(oid), t->GetXIDContext());
    if (tuple) {
      volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
    } else {
      volatile_write(rc._val, RC_FALSE);
    }
  }
}

void DecoupledMasstreeIndex::RecvInsert(transaction *t, rc_t &rc, OID oid,
                                        varstr &key, varstr &value,
                                        dbtuple *tuple) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    // key-OID installed successfully
    t->FinishInsert(this, oid, &key, &value, tuple);
    volatile_write(rc._val, RC_TRUE);
  } else {
    ASSERT(rc._val == RC_FALSE);
    if (is_primary) {
      oidmgr->PrimaryTupleUnlink(table_descriptor->GetTupleArray(), oid);
    }
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(oid)->_ptr, 0);
    }
    volatile_write(rc._val, RC_FALSE);
  }
}
// overload RecvInsert for secondary index
void DecoupledMasstreeIndex::RecvInsert(transaction *t, rc_t &rc, varstr &key,
                                        OID value_oid) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    // key-OID installed successfully
    t->FinishInsert(this, value_oid, &key, nullptr, nullptr);
    volatile_write(rc._val, RC_TRUE);
  } else {
    ASSERT(rc._val == RC_FALSE);
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(value_oid)->_ptr, 0);
    }
    volatile_write(rc._val, RC_FALSE);
  }
}

void DecoupledMasstreeIndex::RecvPut(transaction *t, rc_t &rc, OID &oid,
                                     const varstr &key, varstr &value) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  switch (rc._val) {
  case RC_TRUE:
    rc = t->Update(table_descriptor, oid, &key, &value);
    break;
  case RC_FALSE:
    rc._val = RC_ABORT_INTERNAL;
    break;
  default:
    LOG(FATAL) << "Wrong SendPut result";
  }
}

void DecoupledMasstreeIndex::RecvRemove(transaction *t, rc_t &rc, OID &oid,
                                        const varstr &key) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  switch (rc._val) {
  case RC_TRUE:
    rc = t->Update(table_descriptor, oid, &key, nullptr);
    break;
  case RC_FALSE:
    rc._val = RC_ABORT_INTERNAL;
    break;
  default:
    LOG(FATAL) << "Wrong SendRemove result";
  }
}

void DecoupledMasstreeIndex::RecvScan(transaction *t, rc_t &rc,
                                      DiaScanCallback &dia_callback) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    if (!dia_callback.Receive(t, table_descriptor))
      volatile_write(rc._val, RC_FALSE);
  }
}

void DecoupledMasstreeIndex::RecvReverseScan(transaction *t, rc_t &rc,
                                             DiaScanCallback &dia_callback) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    if (!dia_callback.Receive(t, table_descriptor))
      volatile_write(rc._val, RC_FALSE);
  }
}

