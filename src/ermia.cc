#include "dbcore/rcu.h"
#include "dbcore/sm-chkpt.h"
#include "dbcore/sm-cmd-log.h"
#include "dbcore/sm-dia.h"
#include "dbcore/sm-rep.h"

#include "ermia.h"
#include "txn.h"

namespace ermia {

// Engine initialization, including creating the OID, log, and checkpoint
// managers and recovery if needed.
Engine::Engine() {
  config::sanity_check();

  if (config::is_backup_srv()) {
    rep::BackupStartReplication();
  } else {
    if (!RCU::rcu_is_registered()) {
      RCU::rcu_register();
    }
    RCU::rcu_enter();

    ALWAYS_ASSERT(config::log_dir.size());
    ALWAYS_ASSERT(not logmgr);
    ALWAYS_ASSERT(not oidmgr);
    sm_log::allocate_log_buffer();
    logmgr = sm_log::new_log(config::recover_functor, nullptr);
    sm_oid_mgr::create();
    if (config::command_log) {
      CommandLog::cmd_log = new CommandLog::CommandLogManager();
    }
    ALWAYS_ASSERT(logmgr);
    ALWAYS_ASSERT(oidmgr);

    LSN chkpt_lsn = logmgr->get_chkpt_start();
    if (config::enable_chkpt) {
      chkptmgr = new sm_chkpt_mgr(chkpt_lsn);
    }

    // The backup will want to recover in another thread
    if (sm_log::need_recovery) {
      logmgr->recover();
    }
    RCU::rcu_exit();
  }
}

void Engine::CreateTable(uint16_t index_type, const char *name,
                         const char *primary_name) {
  IndexDescriptor *index_desc = nullptr;

  switch (index_type) {
  case kIndexConcurrentMasstree:
    index_desc =
        (new ConcurrentMasstreeIndex(name, primary_name))->GetDescriptor();
    break;
  case kIndexDecoupledMasstree:
    index_desc =
        (new DecoupledMasstreeIndex(name, primary_name))->GetDescriptor();
    break;
  case kIndexSingleThreadedBTree:
    index_desc = (new SingleThreadedBTree(name, primary_name))->GetDescriptor();
    break;
  default:
    LOG(FATAL) << "Wrong index type: " << index_type;
    break;
  }

  if (!sm_log::need_recovery && !config::is_backup_srv()) {
    ASSERT(ermia::logmgr);
    auto create_file = [=](char *) {
      ermia::RCU::rcu_enter();
      DEFER(ermia::RCU::rcu_exit());
      ermia::sm_tx_log *log = ermia::logmgr->new_tx_log();

      index_desc->Initialize();
      log->log_index(index_desc->GetTupleFid(), index_desc->GetKeyFid(),
                     index_desc->GetName());

      log->commit(nullptr);
    };

    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread.
    ermia::thread::Thread *thread = ermia::thread::GetThread(true);
    ALWAYS_ASSERT(thread);
    thread->StartTask(create_file);
    thread->Join();
    ermia::thread::PutThread(thread);
  }
}

rc_t ConcurrentMasstreeIndex::Scan(transaction *t, const varstr &start_key,
                                   const varstr *end_key,
                                   ScanCallback &callback, str_arena *arena) {
  MARK_REFERENCED(arena);
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (end_key) {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", " << util::hexify(*end_key) << ")" << std::endl);
  } else {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", +inf)" << std::endl);
  }

  if (!unlikely(end_key && *end_key <= start_key)) {
    XctSearchRangeCallback cb(t, &c);

    varstr uppervk;
    if (end_key) {
      uppervk = *end_key;
    }
    masstree_.search_range_call(start_key, end_key ? &uppervk : nullptr, cb,
                                t->xc);
  }
  return c.return_code;
}

rc_t ConcurrentMasstreeIndex::ReverseScan(transaction *t,
                                          const varstr &start_key,
                                          const varstr *end_key,
                                          ScanCallback &callback,
                                          str_arena *arena) {
  MARK_REFERENCED(arena);
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (!unlikely(end_key && start_key <= *end_key)) {
    XctSearchRangeCallback cb(t, &c);

    varstr lowervk;
    if (end_key) {
      lowervk = *end_key;
    }
    masstree_.rsearch_range_call(start_key, end_key ? &lowervk : nullptr, cb,
                                 t->xc);
  }
  return c.return_code;
}

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::Clear() {
  PurgeTreeWalker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

void ConcurrentMasstreeIndex::MultiGet(transaction *t,
                                       std::vector<ConcurrentMasstree::AMACState> &requests,
                                       std::vector<varstr *> &values) {
  ConcurrentMasstree::versioned_node_t sinfo;
  if (!t) {
    auto e = MM::epoch_enter();
    masstree_.search_amac(requests, e);
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    masstree_.search_amac(requests, t->xc->begin_epoch);
    if (config::is_backup_srv()) {
      for (uint32_t i = 0; i < requests.size(); ++i) {
        auto &r = requests[i];
        if (r.out_oid != INVALID_OID) {
          // Key-OID mapping exists, now try to get the actual tuple to be sure
          auto *tuple = oidmgr->BackupGetVersion(descriptor_->GetTupleArray(),
                                                 descriptor_->GetPersistentAddressArray(),
                                                 r.out_oid, t->xc);
          if (tuple) {
            t->DoTupleRead(tuple, values[i]);
          } else if (config::phantom_prot) {
            DoNodeRead(t, sinfo.first, sinfo.second);
          }
        }
      }
    } else if (!config::index_probe_only) {
      if (config::amac_version_chain) {
        // AMAC style version chain traversal
        thread_local std::vector<OIDAMACState> version_requests;
        version_requests.clear();
        for (auto &s : requests) {
          version_requests.emplace_back(s.out_oid);
        }
        oidmgr->oid_get_version_amac(descriptor_->GetTupleArray(), version_requests, t->xc);
        uint32_t i = 0;
        for (auto &vr: version_requests) {
          if (vr.tuple) {
            t->DoTupleRead(vr.tuple, values[i++]);
          } else if (config::phantom_prot) {
            DoNodeRead(t, sinfo.first, sinfo.second);
          }
        }
      } else {
        for (uint32_t i = 0; i < requests.size(); ++i) {
          auto &r = requests[i];
          if (r.out_oid != INVALID_OID) {
            auto *tuple = oidmgr->oid_get_version(descriptor_->GetTupleArray(), r.out_oid, t->xc);
            if (tuple) {
              t->DoTupleRead(tuple, values[i]);
            } else if (config::phantom_prot) {
              DoNodeRead(t, sinfo.first, sinfo.second);
            }
          }
        }
      }
    }
  }
}

void ConcurrentMasstreeIndex::Get(transaction *t, rc_t &rc, const varstr &key,
                                  varstr &value, OID *out_oid) {
  OID oid = 0;
  rc = {RC_INVALID};
  ConcurrentMasstree::versioned_node_t sinfo;

  if (!t) {
    auto e = MM::epoch_enter();
    rc._val = masstree_.search(key, oid, e, &sinfo) ? RC_TRUE : RC_FALSE;
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    bool found = masstree_.search(key, oid, t->xc->begin_epoch, &sinfo);

    dbtuple *tuple = nullptr;
    if (found) {
      // Key-OID mapping exists, now try to get the actual tuple to be sure
      if (config::is_backup_srv()) {
        tuple = oidmgr->BackupGetVersion(descriptor_->GetTupleArray(),
                                         descriptor_->GetPersistentAddressArray(),
                                         oid, t->xc);
      } else {
        tuple = oidmgr->oid_get_version(descriptor_->GetTupleArray(), oid, t->xc);
      }
      if (!tuple) {
        found = false;
      }
    }

    if (found) {
      if (out_oid) {
        *out_oid = oid;
      }
      volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
    } else if (config::phantom_prot) {
      volatile_write(rc._val, DoNodeRead(t, sinfo.first, sinfo.second)._val);
    } else {
      volatile_write(rc._val, RC_FALSE);
    }
    ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
  }

  if (out_oid) {
    *out_oid = oid;
  }
}

void ConcurrentMasstreeIndex::coro_MultiGet(
    transaction *t, std::vector<varstr *> &keys, std::vector<varstr *> &values,
    std::vector<ermia::OID> &oids,
    std::vector<ermia::dia::generator<bool> *> &coroutines) {
  t->ensure_active();
  ConcurrentMasstree::threadinfo ti(t->GetXIDContext()->begin_epoch);
  ConcurrentMasstree::versioned_node_t sinfo;

  int finished = 0;
  for (int i = 0; i < keys.size(); ++i) {
    coroutines.emplace_back(new ermia::dia::generator<bool>(
        masstree_.search_coro(*keys[i], oids[i], ti, &sinfo)));
  }

  while (finished < coroutines.size()) {
    for (auto &c : coroutines) {
      if (c && !c->advance()) {
        delete c;
        c = nullptr;
        ++finished;
      }
    }
  }
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_begin(
    const typename ConcurrentMasstree::node_opaque_t *n) {
  ASSERT(spec_values.empty());
  spec_values = ConcurrentMasstree::ExtractValues(n);
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_success() {
  spec_values.clear();
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_failure() {
  spec_values.clear();
}

bool ConcurrentMasstreeIndex::InsertIfAbsent(transaction *t, const varstr &key,
                                             OID oid) {
  typename ConcurrentMasstree::insert_info_t ins_info;
  bool inserted = masstree_.insert_if_absent(key, oid, t->xc, &ins_info);

  if (!inserted) {
    return false;
  }

  if (config::phantom_prot && !t->masstree_absent_set.empty()) {
    // Update node version number
    ASSERT(ins_info.node);
    auto it = t->masstree_absent_set.find(ins_info.node);
    if (it != t->masstree_absent_set.end()) {
      if (unlikely(it->second != ins_info.old_version)) {
        // Important: caller should unlink the version, otherwise we risk
        // leaving a dead version at chain head -> infinite loop or segfault...
        return false;
      }
      // otherwise, bump the version
      it->second = ins_info.new_version;
    }
  }
  return true;
}

void ConcurrentMasstreeIndex::ScanOID(transaction *t, const varstr &start_key,
                                      const varstr *end_key, rc_t &rc,
                                      OID *dia_callback) {
  SearchRangeCallback c(*(DiaScanCallback *)dia_callback);
  t->ensure_active();
  if (end_key) {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", " << util::hexify(*end_key) << ")" << std::endl);
  } else {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", +inf)" << std::endl);
  }

  int scancount = 0;
  if (!unlikely(end_key && *end_key <= start_key)) {
    XctSearchRangeCallback cb(t, &c);
    varstr uppervk;
    if (end_key) {
      uppervk = *end_key;
    }
    scancount = masstree_.search_range_oid(
        start_key, end_key ? &uppervk : nullptr, cb, t->xc);
  }
  volatile_write(rc._val, scancount ? RC_TRUE : RC_FALSE);
}

void ConcurrentMasstreeIndex::ReverseScanOID(transaction *t,
                                             const varstr &start_key,
                                             const varstr *end_key, rc_t &rc,
                                             OID *dia_callback) {
  SearchRangeCallback c(*(DiaScanCallback *)dia_callback);
  t->ensure_active();
  int scancount = 0;
  if (!unlikely(end_key && start_key <= *end_key)) {
    XctSearchRangeCallback cb(t, &c);
    varstr lowervk;
    if (end_key) {
      lowervk = *end_key;
    }
    scancount = masstree_.rsearch_range_oid(
        start_key, end_key ? &lowervk : nullptr, cb, t->xc);
  }
  volatile_write(rc._val, scancount ? RC_TRUE : RC_FALSE);
}

rc_t OrderedIndex::TryInsert(transaction &t, const varstr *k, varstr *v,
                             bool upsert, OID *inserted_oid) {
  if (t.TryInsertNewTuple(this, k, v, inserted_oid)) {
    return rc_t{RC_TRUE};
  } else if (!upsert) {
    return rc_t{RC_ABORT_INTERNAL};
  } else {
    return rc_t{RC_FALSE};
  }
}

rc_t ConcurrentMasstreeIndex::DoTreePut(transaction &t, const varstr *k,
                                        varstr *v, bool expect_new, bool upsert,
                                        OID *inserted_oid) {
  ASSERT(k);
  ASSERT((char *)k->data() == (char *)k + sizeof(varstr));
  ASSERT(!expect_new || v);
  t.ensure_active();

  if (expect_new) {
    rc_t rc = TryInsert(t, k, v, upsert, inserted_oid);
    if (rc._val != RC_FALSE) {
      return rc;
    }
  }

  // do regular search
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  GetOID(*k, rc, t.xc, oid);
  if (rc._val == RC_TRUE) {
    return t.Update(descriptor_, oid, k, v);
  } else {
    return rc_t{RC_ABORT_INTERNAL};
  }
}

rc_t ConcurrentMasstreeIndex::DoNodeRead(
    transaction *t, const ConcurrentMasstree::node_opaque_t *node,
    uint64_t version) {
  ALWAYS_ASSERT(config::phantom_prot);
  ASSERT(node);
  auto it = t->masstree_absent_set.find(node);
  if (it == t->masstree_absent_set.end()) {
    t->masstree_absent_set[node] = version;
  } else if (it->second != version) {
    return rc_t{RC_ABORT_PHANTOM};
  }
  return rc_t{RC_TRUE};
}

void ConcurrentMasstreeIndex::XctSearchRangeCallback::on_resp_node(
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
                    << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << ConcurrentMasstree::NodeStringify(n)
                    << std::endl);
  if (config::phantom_prot) {
#ifdef SSN
    if (t->flags & transaction::TXN_FLAG_READ_ONLY) {
      return;
    }
#endif
    rc_t rc = DoNodeRead(t, n, version);
    if (rc.IsAbort()) {
      caller_callback->return_code = rc;
    }
  }
}

bool ConcurrentMasstreeIndex::XctSearchRangeCallback::invoke(
    const ConcurrentMasstree *btr_ptr,
    const typename ConcurrentMasstree::string_type &k, dbtuple *v,
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  MARK_REFERENCED(btr_ptr);
  MARK_REFERENCED(n);
  MARK_REFERENCED(version);
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x"
                    << util::hexify(n) << ", version=" << version << ">"
                    << std::endl
                    << "  " << *((dbtuple *)v) << std::endl);
  varstr vv;
  caller_callback->return_code = t->DoTupleRead(v, &vv);
  if (caller_callback->return_code._val == RC_TRUE) {
    return caller_callback->Invoke(k, vv);
  } else if (caller_callback->return_code.IsAbort()) {
    // don't continue the read if the tx should abort
    // ^^^^^ note: see masstree_scan.hh, whose scan() calls
    // visit_value(), which calls this function to determine
    // if it should stop reading.
    return false; // don't continue the read if the tx should abort
  }
  return true;
}

bool ConcurrentMasstreeIndex::XctSearchRangeCallback::invoke(
    const typename ConcurrentMasstree::string_type &k, OID oid,
    uint64_t version) {
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x"
                    << util::hexify(n) << ", version=" << version << ">"
                    << std::endl
                    << " oid: " << oid << std::endl);
  return caller_callback->Invoke(k, oid);
}

void SingleThreadedBTree::Get(transaction *t, rc_t &rc, const varstr &key,
                              varstr &value, OID *oid) {
  t->ensure_active();

  // search the underlying btree to map key=>(btree_node|tuple)
  OID out_oid;
  bool found = btree_.Search((char *)key.data(), key.size(), &out_oid);

  if (oid) {
    *oid = out_oid;
  }

  if (found) {
    dbtuple *tuple = nullptr;
    auto *xc = t->GetXIDContext();
    // Look at version chain to read the actual tuple data
    if (config::is_backup_srv()) {
      tuple = oidmgr->BackupGetVersion(descriptor_->GetTupleArray(),
                                       descriptor_->GetPersistentAddressArray(),
                                       out_oid, xc);
    } else {
      tuple =
          oidmgr->oid_get_version(descriptor_->GetTupleArray(), out_oid, xc);
    }
    if (tuple) {
      rc = t->DoTupleRead(tuple, &value);
      return;
    }
  }
  rc = rc_t{RC_FALSE};
}

rc_t SingleThreadedBTree::DoTreePut(transaction &t, const varstr *k, varstr *v,
                                    bool expect_new, bool upsert,
                                    OID *inserted_oid) {
  t.ensure_active();

  if (expect_new) {
    rc_t rc = TryInsert(t, k, v, upsert, inserted_oid);
    if (rc._val != RC_FALSE) {
      return rc;
    }
  }

  dbtuple *bv = nullptr;
  OID oid = 0;
  if (btree_.Search((char *)k->data(), k->size(), &oid)) {
    return t.Update(descriptor_, oid, k, v);
  } else {
    return rc_t{RC_ABORT_INTERNAL};
  }
}

bool SingleThreadedBTree::InsertIfAbsent(transaction *t, const varstr &key,
                                         OID oid) {
  MARK_REFERENCED(t);
  return btree_.Insert((char *)key.data(), key.size(), oid);
  // TODO(tzwang): phantom protection
}

DecoupledMasstreeIndex::DecoupledMasstreeIndex(std::string name,
                                               const char *primary)
    : ConcurrentMasstreeIndex(name, primary) {}

void DecoupledMasstreeIndex::RecvGet(transaction *t, rc_t &rc, OID &oid,
                                     varstr &value) {
  // Wait for the traversal to finish
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    dbtuple *tuple = oidmgr->oid_get_version(
        descriptor_->GetTupleArray(), volatile_read(oid), t->GetXIDContext());
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
    if (descriptor_->IsPrimary()) {
      oidmgr->PrimaryTupleUnlink(descriptor_->GetTupleArray(), oid);
    }
    if (config::enable_chkpt) {
      volatile_write(descriptor_->GetKeyArray()->get(oid)->_ptr, 0);
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
      volatile_write(descriptor_->GetKeyArray()->get(value_oid)->_ptr, 0);
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
    rc = t->Update(descriptor_, oid, &key, &value);
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
    rc = t->Update(descriptor_, oid, &key, nullptr);
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
    if (!dia_callback.Receive(t, descriptor_))
      volatile_write(rc._val, RC_FALSE);
  }
}

void DecoupledMasstreeIndex::RecvReverseScan(transaction *t, rc_t &rc,
                                             DiaScanCallback &dia_callback) {
  while (volatile_read(rc._val) == RC_INVALID) {
  }
  if (rc._val == RC_TRUE) {
    if (!dia_callback.Receive(t, descriptor_))
      volatile_write(rc._val, RC_FALSE);
  }
}
} // namespace ermia
