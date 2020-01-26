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
  }
}

TableDescriptor *Engine::CreateTable(const char *name) {
  auto *td = TableDescriptor::New(name);

  if (!sm_log::need_recovery && !config::is_backup_srv()) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    char *log_space = (char *)malloc(sizeof(sm_tx_log_impl));
    ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    td->Initialize();
    log->log_table(td->GetTupleFid(), td->GetKeyFid(), td->GetName());
    log->commit(nullptr);
    free(log_space);
  }
  return td;
}

void Engine::LogIndexCreation(bool primary, FID table_fid, FID index_fid, const std::string &index_name) {
  if (!sm_log::need_recovery && !config::is_backup_srv()) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    char *log_space = (char *)malloc(sizeof(sm_tx_log_impl));
    ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    log->log_index(table_fid, index_fid, index_name, primary);
    log->commit(nullptr);
    free(log_space);
  }
}

void Engine::CreateIndex(const char *table_name, const std::string &index_name, bool is_primary) {
  auto *td = TableDescriptor::Get(table_name);
  ALWAYS_ASSERT(td);
  auto *index = new ConcurrentMasstreeIndex(table_name, is_primary);
  if (is_primary) {
    td->SetPrimaryIndex(index, index_name);
  } else {
    td->AddSecondaryIndex(index, index_name);
  }
  FID index_fid = index->GetIndexFid();
  LogIndexCreation(is_primary, td->GetTupleFid(), index_fid, index_name);
}

PROMISE(rc_t) ConcurrentMasstreeIndex::Scan(transaction *t, const varstr &start_key,
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
    AWAIT masstree_.search_range_call(start_key, end_key ? &uppervk : nullptr, cb,
                                t->xc);
  }
  RETURN c.return_code;
}

PROMISE(rc_t) ConcurrentMasstreeIndex::ReverseScan(transaction *t,
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
    AWAIT masstree_.rsearch_range_call(start_key, end_key ? &lowervk : nullptr, cb,
                                 t->xc);
  }
  RETURN c.return_code;
}

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::Clear() {
  PurgeTreeWalker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

void ConcurrentMasstreeIndex::amac_MultiGet(
    transaction *t, std::vector<ConcurrentMasstree::AMACState> &requests,
    std::vector<varstr *> &values) {
#ifndef USE_STATIC_COROUTINE
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
          auto *tuple = oidmgr->BackupGetVersion(
              table_descriptor->GetTupleArray(),
              table_descriptor->GetPersistentAddressArray(), r.out_oid, t->xc);
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
        oidmgr->oid_get_version_amac(table_descriptor->GetTupleArray(),
                                     version_requests, t->xc);
        uint32_t i = 0;
        for (auto &vr : version_requests) {
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
            auto *tuple = oidmgr->oid_get_version(table_descriptor->GetTupleArray(),
                                                  r.out_oid, t->xc);
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
#endif
}

PROMISE(void) ConcurrentMasstreeIndex::GetRecord(transaction *t, rc_t &rc, const varstr &key,
                                        varstr &value, OID *out_oid) {
  OID oid = INVALID_OID;
  rc = {RC_INVALID};
  ConcurrentMasstree::versioned_node_t sinfo;

  if (!t) {
    auto e = MM::epoch_enter();
    rc._val = AWAIT masstree_.search(key, oid, e, &sinfo) ? RC_TRUE : RC_FALSE;
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    bool found = AWAIT masstree_.search(key, oid, t->xc->begin_epoch, &sinfo);

    dbtuple *tuple = nullptr;
    if (found) {
      // Key-OID mapping exists, now try to get the actual tuple to be sure
      if (config::is_backup_srv()) {
        tuple = oidmgr->BackupGetVersion(
            table_descriptor->GetTupleArray(),
            table_descriptor->GetPersistentAddressArray(), oid, t->xc);
      } else {
        tuple =
            AWAIT oidmgr->oid_get_version(table_descriptor->GetTupleArray(), oid, t->xc);
      }
      if (!tuple) {
        found = false;
      }
    }

    if (found) {
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

void ConcurrentMasstreeIndex::simple_coro_MultiGet(
    transaction *t, std::vector<varstr *> &keys, std::vector<varstr *> &values,
    std::vector<std::experimental::coroutine_handle<ermia::dia::generator<bool>::promise_type>> &handles) {
  auto e = MM::epoch_enter();
  ConcurrentMasstree::threadinfo ti(e);
  ConcurrentMasstree::versioned_node_t sinfo;

  OID oid = INVALID_OID;
  for (int i = 0; i < keys.size(); ++i) {
    handles[i] = masstree_.search_coro(*keys[i], oid, ti, &sinfo).get_handle();
  }

  int finished = 0;
  while (finished < handles.size()) {
    for (auto &h : handles) {
      if (h) {
        if (h.done()) {
          ++finished;
          h.destroy();
          h = nullptr;
        } else {
          h.resume();
        }
      }
    }
  }
  MM::epoch_exit(0, e);
}

#ifdef USE_STATIC_COROUTINE
void ConcurrentMasstreeIndex::adv_coro_MultiGet(
    transaction *t, std::vector<varstr *> &keys, std::vector<varstr *> &values,
    std::vector<ermia::dia::task<bool>> &index_probe_tasks,
    std::vector<ermia::dia::task<ermia::dbtuple*>> &value_fetch_tasks,
    std::vector<ermia::dia::coro_task_private::coro_stack> &coro_stacks) {
  ermia::epoch_num e = t ? t->xc->begin_epoch : MM::epoch_enter();
  ConcurrentMasstree::versioned_node_t sinfo;
  thread_local std::vector<OID> oids;
  oids.clear();

  for (int i = 0; i < keys.size(); ++i) {
    oids.emplace_back(INVALID_OID);
    index_probe_tasks[i] = masstree_.search(*keys[i], oids[i], e, &sinfo);
    index_probe_tasks[i].set_call_stack(&coro_stacks[i]);
  }

  int finished = 0;
  while (finished < keys.size()) {
    for (auto &t : index_probe_tasks) {
      if (t.valid()) {
        if (t.done()) {
          ++finished;
          t = ermia::dia::task<bool>(nullptr);
        } else {
          t.resume();
        }
      }
    }
  }

  if (!t) {
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    if (config::is_backup_srv()) {
      // TODO
      assert(false && "Backup not supported in coroutine execution");
    } else {
      int finished = 0;

      for (uint32_t i = 0; i < keys.size(); ++i) {
        if (oids[i] != INVALID_OID) {
          value_fetch_tasks[i] = oidmgr->oid_get_version(table_descriptor->GetTupleArray(), oids[i], t->xc);
          value_fetch_tasks[i].set_call_stack(&coro_stacks[i]);
        } else {
          ++finished;
        }
      }

      while (finished < keys.size()) {
        for (auto &t : value_fetch_tasks) {
          if (t.valid()) {
            if (t.done()) {
              ++finished;
            } else {
              t.resume();
            }
          }
        }
      }

      for (uint32_t i = 0; i < keys.size(); ++i) {
        if (oids[i] != INVALID_OID) {
          auto *tuple = value_fetch_tasks[i].get_return_value();
          if (tuple) {
            t->DoTupleRead(tuple, values[i]);
          } else if (config::phantom_prot) {
            DoNodeRead(t, sinfo.first, sinfo.second);
          }

          value_fetch_tasks[i].destroy();
        }
      }
    }
  }
}
#endif  // USE_STATIC_COROUTINE

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

PROMISE(bool) ConcurrentMasstreeIndex::InsertIfAbsent(transaction *t, const varstr &key,
                                             OID oid) {
  typename ConcurrentMasstree::insert_info_t ins_info;
  bool inserted = AWAIT masstree_.insert_if_absent(key, oid, t->xc, &ins_info);

  if (!inserted) {
    RETURN false;
  }

  if (config::phantom_prot && !t->masstree_absent_set.empty()) {
    // Update node version number
    ASSERT(ins_info.node);
    auto it = t->masstree_absent_set.find(ins_info.node);
    if (it != t->masstree_absent_set.end()) {
      if (unlikely(it->second != ins_info.old_version)) {
        // Important: caller should unlink the version, otherwise we risk
        // leaving a dead version at chain head -> infinite loop or segfault...
        RETURN false;
      }
      // otherwise, bump the version
      it->second = ins_info.new_version;
    }
  }
  RETURN true;
}

PROMISE(void) ConcurrentMasstreeIndex::ScanOID(transaction *t, const varstr &start_key,
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
    scancount = AWAIT masstree_.search_range_oid(
        start_key, end_key ? &uppervk : nullptr, cb, t->xc);
  }
  volatile_write(rc._val, scancount ? RC_TRUE : RC_FALSE);
}

PROMISE(void) ConcurrentMasstreeIndex::ReverseScanOID(transaction *t,
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
    scancount = AWAIT masstree_.rsearch_range_oid(
        start_key, end_key ? &lowervk : nullptr, cb, t->xc);
  }
  volatile_write(rc._val, scancount ? RC_TRUE : RC_FALSE);
}

////////////////// Index interfaces /////////////////

PROMISE(bool) ConcurrentMasstreeIndex::InsertOID(transaction *t, const varstr &key, OID oid) {
  bool inserted = AWAIT InsertIfAbsent(t, key, oid);
  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
    if (config::enable_chkpt) {
      auto *key_array = GetTableDescriptor()->GetKeyArray();
      volatile_write(key_array->get(oid)->_ptr, 0);
    }
  }
  RETURN inserted;
}

PROMISE(rc_t) ConcurrentMasstreeIndex::InsertRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  t->ensure_active();

  // Insert to the table first
  dbtuple *tuple = nullptr;
  OID oid = t->Insert(table_descriptor, &value, &tuple);

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  if (!AWAIT InsertOID(t, key, oid)) {
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(oid)->_ptr, 0);
    }
    RETURN rc_t{RC_ABORT_INTERNAL};
  }

  // Succeeded, now put the key there if we need it
  if (config::enable_chkpt) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key =
        (varstr *)MM::allocate(sizeof(varstr) + key.size());
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(&key);
    auto *key_array = table_descriptor->GetKeyArray();
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  if (out_oid) {
    *out_oid = oid;
  }

  RETURN rc_t{RC_TRUE};
}

PROMISE(rc_t) ConcurrentMasstreeIndex::UpdateRecord(transaction *t, const varstr &key, varstr &value) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
    rc_t rc = t->Update(table_descriptor, oid, &key, &value);
#if defined(WAITDIE)
    if (rc._val == RC_WAIT) {
      while (!t->xc->lock_ready) {}
      rc = t->Update(table_descriptor, oid, &key, &value);
    }
#endif
    RETURN rc;
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }
}

PROMISE(rc_t) ConcurrentMasstreeIndex::RemoveRecord(transaction *t, const varstr &key) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
		rc_t rc = t->Update(table_descriptor, oid, &key, nullptr);
#if defined(WAITDIE)
		if (rc._val == RC_WAIT) {
			while (!t->xc->lock_ready) {}
			rc = t->Update(table_descriptor, oid, &key, nullptr);
		}
#endif
		RETURN rc ;
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
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

////////////////// End of index interfaces //////////

////////////////// Table interfaces /////////////////
rc_t Table::Insert(transaction &t, varstr *value, OID *out_oid) {
  t.ensure_active();
  OID oid = t.Insert(td, value);
  if (out_oid) {
    *out_oid = oid;
  }
  return oid == INVALID_OID ? rc_t{RC_FALSE} : rc_t{RC_FALSE};
}

rc_t Table::Read(transaction &t, OID oid, varstr *out_value) {
  auto *tuple = sync_wait_coro(oidmgr->oid_get_version(td->GetTupleArray(), oid, t.GetXIDContext()));
  rc_t rc = {RC_INVALID};
  if (tuple) {
    // Record exists
    volatile_write(rc._val, t.DoTupleRead(tuple, out_value)._val);
  } else {
    volatile_write(rc._val, RC_FALSE);
  }
  ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
  return rc;
}

rc_t Table::Update(transaction &t, OID oid, varstr &value) {
  return t.Update(td, oid, &value);
}

rc_t Table::Remove(transaction &t, OID oid) {
  return t.Update(td, oid, nullptr);
}

////////////////// End of Table interfaces //////////

OrderedIndex::OrderedIndex(std::string table_name, bool is_primary) : is_primary(is_primary) {
  table_descriptor = TableDescriptor::Get(table_name);
  self_fid = oidmgr->create_file(true);
}

} // namespace ermia
