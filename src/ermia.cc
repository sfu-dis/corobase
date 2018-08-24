#include "dbcore/rcu.h"
#include "dbcore/sm-chkpt.h"
#include "dbcore/sm-cmd-log.h"
#include "dbcore/sm-rep.h"

#include "ermia.h"
#include "masstree_btree.h"
#include "txn.h"

namespace ermia {

write_set_t tls_write_set[config::MAX_THREADS];

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

void Engine::CreateTable(uint16_t index_type, const char *name, const char *primary_name)
{
  IndexDescriptor *index_desc = nullptr;

  switch(index_type) {
    case kIndexConcurrentMasstree:
      index_desc = (new ConcurrentMasstreeIndex(name, primary_name))->GetDescriptor();
      break;
    default:
      LOG(FATAL) << "Wrong index type: " << index_type;
      break;
  }

  if (!sm_log::need_recovery && !config::is_backup_srv()) {
    ASSERT(ermia::logmgr);
    auto create_file = [=](char*) {
      ermia::RCU::rcu_enter();
      DEFER(ermia::RCU::rcu_exit());
      ermia::sm_tx_log *log = ermia::logmgr->new_tx_log();

      index_desc->Initialize();
      log->log_index(index_desc->GetTupleFid(), index_desc->GetKeyFid(), index_desc->GetName());

      log->commit(nullptr);
    };

    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread.
    ermia::thread::sm_thread *thread = ermia::thread::get_thread();
    ALWAYS_ASSERT(thread);
    thread->start_task(create_file);
    thread->join();
    ermia::thread::put_thread(thread);
  }
}

rc_t ConcurrentMasstreeIndex::Scan(transaction *t, const varstr &start_key,
                                   const varstr *end_key, ScanCallback &callback,
                                   str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (end_key) {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key) << ", "
                      << util::hexify(*end_key) << ")" << std::endl);
  } else {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", +inf)" << std::endl);
  }

  if (!unlikely(end_key && *end_key <= start_key)) {
    txn_search_range_callback cb(t, &c);

    varstr uppervk;
    if (end_key) {
      uppervk = *end_key;
    }
    masstree_.search_range_call(start_key, end_key ? &uppervk : nullptr, cb, t->xc);
  }
  return c.return_code;
}

rc_t ConcurrentMasstreeIndex::ReverseScan(transaction *t, const varstr &start_key,
                                          const varstr *end_key, ScanCallback &callback,
                                          str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (!unlikely(end_key && start_key <= *end_key)) {
    txn_search_range_callback cb(t, &c);

    varstr lowervk;
    if (end_key) {
      lowervk = *end_key;
    }
    masstree_.rsearch_range_call(start_key, end_key ? &lowervk : nullptr, cb, t->xc);
  }
  return c.return_code;
}

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::Clear() {
  purge_tree_walker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

rc_t ConcurrentMasstreeIndex::Get(transaction *t, const varstr &key, varstr &value, OID *oid) {
  t->ensure_active();

  // search the underlying btree to map key=>(btree_node|tuple)
  dbtuple *tuple{};
  OID out_oid;
  ConcurrentMasstree::versioned_node_t sinfo;
  bool found = masstree_.search(key, out_oid, tuple, t->xc, &sinfo);
  if (oid) {
    *oid = out_oid;
  }
  if (found) {
    return t->do_tuple_read(tuple, &value);
  } else if (config::phantom_prot) {
    rc_t rc = t->do_node_read(sinfo.first, sinfo.second);
    if (rc_is_abort(rc)) {
      return rc;
    }
  }
  return rc_t{RC_FALSE};
}

void ConcurrentMasstreeIndex::purge_tree_walker::on_node_begin(
    const typename ConcurrentMasstree::node_opaque_t *n) {
  ASSERT(spec_values.empty());
  spec_values = ConcurrentMasstree::ExtractValues(n);
}

void ConcurrentMasstreeIndex::purge_tree_walker::on_node_success() {
  spec_values.clear();
}

void ConcurrentMasstreeIndex::purge_tree_walker::on_node_failure() {
  spec_values.clear();
}

rc_t ConcurrentMasstreeIndex::do_tree_put(transaction &t, const varstr *k, varstr *v,
                                          bool expect_new, bool upsert,
                                          OID *inserted_oid) {
  ASSERT(k);
  ASSERT(!expect_new || v);  // makes little sense to remove() a key you expect
  // to not be present, so we assert this doesn't happen
  // for now [since this would indicate a suboptimality]
  t.ensure_active();
  if (expect_new) {
    if (t.try_insert_new_tuple(&masstree_, k, v, inserted_oid)) {
      return rc_t{RC_TRUE};
    } else if (!upsert) {
      return rc_t{RC_ABORT_INTERNAL};
    }
  }

  // do regular search
  dbtuple *bv = nullptr;
  OID oid = 0;
  if (masstree_.search(*k, oid, bv, t.xc)) {
    return t.Update(descriptor_, oid, k, v);
  } else {
    return rc_t{RC_ABORT_INTERNAL};
  }
}

void ConcurrentMasstreeIndex::txn_search_range_callback::on_resp_node(
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
                    << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << ConcurrentMasstree::NodeStringify(n) << std::endl);
  if (config::phantom_prot) {
#ifdef SSN
    if (t->flags & transaction::TXN_FLAG_READ_ONLY) {
      return;
    }
#endif
    rc_t rc = t->do_node_read(n, version);
    if (rc_is_abort(rc)) {
      caller_callback->return_code = rc;
    }
  }
}

bool ConcurrentMasstreeIndex::txn_search_range_callback::invoke(
    const ConcurrentMasstree *btr_ptr,
    const typename ConcurrentMasstree::string_type &k, dbtuple *v,
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x"
                    << util::hexify(n) << ", version=" << version << ">"
                    << std::endl
                    << "  " << *((dbtuple *)v) << std::endl);
  varstr vv;
  caller_callback->return_code = t->do_tuple_read(v, &vv);
  if (caller_callback->return_code._val == RC_TRUE)
    return caller_callback->Invoke(k, vv);
  else if (rc_is_abort(caller_callback->return_code))
    return false;  // don't continue the read if the tx should abort
                   // ^^^^^ note: see masstree_scan.hh, whose scan() calls
                   // visit_value(), which calls this function to determine
                   // if it should stop reading.
  return true;
}

}  // namespace ermia
