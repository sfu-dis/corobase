#include "dbcore/rcu.h"
#include "dbcore/sm-chkpt.h"
#include "dbcore/sm-cmd-log.h"
#include "dbcore/sm-rep.h"
#include "ermia.h"

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

void Engine::CreateTable(const char *name, const char *primary_name)
{
  IndexDescriptor *index_desc = IndexDescriptor::New(name, primary_name);

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

class SearchRangeCallback : public base_txn_btree::search_range_callback {
 public:
  SearchRangeCallback(OrderedIndex::scan_callback &upcall)
      : base_txn_btree::search_range_callback(), upcall(&upcall) {}

  virtual bool invoke(const base_txn_btree::keystring_type &k,
                      const varstr &v) {
    return upcall->invoke(k.data(), k.length(), v);
  }

 private:
  OrderedIndex::scan_callback *upcall;
};

rc_t OrderedIndex::scan(transaction *t, const varstr &start_key,
                        const varstr *end_key, scan_callback &callback,
                        str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  tree_.do_search_range_call(*t, start_key, end_key, c);
  return c.return_code;
}

rc_t OrderedIndex::rscan(transaction *t, const varstr &start_key,
                         const varstr *end_key, scan_callback &callback,
                         str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);
  tree_.do_rsearch_range_call(*t, start_key, end_key, c);
  return c.return_code;
}
}  // namespace ermia
