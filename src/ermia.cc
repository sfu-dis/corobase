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

void Engine::CreateTable(const char *name, const char *primary_name)
{
  ConcurrentMasstreeIndex *masstree = new ConcurrentMasstreeIndex(name, primary_name);
  IndexDescriptor *index_desc = masstree->GetDescriptor();

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

rc_t ConcurrentMasstreeIndex::scan(transaction *t, const varstr &start_key,
                                   const varstr *end_key, scan_callback &callback,
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

rc_t ConcurrentMasstreeIndex::rscan(transaction *t, const varstr &start_key,
                                    const varstr *end_key, scan_callback &callback,
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

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::clear() {
  purge_tree_walker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

rc_t ConcurrentMasstreeIndex::get(transaction *t, const varstr &key, varstr &value, OID *oid) {
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
  dbtuple *bv = 0;
  OID oid = 0;
  if (!masstree_.search(*k, oid, bv, t.xc))
    return rc_t{RC_ABORT_INTERNAL};

  oid_array *tuple_array = descriptor_->GetTupleArray();
  FID tuple_fid = descriptor_->GetTupleFid();

  // first *updater* wins
  fat_ptr new_obj_ptr = NULL_PTR;
  fat_ptr prev_obj_ptr =
      oidmgr->PrimaryTupleUpdate(tuple_array, oid, v, t.xc, &new_obj_ptr);
  Object *prev_obj = (Object *)prev_obj_ptr.offset();

  if (prev_obj) {  // succeeded
    dbtuple *tuple = ((Object *)new_obj_ptr.offset())->GetPinnedTuple();
    ASSERT(tuple);
    dbtuple *prev = prev_obj->GetPinnedTuple();
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    ASSERT(t.xc);
#ifdef SSI
    ASSERT(prev->sstamp == NULL_PTR);
    if (t.xc->ct3) {
      // Check if we are the T2 with a committed T3 earlier than a safesnap
      // (being T1)
      if (t.xc->ct3 <= t.xc->last_safesnap) return {RC_ABORT_SERIAL};

      if (volatile_read(prev->xstamp) >= t.xc->ct3 or
          not prev->readers_bitmap.is_empty(true)) {
        // Read-only optimization: safe if T1 is read-only (so far) and T1's
        // begin ts
        // is before ct3.
        if (config::enable_ssi_read_only_opt) {
          TXN::readers_bitmap_iterator readers_iter(&prev->readers_bitmap);
          while (true) {
            int32_t xid_idx = readers_iter.next(true);
            if (xid_idx == -1) break;

            XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
            ASSERT(rxid != t.xc->owner);
            if (rxid == INVALID_XID)  // reader is gone, check xstamp in the end
              continue;

            XID reader_owner = INVALID_XID;
            uint64_t reader_begin = 0;
            TXN::xid_context *reader_xc = NULL;
            reader_xc = TXN::xid_get_context(rxid);
            if (not reader_xc)  // context change, consult xstamp later
              continue;

            // copy everything before doing anything
            reader_begin = volatile_read(reader_xc->begin);
            reader_owner = volatile_read(reader_xc->owner);
            if (reader_owner != rxid)  // consult xstamp later
              continue;

            // we're safe if the reader is read-only (so far) and started after
            // ct3
            if (reader_xc->xct->write_set.size() > 0 and
                reader_begin <= t.xc->ct3) {
              oidmgr->PrimaryTupleUnlink(tuple_array, oid);
              return {RC_ABORT_SERIAL};
            }
          }
        } else {
          oidmgr->PrimaryTupleUnlink(tuple_array, oid);
          return {RC_ABORT_SERIAL};
        }
      }
    }
#endif
#ifdef SSN
    // update hi watermark
    // Overwriting a version could trigger outbound anti-dep,
    // i.e., I'll depend on some tx who has read the version that's
    // being overwritten by me. So I'll need to see the version's
    // access stamp to tell if the read happened.
    ASSERT(prev->sstamp == NULL_PTR);
    auto prev_xstamp = volatile_read(prev->xstamp);
    if (t.xc->pstamp < prev_xstamp) t.xc->pstamp = prev_xstamp;

#ifdef EARLY_SSN_CHECK
    if (not ssn_check_exclusion(t.xc)) {
      // unlink the version here (note abort_impl won't be able to catch
      // it because it's not yet in the write set)
      oidmgr->PrimaryTupleUnlink(tuple_array, oid);
      return rc_t{RC_ABORT_SERIAL};
    }
#endif

    // copy access stamp to new tuple from overwritten version
    // (no need to copy sucessor lsn (slsn))
    volatile_write(tuple->xstamp, prev->xstamp);
#endif

    // read prev's clsn first, in case it's a committing XID, the clsn's state
    // might change to ASI_LOG anytime
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    fat_ptr prev_clsn = prev->GetObject()->GetClsn();
    fat_ptr prev_persistent_ptr = NULL_PTR;
    if (prev_clsn.asi_type() == fat_ptr::ASI_XID and
        XID::from_ptr(prev_clsn) == t.xid) {
      // updating my own updates!
      // prev's prev: previous *committed* version
      ASSERT(((Object *)prev_obj_ptr.offset())->GetAllocateEpoch() ==
             t.xc->begin_epoch);
      prev_persistent_ptr = prev_obj->GetNextPersistent();
      MM::deallocate(prev_obj_ptr);
    } else {  // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      volatile_write(prev->sstamp, t.xc->owner.to_ptr());
      ASSERT(prev->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(prev->sstamp) == t.xc->owner);
      ASSERT(tuple->NextVolatile() == prev);
#endif
      t.add_to_write_set(tuple_array->get(oid));
      prev_persistent_ptr = prev_obj->GetPersistentAddress();
    }

    ASSERT(not tuple->pvalue or tuple->pvalue->size() == tuple->size);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_XID);
    ASSERT(oidmgr->oid_get_version(tuple_fid, oid, t.xc) == tuple);
    ASSERT(t.log);

    // FIXME(tzwang): mark deleted in all 2nd indexes as well?

    // The varstr also encodes the pdest of the overwritten version.
    // FIXME(tzwang): the pdest of the overwritten version doesn't belong to
    // varstr. Embedding it in varstr makes it part of the payload and is
    // helpful for digging out versions on backups. Not used by the primary.
    bool is_delete = !v;
    if (!v) {
      // Get an empty varstr just to store the overwritten tuple's
      // persistent address
      v = t.string_allocator().next(0);
      v->p = nullptr;
      v->l = 0;
    }
    ASSERT(v);
    v->ptr = prev_persistent_ptr;
    ASSERT(v->ptr.offset() && v->ptr.asi_type() == fat_ptr::ASI_LOG);

    // log the whole varstr so that recovery can figure out the real size
    // of the tuple, instead of using the decoded (larger-than-real) size.
    size_t data_size = v->size() + sizeof(varstr);
    auto size_code = encode_size_aligned(data_size);
    if (is_delete) {
      t.log->log_enhanced_delete(tuple_fid, oid,
                                 fat_ptr::make((void *)v, size_code),
                                 DEFAULT_ALIGNMENT_BITS);
    } else {
      t.log->log_update(tuple_fid, oid, fat_ptr::make((void *)v, size_code),
                        DEFAULT_ALIGNMENT_BITS,
                        tuple->GetObject()->GetPersistentAddressPtr());

      if (config::log_key_for_update) {
        auto key_size = align_up(k->size() + sizeof(varstr));
        auto key_size_code = encode_size_aligned(key_size);
        t.log->log_update_key(tuple_fid, oid,
                              fat_ptr::make((void *)k, key_size_code),
                              DEFAULT_ALIGNMENT_BITS);
      }
    }
    return rc_t{RC_TRUE};
  } else {  // somebody else acted faster than we did
    return rc_t{RC_ABORT_SI_CONFLICT};
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
    return caller_callback->invoke(k, vv);
  else if (rc_is_abort(caller_callback->return_code))
    return false;  // don't continue the read if the tx should abort
                   // ^^^^^ note: see masstree_scan.hh, whose scan() calls
                   // visit_value(), which calls this function to determine
                   // if it should stop reading.
  return true;
}

}  // namespace ermia
