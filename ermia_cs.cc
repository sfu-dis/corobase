#include "dbcore/rcu.h"
#include "dbcore/sm-chkpt.h"
#include "dbcore/sm-cmd-log.h"
#include "dbcore/sm-rep.h"

#include "ermia.h"
#include "txn.h"

#include "masstree/masstree_scan.hh"

namespace ermia {

void ConcurrentMasstreeIndex::amac_MultiGet(
    transaction *t, std::vector<ConcurrentMasstree::AMACState> &requests,
    std::vector<varstr *> &values) {
#ifndef ADV_COROUTINE
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

void ConcurrentMasstreeIndex::simple_coro_MultiGet(
    transaction *t, std::vector<varstr *> &keys, std::vector<varstr *> &values,
    std::vector<std::experimental::coroutine_handle<>> &handles) {
  ermia::epoch_num e;
  if (!t) {
    e = MM::epoch_enter();
    ConcurrentMasstree::threadinfo ti(e);
    ConcurrentMasstree::versioned_node_t sinfo;

    OID oid = INVALID_OID;
    for (int i = 0; i < keys.size(); ++i) {
      handles[i] = masstree_.search_coro(*keys[i], oid, ti, &sinfo).get_handle();
    }
  } else {
    for (int i = 0; i < keys.size(); ++i) {
      handles[i] = coro_GetRecord(t, *keys[i], *values[i]).get_handle();
    }
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

  if (!t)
    MM::epoch_exit(0, e);
}

#ifdef ADV_COROUTINE
void ConcurrentMasstreeIndex::adv_coro_MultiGet(
    transaction *t, std::vector<varstr *> &keys, std::vector<varstr *> &values,
    std::vector<ermia::dia::task<bool>> &index_probe_tasks,
    std::vector<ermia::dia::task<ermia::dbtuple*>> &value_fetch_tasks) {
  ermia::epoch_num e = t ? t->xc->begin_epoch : MM::epoch_enter();
  ConcurrentMasstree::versioned_node_t sinfo;
  thread_local std::vector<OID> oids;
  oids.clear();

  for (int i = 0; i < keys.size(); ++i) {
    oids.emplace_back(INVALID_OID);
    index_probe_tasks[i] = masstree_.search(*keys[i], oids[i], e, &sinfo);
    index_probe_tasks[i].start();
  }

  int finished = 0;
  while (finished < keys.size()) {
    for (auto &t : index_probe_tasks) {
      if (t.valid()) {
        if (t.done()) {
          ++finished;
          t.destroy();
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
          value_fetch_tasks[i].start();
        } else {
          ++finished;
        }
      }

      while (finished < keys.size()) {
        for (uint32_t i = 0; i < keys.size(); ++i) {
          if (value_fetch_tasks[i].valid()) {
            if (value_fetch_tasks[i].done()) {
              if (oids[i] != INVALID_OID) {
                auto *tuple = value_fetch_tasks[i].get_return_value();
                if (tuple) {
                  t->DoTupleRead(tuple, values[i]);
                } else if (config::phantom_prot) {
                  DoNodeRead(t, sinfo.first, sinfo.second);
                }
              }
              ++finished;
              value_fetch_tasks[i].destroy();
            } else {
              value_fetch_tasks[i].resume();
            }
          }
        }
      }
    }
  }
}
#endif  // ADV_COROUTINE
ermia::dia::generator<rc_t> ConcurrentMasstreeIndex::coro_GetRecordSV(transaction *t, const varstr &key,
                                                                    varstr &value, OID *out_oid) {
  OID oid = INVALID_OID;
  rc_t rc = rc_t{RC_INVALID};
  t->ensure_active();

// start: masstree search
  ConcurrentMasstree::threadinfo ti(t->xc->begin_epoch);
  ConcurrentMasstree::unlocked_tcursor_type lp(*masstree_.get_table(), key.data(), key.size());

// start: find_unlocked
  int match;
  key_indexed_position kx;
  ConcurrentMasstree::node_base_type *root = const_cast<ConcurrentMasstree::node_base_type *>(lp.root_);

retry:
// start: reach_leaf
  const ConcurrentMasstree::node_base_type* n[2];
  ConcurrentMasstree::nodeversion_type v[2];
  bool sense;

// Get a non-stale root.
// Detect staleness by checking whether n has ever split.
// The true root has never split.
retry2:
  sense = false;
  n[sense] = root;
  while (1) {
    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
    if (!v[sense].has_split()) break;
    n[sense] = n[sense]->unsplit_ancestor();
  }

  // Loop over internal nodes.
  while (!v[sense].isleaf()) {
    const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
    in->prefetch();
    co_await std::experimental::suspend_always{};
    int kp = ConcurrentMasstree::internode_type::bound_type::upper(lp.ka_, *in);
    n[!sense] = in->child_[kp];
    if (!n[!sense]) goto retry2;
    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

    if (likely(!in->has_changed(v[sense]))) {
      sense = !sense;
      continue;
    }

    ConcurrentMasstree::nodeversion_type oldv = v[sense];
    v[sense] = in->stable_annotated(ti.stable_fence());
    if (oldv.has_split(v[sense]) &&
        in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
      goto retry2;
    }
  }

  lp.v_ = v[sense];
  lp.n_ = const_cast<ConcurrentMasstree::leaf_type *>(static_cast<const ConcurrentMasstree::leaf_type *>(n[sense]));
// end: reach_leaf

forward:
  if (lp.v_.deleted()) goto retry;

  lp.n_->prefetch();
  co_await std::experimental::suspend_always{};
  lp.perm_ = lp.n_->permutation();
  kx = ConcurrentMasstree::leaf_type::bound_type::lower(lp.ka_, lp);
  if (kx.p >= 0) {
    lp.lv_ = lp.n_->lv_[kx.p];
    lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
    co_await std::experimental::suspend_always{};
    match = lp.n_->ksuf_matches(kx.p, lp.ka_);
  } else
    match = 0;
  if (lp.n_->has_changed(lp.v_)) {
    lp.n_ = lp.n_->advance_to_key(lp.ka_, lp.v_, ti);
    goto forward;
  }

  if (match < 0) {
    lp.ka_.shift_by(-match);
    root = lp.lv_.layer();
    goto retry;
  }
// end: find_unlocked

  bool found = match;
  dbtuple *tuple = nullptr;
  if (found) {
    oid = lp.value();
// end: masstree search

// start: oid_get_version
    oid_array *oa = table_descriptor->GetTupleArray();
    TXN::xid_context *visitor_xc = t->xc;
    fat_ptr *entry = oa->get(oid);
start_over:
    //::prefetch((const char*)entry);
    //co_await std::experimental::suspend_always{};

    fat_ptr ptr = volatile_read(*entry);
    ASSERT(ptr.asi_type() == 0);
    Object *prev_obj = nullptr;
    while (ptr.offset()) {
      Object *cur_obj = nullptr;
      // Must read next_ before reading cur_obj->_clsn:
      // the version we're currently reading (ie cur_obj) might be unlinked
      // and thus recycled by the memory allocator at any time if it's not
      // a committed version. If so, cur_obj->_next will be pointing to some
      // other object in the allocator's free object pool - we'll probably
      // end up at la-la land if we followed this _next pointer value...
      // Here we employ some flavor of OCC to solve this problem:
      // the aborting transaction that will unlink cur_obj will update
      // cur_obj->_clsn to NULL_PTR, then deallocate(). Before reading
      // cur_obj->_clsn, we (as the visitor), first dereference pp to get
      // a stable value that "should" contain the right address of the next
      // version. We then read cur_obj->_clsn to verify: if it's NULL_PTR
      // that means we might have read a wrong _next value that's actually
      // pointing to some irrelevant object in the allocator's memory pool,
      // hence must start over from the beginning of the version chain.
      fat_ptr tentative_next = NULL_PTR;
      // If this is a backup server, then must see persistent_next to find out
      // the **real** overwritten version.
      ASSERT(ptr.asi_type() == 0);
      cur_obj = (Object *)ptr.offset();
      //Object::PrefetchHeader(cur_obj);
      //co_await std::experimental::suspend_always{};
      tentative_next = cur_obj->GetNextVolatile();
      ASSERT(tentative_next.asi_type() == 0);

      bool retry = false;
      bool visible = oidmgr->TestVisibility(cur_obj, visitor_xc, retry);
      if (retry) {
        goto start_over;
      }
      if (visible) {
        tuple = cur_obj->GetPinnedTuple();
        volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
        if (out_oid) {
          *out_oid = oid;
        }
        co_return rc;
      }
      ptr = tentative_next;
      prev_obj = cur_obj;
    }
  }
  co_return {RC_FALSE};
  ALWAYS_ASSERT(false);
}

ermia::dia::generator<rc_t> ConcurrentMasstreeIndex::coro_GetRecord(transaction *t, const varstr &key,
                                                                    varstr &value, OID *out_oid) {
  OID oid = INVALID_OID;
  rc_t rc = rc_t{RC_INVALID};
  t->ensure_active();

// start: masstree search
  ConcurrentMasstree::threadinfo ti(t->xc->begin_epoch);
  ConcurrentMasstree::unlocked_tcursor_type lp(*masstree_.get_table(), key.data(), key.size());

// start: find_unlocked
  int match;
  key_indexed_position kx;
  ConcurrentMasstree::node_base_type *root = const_cast<ConcurrentMasstree::node_base_type *>(lp.root_);

retry:
// start: reach_leaf
  const ConcurrentMasstree::node_base_type* n[2];
  ConcurrentMasstree::nodeversion_type v[2];
  bool sense;

// Get a non-stale root.
// Detect staleness by checking whether n has ever split.
// The true root has never split.
retry2:
  sense = false;
  n[sense] = root;
  while (1) {
    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
    if (!v[sense].has_split()) break;
    n[sense] = n[sense]->unsplit_ancestor();
  }

  // Loop over internal nodes.
  while (!v[sense].isleaf()) {
    const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
    in->prefetch();
    co_await std::experimental::suspend_always{};
    int kp = ConcurrentMasstree::internode_type::bound_type::upper(lp.ka_, *in);
    n[!sense] = in->child_[kp];
    if (!n[!sense]) goto retry2;
    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

    if (likely(!in->has_changed(v[sense]))) {
      sense = !sense;
      continue;
    }

    ConcurrentMasstree::nodeversion_type oldv = v[sense];
    v[sense] = in->stable_annotated(ti.stable_fence());
    if (oldv.has_split(v[sense]) &&
        in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
      goto retry2;
    }
  }

  lp.v_ = v[sense];
  lp.n_ = const_cast<ConcurrentMasstree::leaf_type *>(static_cast<const ConcurrentMasstree::leaf_type *>(n[sense]));
// end: reach_leaf

forward:
  if (lp.v_.deleted()) goto retry;

  lp.n_->prefetch();
  co_await std::experimental::suspend_always{};
  lp.perm_ = lp.n_->permutation();
  kx = ConcurrentMasstree::leaf_type::bound_type::lower(lp.ka_, lp);
  if (kx.p >= 0) {
    lp.lv_ = lp.n_->lv_[kx.p];
    lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
    co_await std::experimental::suspend_always{};
    match = lp.n_->ksuf_matches(kx.p, lp.ka_);
  } else
    match = 0;
  if (lp.n_->has_changed(lp.v_)) {
    lp.n_ = lp.n_->advance_to_key(lp.ka_, lp.v_, ti);
    goto forward;
  }

  if (match < 0) {
    lp.ka_.shift_by(-match);
    root = lp.lv_.layer();
    goto retry;
  }
// end: find_unlocked

  bool found = match;
  dbtuple *tuple = nullptr;
  if (found) {
    oid = lp.value();
// end: masstree search

// start: oid_get_version
    oid_array *oa = table_descriptor->GetTupleArray();
    TXN::xid_context *visitor_xc = t->xc;
    fat_ptr *entry = oa->get(oid);
start_over:
    ::prefetch((const char*)entry);
    co_await std::experimental::suspend_always{};

    fat_ptr ptr = volatile_read(*entry);
    ASSERT(ptr.asi_type() == 0);
    Object *prev_obj = nullptr;
    while (ptr.offset()) {
      Object *cur_obj = nullptr;
      // Must read next_ before reading cur_obj->_clsn:
      // the version we're currently reading (ie cur_obj) might be unlinked
      // and thus recycled by the memory allocator at any time if it's not
      // a committed version. If so, cur_obj->_next will be pointing to some
      // other object in the allocator's free object pool - we'll probably
      // end up at la-la land if we followed this _next pointer value...
      // Here we employ some flavor of OCC to solve this problem:
      // the aborting transaction that will unlink cur_obj will update
      // cur_obj->_clsn to NULL_PTR, then deallocate(). Before reading
      // cur_obj->_clsn, we (as the visitor), first dereference pp to get
      // a stable value that "should" contain the right address of the next
      // version. We then read cur_obj->_clsn to verify: if it's NULL_PTR
      // that means we might have read a wrong _next value that's actually
      // pointing to some irrelevant object in the allocator's memory pool,
      // hence must start over from the beginning of the version chain.
      fat_ptr tentative_next = NULL_PTR;
      // If this is a backup server, then must see persistent_next to find out
      // the **real** overwritten version.
      ASSERT(ptr.asi_type() == 0);
      cur_obj = (Object *)ptr.offset();
      Object::PrefetchHeader(cur_obj);
      co_await std::experimental::suspend_always{};
      tentative_next = cur_obj->GetNextVolatile();
      ASSERT(tentative_next.asi_type() == 0);

      bool retry = false;
      bool visible = oidmgr->TestVisibility(cur_obj, visitor_xc, retry);
      if (retry) {
        goto start_over;
      }
      if (visible) {
        tuple = cur_obj->GetPinnedTuple();
        volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
        if (out_oid) {
          *out_oid = oid;
        }
        co_return rc;
      }
      ptr = tentative_next;
      prev_obj = cur_obj;
    }
  }
  co_return {RC_FALSE};
  ALWAYS_ASSERT(false);
}

ermia::dia::generator<rc_t> ConcurrentMasstreeIndex::coro_UpdateRecord(transaction *t, const varstr &key,
                                                                       varstr &value) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = INVALID_OID;
  rc_t rc = rc_t{RC_INVALID};
  ConcurrentMasstree::versioned_node_t sinfo;
  t->ensure_active();

// start: masstree search
  ConcurrentMasstree::threadinfo ti(t->xc->begin_epoch);
  ConcurrentMasstree::unlocked_tcursor_type lp(*masstree_.get_table(), key.data(), key.size());

// start: find_unlocked
  int match;
  key_indexed_position kx;
  ConcurrentMasstree::node_base_type* root = const_cast<ConcurrentMasstree::node_base_type*>(lp.root_);

retry:
// start: reach_leaf
  const ConcurrentMasstree::node_base_type* n[2];
  ConcurrentMasstree::nodeversion_type v[2];
  bool sense;

retry2:
  sense = false;
  n[sense] = root;
  while (1) {
    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
    if (!v[sense].has_split()) break;
    n[sense] = n[sense]->unsplit_ancestor();
  }

  // Loop over internal nodes.
  while (!v[sense].isleaf()) {
    const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
    in->prefetch();
    co_await std::experimental::suspend_always{};
    int kp = ConcurrentMasstree::internode_type::bound_type::upper(lp.ka_, *in);
    n[!sense] = in->child_[kp];
    if (!n[!sense]) goto retry2;
    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

    if (likely(!in->has_changed(v[sense]))) {
      sense = !sense;
      continue;
    }

    ConcurrentMasstree::nodeversion_type oldv = v[sense];
    v[sense] = in->stable_annotated(ti.stable_fence());
    if (oldv.has_split(v[sense]) &&
        in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
      goto retry2;
    }
  }

  lp.v_ = v[sense];
  lp.n_ = const_cast<ConcurrentMasstree::leaf_type*>(static_cast<const ConcurrentMasstree::leaf_type*>(n[sense]));
// end: reach_leaf

forward:
  if (lp.v_.deleted()) goto retry;

  lp.n_->prefetch();
  co_await std::experimental::suspend_always{};
  lp.perm_ = lp.n_->permutation();
  kx = ConcurrentMasstree::leaf_type::bound_type::lower(lp.ka_, lp);
  if (kx.p >= 0) {
    lp.lv_ = lp.n_->lv_[kx.p];
    lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
    co_await std::experimental::suspend_always{};
    match = lp.n_->ksuf_matches(kx.p, lp.ka_);
  } else
    match = 0;
  if (lp.n_->has_changed(lp.v_)) {
    lp.n_ = lp.n_->advance_to_key(lp.ka_, lp.v_, ti);
    goto forward;
  }

  if (match < 0) {
    lp.ka_.shift_by(-match);
    root = lp.lv_.layer();
    goto retry;
  }
// end: find_unlocked

  if (match) {
    oid = lp.value();
  }
  sinfo = ConcurrentMasstree::versioned_node_t(lp.node(), lp.full_version_value());
// end: masstree search

  if (match) {
    // By default we don't do coroutine prefetch-yield here for updates, assuming
    // it's part of an RMW (most cases) which means the data is probably in cache
    // anyway. This may not be true however for blind updates.
#ifndef CORO_UPDATE_VERSION_CHAIN
    rc = t->Update(table_descriptor, oid, &key, &value);
#else
    oid_array *tuple_array = table_descriptor->GetTupleArray();
    FID tuple_fid = table_descriptor->GetTupleFid();
    fat_ptr new_obj_ptr = NULL_PTR;
    fat_ptr prev_obj_ptr = NULL_PTR;
    Object *new_object = nullptr;

  start_over:
    auto *ptr = tuple_array->get(oid);
    ::prefetch((const char*)ptr);
    co_await std::experimental::suspend_always{};

    fat_ptr head = volatile_read(*ptr);
    ASSERT(head.asi_type() == 0);
    Object *old_desc = (Object *)head.offset();
    ASSERT(old_desc);
    ASSERT(head.size_code() != INVALID_SIZE_CODE);

    Object::PrefetchHeader(old_desc);
    co_await std::experimental::suspend_always{};
    dbtuple *version = (dbtuple *)old_desc->GetPayload();
    bool overwrite = false;

    auto clsn = old_desc->GetClsn();
    if (clsn == NULL_PTR) {
      // stepping on an unlinked version?
      goto start_over;
    } else if (clsn.asi_type() == fat_ptr::ASI_XID) {
      /* Grab the context for this XID. If we're too slow,
         the context might be recycled for a different XID,
         perhaps even *while* we are reading the
         context. Copy everything we care about and then
         (last) check the context's XID for a mismatch that
         would indicate an inconsistent read. If this
         occurs, just start over---the version we cared
         about is guaranteed to have a LSN now.
       */
      auto holder_xid = XID::from_ptr(clsn);
      XID updater_xid = volatile_read(t->xid);

      // in-place update case (multiple updates on the same record  by same
      // transaction)
      if (holder_xid == updater_xid) {
        overwrite = true;
        goto install;
      }

      TXN::xid_context *holder = TXN::xid_get_context(holder_xid);
      if (not holder) {
#ifndef NDEBUG
        auto t = old_desc->GetClsn().asi_type();
        ASSERT(t == fat_ptr::ASI_LOG or oid_get(oa, o) != head);
#endif
        goto start_over;
      }
      ASSERT(holder);
      auto state = volatile_read(holder->state);
      auto owner = volatile_read(holder->owner);
      holder = NULL;  // use cached values instead!

      // context still valid for this XID?
      if (unlikely(owner != holder_xid)) {
        goto start_over;
      }
      ASSERT(holder_xid != updater_xid);
      if (state == TXN::TXN_CMMTD) {
        // Allow installing a new version if the tx committed (might
        // still hasn't finished post-commit). Note that the caller
        // (ie do_tree_put) should look at the clsn field of the
        // returned version (prev) to see if this is an overwrite
        // (ie xids match) or not (xids don't match).
        ASSERT(holder_xid != updater_xid);
        goto install;
      }
      prev_obj_ptr = NULL_PTR;
      goto check_prev;
    }
    // check dirty writes
    else {
      ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
#ifndef RC
      // First updater wins: if some concurrent tx committed first,
      // I have to abort. Same as in Oracle. Otherwise it's an isolation
      // failure: I can modify concurrent transaction's writes.
      if (LSN::from_ptr(clsn).offset() >= t->xc->begin) {
        prev_obj_ptr = NULL_PTR;
        goto check_prev;
      }
#endif
      goto install;
    }

  install:
    // remove uncommitted overwritten version
    // (tx's repetitive updates, keep the latest one only)
    // Note for this to be correct we shouldn't allow multiple txs
    // working on the same tuple at the same time.

    new_obj_ptr = Object::Create(&value, false, t->xc->begin_epoch);
    ASSERT(new_obj_ptr.asi_type() == 0);
    new_object = (Object *)new_obj_ptr.offset();
    new_object->SetClsn(t->xc->owner.to_ptr());
    if (overwrite) {
      new_object->SetNextPersistent(old_desc->GetNextPersistent());
      new_object->SetNextVolatile(old_desc->GetNextVolatile());
      // I already claimed it, no need to use cas then
      volatile_write(ptr->_ptr, new_obj_ptr._ptr);
      __sync_synchronize();
      prev_obj_ptr = head;
      goto check_prev;
    } else {
      fat_ptr pa = old_desc->GetPersistentAddress();
      while (pa == NULL_PTR) {
        pa = old_desc->GetPersistentAddress();
      }
      new_object->SetNextPersistent(pa);
      new_object->SetNextVolatile(head);
      if (__sync_bool_compare_and_swap(&ptr->_ptr, head._ptr,
                                       new_obj_ptr._ptr)) {
        // Succeeded installing a new version, now only I can modify the
        // chain, try recycle some objects
        if (config::enable_gc) {
          MM::gc_version_chain(ptr);
        }
        prev_obj_ptr = head;
        goto check_prev;
      } else {
        MM::deallocate(new_obj_ptr);
      }
    }
    prev_obj_ptr = NULL_PTR;

  check_prev:
    Object *prev_obj = (Object *)prev_obj_ptr.offset();
    if (prev_obj) {  // succeeded
      Object::PrefetchHeader(prev_obj);
      co_await std::experimental::suspend_always{};
      dbtuple *tuple = ((Object *)new_obj_ptr.offset())->GetPinnedTuple();
      ASSERT(tuple);
      dbtuple *prev = prev_obj->GetPinnedTuple();
      ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
      ASSERT(xc);

#ifdef SSI
      // TODO
#endif
#ifdef SSN
      // TODO
#endif

      // read prev's clsn first, in case it's a committing XID, the clsn's state
      // might change to ASI_LOG anytime
      ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
      fat_ptr prev_clsn = prev->GetObject()->GetClsn();
      fat_ptr prev_persistent_ptr = NULL_PTR;
      if (prev_clsn.asi_type() == fat_ptr::ASI_XID and
          XID::from_ptr(prev_clsn) == t->xid) {
        // updating my own updates!
        // prev's prev: previous *committed* version
        ASSERT(((Object *)prev_obj_ptr.offset())->GetAllocateEpoch() ==
               xc->begin_epoch);
        prev_persistent_ptr = prev_obj->GetNextPersistent();
        // FIXME(tzwang): 20190210: seems the deallocation here is too early,
        // causing readers to not find any visible version. Fix this together with
        // GC later.
        //MM::deallocate(prev_obj_ptr);
      } else {  // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN) || defined(MVOCC)
        // TODO
#endif
        t->add_to_write_set(tuple_array->get(oid));
        prev_persistent_ptr = prev_obj->GetPersistentAddress();
      }

      ASSERT(not tuple->pvalue or tuple->pvalue->size() == tuple->size);
      ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_XID);
      ASSERT(sync_wait_coro(oidmgr->oid_get_version(tuple_fid, oid, xc)) == tuple);
      ASSERT(log);

      // FIXME(tzwang): mark deleted in all 2nd indexes as well?

      // The varstr also encodes the pdest of the overwritten version.
      // FIXME(tzwang): the pdest of the overwritten version doesn't belong to
      // varstr. Embedding it in varstr makes it part of the payload and is
      // helpful for digging out versions on backups. Not used by the primary.
      value.ptr = prev_persistent_ptr;
      ASSERT(is_delete || (value.ptr.offset() && value.ptr.asi_type() == fat_ptr::ASI_LOG));

      // log the whole varstr so that recovery can figure out the real size
      // of the tuple, instead of using the decoded (larger-than-real) size.
      size_t data_size = value.size() + sizeof(varstr);
      auto size_code = encode_size_aligned(data_size);
      t->log->log_update(tuple_fid, oid, fat_ptr::make((void *)&value, size_code),
                      DEFAULT_ALIGNMENT_BITS,
                      tuple->GetObject()->GetPersistentAddressPtr());

      if (config::log_key_for_update) {
        auto key_size = align_up(key.size() + sizeof(varstr));
        auto key_size_code = encode_size_aligned(key_size);
        t->log->log_update_key(tuple_fid, oid,
                              fat_ptr::make((void *)&key, key_size_code),
                              DEFAULT_ALIGNMENT_BITS);
      }
      rc = rc_t{RC_TRUE};
    } else {  // somebody else acted faster than we did
      rc = rc_t{RC_ABORT_SI_CONFLICT};
    }
#endif // CORO_UPDATE_VERSION_CHAIN
  } else {
    rc = rc_t{RC_ABORT_INTERNAL};
  }

  co_return rc;
}

ermia::dia::generator<bool> ConcurrentMasstreeIndex::coro_InsertOID(transaction *t, const varstr &key, OID oid) {
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  t->ensure_active();

// start: InsertIfAbsent
  ConcurrentMasstree::insert_info_t ins_info;
// strat: insert_if_absent
  ermia::ConcurrentMasstree::insert_info_t *insert_info = &ins_info;
  // Recovery will give a null xc, use epoch 0 for the memory allocated
  epoch_num e = 0;
  if (t->xc)
    e = t->xc->begin_epoch;
  ConcurrentMasstree::threadinfo ti(e);
  ConcurrentMasstree::tcursor_type lp(*masstree_.get_table(), key.data(), key.size());

// start: find_insert
// start: find_locked
  ConcurrentMasstree::node_base_type* root = const_cast<ConcurrentMasstree::node_base_type*>(lp.root_);
  ConcurrentMasstree::nodeversion_type version;
  ConcurrentMasstree::permuter_type perm;

retry:
// start: reach_leaf
  const ConcurrentMasstree::node_base_type* n[2];
  ConcurrentMasstree::nodeversion_type v[2];
  bool sense;

// Get a non-stale root.
// Detect staleness by checking whether n has ever split.
// The true root has never split.
retry2:
  sense = false;
  n[sense] = root;
  while (1) {
    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
    if (!v[sense].has_split()) break;
    n[sense] = n[sense]->unsplit_ancestor();
  }

  // Loop over internal nodes.
  while (!v[sense].isleaf()) {
    const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
    in->prefetch();
    co_await std::experimental::suspend_always{};
    int kp = ConcurrentMasstree::internode_type::bound_type::upper(lp.ka_, *in);
    n[!sense] = in->child_[kp];
    if (!n[!sense]) goto retry2;
    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

    if (likely(!in->has_changed(v[sense]))) {
      sense = !sense;
      continue;
    }

    ConcurrentMasstree::nodeversion_type oldv = v[sense];
    v[sense] = in->stable_annotated(ti.stable_fence());
    if (oldv.has_split(v[sense]) &&
        in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
      goto retry2;
    }
  }

  version = v[sense];
  lp.n_ = const_cast<ConcurrentMasstree::leaf_type*>(static_cast<const ConcurrentMasstree::leaf_type*>(n[sense]));
// end: reach_leaf

forward:
  if (version.deleted()) goto retry;

  lp.n_->prefetch();
  co_await std::experimental::suspend_always{};
  perm = lp.n_->permutation();
  fence();
  lp.kx_ = ConcurrentMasstree::leaf_type::bound_type::lower(lp.ka_, *lp.n_);
  if (lp.kx_.p >= 0) {
    ConcurrentMasstree::leafvalue_type lv = lp.n_->lv_[lp.kx_.p];
    lv.prefetch(lp.n_->keylenx_[lp.kx_.p]);
    co_await std::experimental::suspend_always{};
    lp.state_ = lp.n_->ksuf_matches(lp.kx_.p, lp.ka_);
    if (lp.state_ < 0 && !lp.n_->has_changed(version) && !lv.layer()->has_split()) {
      lp.ka_.shift_by(-lp.state_);
      root = lv.layer();
      goto retry;
    }
  } else
    lp.state_ = 0;

  lp.n_->lock(version, ti.lock_fence(tc_leaf_lock));
  if (lp.n_->has_changed(version) || lp.n_->permutation() != perm) {
    lp.n_->unlock();
    lp.n_ = lp.n_->advance_to_key(lp.ka_, version, ti);
    goto forward;
  } else if (unlikely(lp.state_ < 0)) {
    lp.ka_.shift_by(-lp.state_);
    lp.n_->lv_[lp.kx_.p] = root = lp.n_->lv_[lp.kx_.p].layer()->unsplit_ancestor();
    lp.n_->unlock();
    goto retry;
  } else if (unlikely(lp.n_->deleted_layer())) {
    lp.ka_.unshift_all();
    root = const_cast<ConcurrentMasstree::node_base_type*>(lp.root_);
    goto retry;
  }
// end: find_locked

  lp.original_n_ = lp.n_;
  lp.original_v_ = lp.n_->full_unlocked_version_value();
  bool found = true;

  // maybe we found it
  if (lp.state_) {
    found = true;
  } else {
    // otherwise mark as inserted but not present
    lp.state_ = 2;

    // maybe we need a new layer
    if (lp.kx_.p >= 0) {
      found = lp.make_new_layer(ti);
    } else {
      // mark insertion if we are changing modification state
      if (unlikely(lp.n_->modstate_ != ConcurrentMasstree::leaf_type::modstate_insert)) {
        masstree_invariant(lp.n_->modstate_ == ConcurrentMasstree::leaf_type::modstate_remove);
        lp.n_->mark_insert();
        lp.n_->modstate_ = ConcurrentMasstree::leaf_type::modstate_insert;
      }

      // try inserting into this node
      if (lp.n_->size() < lp.n_->width) {
        lp.kx_.p = ConcurrentMasstree::leaf_type::permuter_type(lp.n_->permutation_).back();
        // don't inappropriately reuse position 0, which holds the ikey_bound
        if (likely(lp.kx_.p != 0) || !lp.n_->prev_ || lp.n_->ikey_bound() == lp.ka_.ikey()) {
          lp.n_->assign(lp.kx_.p, lp.ka_, ti);
          found = false;
        }
      }

      // otherwise must split
      if (found)
        found = lp.make_split(ti);
    }
  }
// end: find_insert

  if (!found) {
insert_new:
    found = false;
    ti.advance_timestamp(lp.node_timestamp());
    lp.value() = oid;
    if (insert_info) {
      insert_info->node = lp.node();
      insert_info->old_version = lp.previous_full_version_value();
      insert_info->new_version = lp.next_full_version_value(1);
    }
  } else if (IsPrimary()) {
    // we have two cases: 1) predecessor's inserts are still remaining in tree,
    // even though version chain is empty or 2) somebody else are making dirty
    // data in this chain. If it's the first case, version chain is considered
    // empty, then we retry insert.
    OID o = lp.value();
    if (oidmgr->oid_get_latest_version(table_descriptor->GetTupleArray(), o))
      found = true;
    else
      goto insert_new;
  }
  lp.finish(!found, ti);
// end: insert_if_absent

  bool inserted = !found;
// end: InsertIfAbsent

  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
    co_return true;
  }
  co_return false;
}

ermia::dia::generator<rc_t> ConcurrentMasstreeIndex::coro_InsertRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  t->ensure_active();

  // Insert to the table first
  dbtuple *tuple = nullptr;
  OID oid = t->Insert(table_descriptor, &value, &tuple);

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));

// start: InsertOID
// start: InsertIfAbsent
  ConcurrentMasstree::insert_info_t ins_info;
// strat: insert_if_absent
  ermia::ConcurrentMasstree::insert_info_t *insert_info = &ins_info;
  // Recovery will give a null xc, use epoch 0 for the memory allocated
  epoch_num e = 0;
  if (t->xc)
    e = t->xc->begin_epoch;
  ConcurrentMasstree::threadinfo ti(e);
  ConcurrentMasstree::tcursor_type lp(*masstree_.get_table(), key.data(), key.size());

// start: find_insert
// start: find_locked
  ConcurrentMasstree::node_base_type* root = const_cast<ConcurrentMasstree::node_base_type*>(lp.root_);
  ConcurrentMasstree::nodeversion_type version;
  ConcurrentMasstree::permuter_type perm;

retry:
// start: reach_leaf
  const ConcurrentMasstree::node_base_type* n[2];
  ConcurrentMasstree::nodeversion_type v[2];
  bool sense;

// Get a non-stale root.
// Detect staleness by checking whether n has ever split.
// The true root has never split.
retry2:
  sense = false;
  n[sense] = root;
  while (1) {
    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
    if (!v[sense].has_split()) break;
    n[sense] = n[sense]->unsplit_ancestor();
  }

  // Loop over internal nodes.
  while (!v[sense].isleaf()) {
    const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
    in->prefetch();
    co_await std::experimental::suspend_always{};
    int kp = ConcurrentMasstree::internode_type::bound_type::upper(lp.ka_, *in);
    n[!sense] = in->child_[kp];
    if (!n[!sense]) goto retry2;
    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

    if (likely(!in->has_changed(v[sense]))) {
      sense = !sense;
      continue;
    }

    ConcurrentMasstree::nodeversion_type oldv = v[sense];
    v[sense] = in->stable_annotated(ti.stable_fence());
    if (oldv.has_split(v[sense]) &&
        in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
      goto retry2;
    }
  }

  version = v[sense];
  lp.n_ = const_cast<ConcurrentMasstree::leaf_type*>(static_cast<const ConcurrentMasstree::leaf_type*>(n[sense]));
// end: reach_leaf

forward:
  if (version.deleted()) goto retry;

  lp.n_->prefetch();
  co_await std::experimental::suspend_always{};
  perm = lp.n_->permutation();
  fence();
  lp.kx_ = ConcurrentMasstree::leaf_type::bound_type::lower(lp.ka_, *lp.n_);
  if (lp.kx_.p >= 0) {
    ConcurrentMasstree::leafvalue_type lv = lp.n_->lv_[lp.kx_.p];
    lv.prefetch(lp.n_->keylenx_[lp.kx_.p]);
    co_await std::experimental::suspend_always{};
    lp.state_ = lp.n_->ksuf_matches(lp.kx_.p, lp.ka_);
    if (lp.state_ < 0 && !lp.n_->has_changed(version) && !lv.layer()->has_split()) {
      lp.ka_.shift_by(-lp.state_);
      root = lv.layer();
      goto retry;
    }
  } else
    lp.state_ = 0;

  lp.n_->lock(version, ti.lock_fence(tc_leaf_lock));
  if (lp.n_->has_changed(version) || lp.n_->permutation() != perm) {
    lp.n_->unlock();
    lp.n_ = lp.n_->advance_to_key(lp.ka_, version, ti);
    goto forward;
  } else if (unlikely(lp.state_ < 0)) {
    lp.ka_.shift_by(-lp.state_);
    lp.n_->lv_[lp.kx_.p] = root = lp.n_->lv_[lp.kx_.p].layer()->unsplit_ancestor();
    lp.n_->unlock();
    goto retry;
  } else if (unlikely(lp.n_->deleted_layer())) {
    lp.ka_.unshift_all();
    root = const_cast<ConcurrentMasstree::node_base_type*>(lp.root_);
    goto retry;
  }
// end: find_locked

  lp.original_n_ = lp.n_;
  lp.original_v_ = lp.n_->full_unlocked_version_value();
  bool found = true;

  // maybe we found it
  if (lp.state_) {
    found = true;
  } else {
    // otherwise mark as inserted but not present
    lp.state_ = 2;

    // maybe we need a new layer
    if (lp.kx_.p >= 0) {
      found = lp.make_new_layer(ti);
    } else {
      // mark insertion if we are changing modification state
      if (unlikely(lp.n_->modstate_ != ConcurrentMasstree::leaf_type::modstate_insert)) {
        masstree_invariant(lp.n_->modstate_ == ConcurrentMasstree::leaf_type::modstate_remove);
        lp.n_->mark_insert();
        lp.n_->modstate_ = ConcurrentMasstree::leaf_type::modstate_insert;
      }

      // try inserting into this node
      if (lp.n_->size() < lp.n_->width) {
        lp.kx_.p = ConcurrentMasstree::leaf_type::permuter_type(lp.n_->permutation_).back();
        // don't inappropriately reuse position 0, which holds the ikey_bound
        if (likely(lp.kx_.p != 0) || !lp.n_->prev_ || lp.n_->ikey_bound() == lp.ka_.ikey()) {
          lp.n_->assign(lp.kx_.p, lp.ka_, ti);
          found = false;
        }
      }

      // otherwise must split
      if (found)
        found = lp.make_split(ti);
    }
  }
// end: find_insert

  if (!found) {
insert_new:
    found = false;
    ti.advance_timestamp(lp.node_timestamp());
    lp.value() = oid;
    if (insert_info) {
      insert_info->node = lp.node();
      insert_info->old_version = lp.previous_full_version_value();
      insert_info->new_version = lp.next_full_version_value(1);
    }
  } else if (IsPrimary()) {
    // we have two cases: 1) predecessor's inserts are still remaining in tree,
    // even though version chain is empty or 2) somebody else are making dirty
    // data in this chain. If it's the first case, version chain is considered
    // empty, then we retry insert.
    OID o = lp.value();
    if (oidmgr->oid_get_latest_version(table_descriptor->GetTupleArray(), o))
      found = true;
    else
      goto insert_new;
  }
  lp.finish(!found, ti);
// end: insert_if_absent

  bool inserted = !found;
// end: InsertIfAbsent

  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
  }
// end: InsertOID

  if (!inserted)
    co_return rc_t{RC_ABORT_INTERNAL};

  if (out_oid) {
    *out_oid = oid;
  }

  co_return rc_t{RC_TRUE};
}

ermia::dia::generator<ConcurrentMasstree::coro_ScanIteratorForward>
ConcurrentMasstreeIndex::coro_IteratorScan(transaction *t,
                                           const varstr &start_key,
                                           const varstr *end_key,
                                           bool emit_firstkey) {
    typedef typename masstree_params::ikey_type ikey_type;
    typedef typename ConcurrentMasstree::node_type::key_type key_type;
    typedef typename ConcurrentMasstree::node_type::leaf_type::leafvalue_type leafvalue_type;

    ConcurrentMasstree::coro_ScanIteratorForward scan_iterator(
        t->xc, &masstree_, start_key, end_key);
    ConcurrentMasstree::threadinfo ti(t->xc->begin_epoch);

    auto &keybuf = scan_iterator.sinfo_.keybuf;
    key_type &ka = scan_iterator.sinfo_.ka;

    typedef Masstree::scanstackelt<masstree_params> mystack_type;
    mystack_type *const stack = scan_iterator.sinfo_.stack;
    int &stackpos = scan_iterator.sinfo_.stackpos;

    leafvalue_type &entry = scan_iterator.sinfo_.entry;

    int &state = scan_iterator.sinfo_.state;

    auto &helper = scan_iterator.helper_;

    while (1) {
        /* flattend function: stack[stackpos].find_initial(helper, ka,
         * emit_firstkey, entry, ti); */
        {
            /* args */
            mystack_type *find_initial_this = &stack[stackpos];
            mystack_type *find_initial_next = &stack[stackpos + 1];
            /* args */

            int kp, keylenx = 0;
            char suffixbuf[MASSTREE_MAXKEYLEN];
            Masstree::Str suffix;

        find_initial_retry_root:
            /* flattened function: n_ = root_->reach_leaf(ka, v_, ti); */
            {
                /* args */
                ConcurrentMasstree::node_base_type *reach_leaf_this = find_initial_this->root_;
                ConcurrentMasstree::nodeversion_type &version = find_initial_this->v_;
                /* args */

                const ConcurrentMasstree::node_base_type* n[2];
                ConcurrentMasstree::nodeversion_type v[2];
                bool sense;

            // Get a non-stale root.
            // Detect staleness by checking whether n has ever split.
            // The true root has never split.
            reach_leaf_retry:
                sense = false;
                n[sense] = reach_leaf_this;
                while (1) {
                    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
                    if (!v[sense].has_split()) break;
                    n[sense] = n[sense]->unsplit_ancestor();
                }

                // Loop over internal nodes.
                while (!v[sense].isleaf()) {
                    const ConcurrentMasstree::internode_type *in =
                        static_cast<const ConcurrentMasstree::internode_type *>(
                            n[sense]);
                    in->prefetch();
                    co_await std::experimental::suspend_always{};
                    int kp = ConcurrentMasstree::internode_type::bound_type::upper(ka, *in);
                    n[!sense] = in->child_[kp];
                    if (!n[!sense]) {
                        goto reach_leaf_retry;
                    }
                    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

                    if (likely(!in->has_changed(v[sense]))) {
                        sense = !sense;
                        continue;
                    }

                    ConcurrentMasstree::nodeversion_type oldv = v[sense];
                    v[sense] = in->stable_annotated(ti.stable_fence());
                    if (oldv.has_split(v[sense]) &&
                        in->stable_last_key_compare(ka, v[sense], ti) > 0) {
                        goto reach_leaf_retry;
                    }
                }

                version = v[sense];
                find_initial_this->n_ =
                    const_cast<ConcurrentMasstree::leaf_type *>(
                        static_cast<const ConcurrentMasstree::leaf_type *>(
                            n[sense]));
            }
            /* flattened function end: reach_leaf */

        find_initial_retry_node:
            if (find_initial_this->v_.deleted()) {
                goto find_initial_retry_root;
            }
            find_initial_this->n_->prefetch();
            co_await std::experimental::suspend_always{};
            find_initial_this->perm_ = find_initial_this->n_->permutation();

            find_initial_this->ki_ = helper.lower_with_position(ka, find_initial_this, kp);
            if (kp >= 0) {
                keylenx = find_initial_this->n_->keylenx_[kp];
                fence();
                entry = find_initial_this->n_->lv_[kp];
                entry.prefetch(keylenx);
                co_await std::experimental::suspend_always{};
                if (find_initial_this->n_->keylenx_has_ksuf(keylenx)) {
                    suffix = find_initial_this->n_->ksuf(kp);
                    memcpy(suffixbuf, suffix.s, suffix.len);
                    suffix.s = suffixbuf;
                }
            }
            if (find_initial_this->n_->has_changed(find_initial_this->v_)) {
                find_initial_this->n_ =
                    find_initial_this->n_->advance_to_key(ka, find_initial_this->v_, ti);
                goto find_initial_retry_node;
            }

            if (kp >= 0) {
                if (find_initial_this->n_->keylenx_is_layer(keylenx)) {
                    find_initial_next->root_ = entry.layer();
                    state = mystack_type::scan_down;
                    goto find_initial_return;
                } else if (find_initial_this->n_->keylenx_has_ksuf(keylenx)) {
                    int ksuf_compare = suffix.compare(ka.suffix());
                    if (helper.initial_ksuf_match(ksuf_compare,
                                                  emit_firstkey)) {
                        int keylen = ka.assign_store_suffix(suffix);
                        ka.assign_store_length(keylen);
                        state = mystack_type::scan_emit;
                        goto find_initial_return;
                    }
                } else if (emit_firstkey) {
                    state = mystack_type::scan_emit;
                    goto find_initial_return;
                }
                // otherwise, this entry must be skipped
                find_initial_this->ki_ = helper.next(find_initial_this->ki_);
            }

            state = mystack_type::scan_find_next;
            goto find_initial_return;
    find_initial_return:
            ;
        }
        /* flattend function end: find_initial */
        scan_iterator.scanner_.visit_leaf(stack[stackpos], ka, ti);
        if (state != mystack_type::scan_down) break;
        ka.shift();
        ++stackpos;
    }

    co_return scan_iterator;
}

ermia::dia::generator<rc_t> ConcurrentMasstreeIndex::coro_Scan(transaction *t,
                            const varstr &start_key, const varstr *end_key,
                            ScanCallback &callback, uint32_t max_keys) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();

  if (unlikely(end_key && *end_key <= start_key)) {
    co_return c.return_code;
  }

  XctSearchRangeCallback cb(t, &c);

  ConcurrentMasstree:: low_level_search_range_scanner<false>
    scanner(&masstree_, end_key ? end_key : nullptr, cb);
  ConcurrentMasstree::threadinfo ti(t->xc->begin_epoch);
  auto firstkey = lcdf::Str(start_key.data(), start_key.size());
  auto helper = Masstree::forward_scan_helper();
  auto &table = *masstree_.get_table();

  typedef typename masstree_params::ikey_type ikey_type;
  typedef typename ConcurrentMasstree::node_type::key_type key_type;
  typedef typename ConcurrentMasstree::node_type::leaf_type::leafvalue_type leafvalue_type;
  union {
    ikey_type
        x[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1) / sizeof(ikey_type)];
    char s[MASSTREE_MAXKEYLEN];
  } keybuf;
  masstree_precondition(firstkey.len <= (int)sizeof(keybuf));
  memcpy(keybuf.s, firstkey.s, firstkey.len);
  key_type ka(keybuf.s, firstkey.len);

  typedef Masstree::scanstackelt<masstree_params> mystack_type;
  mystack_type
      stack[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1) / sizeof(ikey_type)];
  int stackpos = 0;
  stack[0].root_ = table.root_;
  leafvalue_type entry = leafvalue_type::make_empty();

  int scancount = 0;
  int state;
  bool emit_firstkey = true;

  while (1) {
    {
      auto &s = stack[stackpos];
      int kp, keylenx = 0;
      char suffixbuf[MASSTREE_MAXKEYLEN];
      Masstree::Str suffix;

    find_initial_retry_root:
    {
      const ConcurrentMasstree::node_base_type* n[2];
      ConcurrentMasstree::nodeversion_type v[2];
      bool sense;

    __reach_leaf_retry:
      sense = false;
      n[sense] = s.root_;
      while (1) {
        v[sense] = n[sense]->stable_annotated(ti.stable_fence());
        if (!v[sense].has_split()) break;
        n[sense] = n[sense]->unsplit_ancestor();
      }

      // Loop over internal nodes.
      while (!v[sense].isleaf()) {
        const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
        in->prefetch();
        co_await std::experimental::suspend_always{};
        int kp = ConcurrentMasstree::internode_type::bound_type::upper(ka, *in);
        n[!sense] = in->child_[kp];
        if (!n[!sense]) goto __reach_leaf_retry;
        v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

        if (likely(!in->has_changed(v[sense]))) {
          sense = !sense;
          continue;
        }

        ConcurrentMasstree::nodeversion_type oldv = v[sense];
        v[sense] = in->stable_annotated(ti.stable_fence());
        if (oldv.has_split(v[sense]) &&
            in->stable_last_key_compare(ka, v[sense], ti) > 0) {
          goto __reach_leaf_retry;
        }
      }

      s.v_ = v[sense];
      s.n_ = const_cast<ConcurrentMasstree::leaf_type *>(static_cast<const ConcurrentMasstree::leaf_type *>(n[sense]));
    }

    find_initial_retry_node:
      if (s.v_.deleted())
        goto find_initial_retry_root;
      s.n_->prefetch();
      co_await std::experimental::suspend_always{};

      s.perm_ = s.n_->permutation();

      s.ki_ = helper.lower_with_position(ka, &s, kp);
      if (kp >= 0) {
        keylenx = s.n_->keylenx_[kp];
        fence();
        entry = s.n_->lv_[kp];
        entry.prefetch(keylenx);
        co_await std::experimental::suspend_always{};

        if (s.n_->keylenx_has_ksuf(keylenx)) {
          suffix = s.n_->ksuf(kp);
          memcpy(suffixbuf, suffix.s, suffix.len);
          suffix.s = suffixbuf;
        }
      }
      if (s.n_->has_changed(s.v_)) {
        s.n_ = s.n_->advance_to_key(ka, s.v_, ti);
        goto find_initial_retry_node;
      }

      if (kp >= 0) {
        if (s.n_->keylenx_is_layer(keylenx)) {
          (&s)[1].root_ = entry.layer();
          state =  mystack_type::scan_down;
          goto find_initial_done;
        } else if (s.n_->keylenx_has_ksuf(keylenx)) {
          int ksuf_compare = suffix.compare(ka.suffix());
          if (helper.initial_ksuf_match(ksuf_compare, emit_firstkey)) {
            int keylen = ka.assign_store_suffix(suffix);
            ka.assign_store_length(keylen);
            state =  mystack_type::scan_emit;
            goto find_initial_done;
          }
        } else if (emit_firstkey) {
          state =  mystack_type::scan_emit;
          goto find_initial_done;
        }
        // otherwise, this entry must be skipped
        s.ki_ = helper.next(s.ki_);
      }

      state =  mystack_type::scan_find_next;
    }

  find_initial_done:
    scanner.visit_leaf(stack[stackpos], ka, ti);
    if (state != mystack_type::scan_down) {
      break;
    }
    ka.shift();
    ++stackpos;
  }

  auto *tuple_array = table_descriptor->GetTupleArray();
  uint32_t cnt = 0;
  std::vector<OIDAMACState> version_requests;
  std::vector<key_type> keys;

  while (1) {
    switch (state) {
    case mystack_type::scan_emit: { // surpress cross init warning about v
      ++scancount;
      ermia::dbtuple *v = NULL;
      ermia::OID o = entry.value();
      ++cnt;
      version_requests.emplace_back(o);
      keys.emplace_back(ka);
      if (!scanner.visit_value_no_callback(ka)) {
        goto done;
      }

      if (cnt == max_keys) {
        goto done;
      }

      /*
      v = sync_wait_coro(ermia::oidmgr->oid_get_version(tuple_array, o, t->xc));
      if (v) {
        if (!scanner.visit_value(ka, v))
          goto done;
      }
      */
      stack[stackpos].ki_ = helper.next(stack[stackpos].ki_);
      {
      //state = stack[stackpos].find_next(helper, ka, entry);
      auto &s = stack[stackpos];
      int kp;

      if (s.v_.deleted()) {
        state = mystack_type::scan_retry;
        goto __find_next_done;
      }

    __find_next_retry_entry:
      kp = s.kp();
      if (kp >= 0) {
        ikey_type ikey = s.n_->ikey0_[kp];
        int keylenx = s.n_->keylenx_[kp];
        int keylen = keylenx;
        fence();
        entry = s.n_->lv_[kp];
        entry.prefetch(keylenx);
        co_await std::experimental::suspend_always{};
        if (s.n_->keylenx_has_ksuf(keylenx))
          keylen = ka.assign_store_suffix(s.n_->ksuf(kp));

        if (s.n_->has_changed(s.v_))
          goto __find_next_changed;
        else if (helper.is_duplicate(ka, ikey, keylenx)) {
          s.ki_ = helper.next(s.ki_);
          goto __find_next_retry_entry;
        }

        // We know we can emit the data collected above.
        ka.assign_store_ikey(ikey);
        helper.found();
        if (s.n_->keylenx_is_layer(keylenx)) {
          (&s)[1].root_ = entry.layer();
          state = mystack_type::scan_down;
          goto __find_next_done;
        } else {
          ka.assign_store_length(keylen);
          state = mystack_type::scan_emit;
          goto __find_next_done;
        }
      }

      if (!s.n_->has_changed(s.v_)) {
        s.n_ = helper.advance(s.n_, ka);
        if (!s.n_) {
          state = mystack_type::scan_up;
          goto __find_next_done;
        }
        s.n_->prefetch();
        co_await std::experimental::suspend_always{};
      }

    __find_next_changed:
      s.v_ = helper.stable(s.n_, ka);
      s.perm_ = s.n_->permutation();
      s.ki_ = helper.lower(ka, &s);
      state = mystack_type::scan_find_next;
    }
    __find_next_done:
      ;
    } break;

    case mystack_type::scan_find_next:
    find_next:
      state = stack[stackpos].find_next(helper, ka, entry);
      if (state != mystack_type::scan_up)
        scanner.visit_leaf(stack[stackpos], ka, ti);
      break;

    case mystack_type::scan_up:
      do {
        if (--stackpos < 0)
          goto done;
        ka.unshift();
        stack[stackpos].ki_ = helper.next(stack[stackpos].ki_);
      } while (unlikely(ka.empty()));
      goto find_next;

    case mystack_type::scan_down:
      helper.shift_clear(ka);
      ++stackpos;
      goto retry;

    case mystack_type::scan_retry:
    retry:
      {
      //state = sync_wait_coro(stack[stackpos].find_retry(helper, ka, ti));
      auto &s = stack[stackpos];
    __find_retry_retry:
      {
      //s.n_ = sync_wait_coro(s.root_->reach_leaf(ka, s.v_, ti));
      const ConcurrentMasstree::node_base_type* n[2];
      ConcurrentMasstree::nodeversion_type v[2];
      bool sense;

    __reach_leaf_retry2:
      sense = false;
      n[sense] = s.root_;
      while (1) {
        v[sense] = n[sense]->stable_annotated(ti.stable_fence());
        if (!v[sense].has_split()) break;
        n[sense] = n[sense]->unsplit_ancestor();
      }

      // Loop over internal nodes.
      while (!v[sense].isleaf()) {
        const ConcurrentMasstree::internode_type* in = static_cast<const ConcurrentMasstree::internode_type*>(n[sense]);
        in->prefetch();
        co_await std::experimental::suspend_always{};
        int kp = ConcurrentMasstree::internode_type::bound_type::upper(ka, *in);
        n[!sense] = in->child_[kp];
        if (!n[!sense]) goto __reach_leaf_retry2;
        v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

        if (likely(!in->has_changed(v[sense]))) {
          sense = !sense;
          continue;
        }

        ConcurrentMasstree::nodeversion_type oldv = v[sense];
        v[sense] = in->stable_annotated(ti.stable_fence());
        if (oldv.has_split(v[sense]) &&
            in->stable_last_key_compare(ka, v[sense], ti) > 0) {
          goto __reach_leaf_retry2;
        }
      }

      s.v_ = v[sense];
      s.n_ = const_cast<ConcurrentMasstree::leaf_type *>(static_cast<const ConcurrentMasstree::leaf_type *>(n[sense]));
      }

      if (s.v_.deleted()) {
        goto __find_retry_retry;
      }

      s.n_->prefetch();
      co_await std::experimental::suspend_always{};
      s.perm_ = s.n_->permutation();
      s.ki_ = helper.lower(ka, &s);
      state = mystack_type::scan_find_next;
      }
      break;
    }
  }
done:

  ALWAYS_ASSERT(cnt == version_requests.size());
  if (cnt > 0) {
    oidmgr->oid_get_version_amac(tuple_array, version_requests, t->xc);
    for (uint32_t i = 0; i < version_requests.size(); ++i) {
      cb.invoke(&masstree_, keys[i].full_string(), version_requests[i].tuple, nullptr, 0);
    }
  }
  co_return c.return_code;
}
} // namespace ermia
