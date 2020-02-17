#include "dbcore/rcu.h"
#include "dbcore/sm-chkpt.h"
#include "dbcore/sm-cmd-log.h"
#include "dbcore/sm-rep.h"

#include "ermia.h"
#include "txn.h"

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

ermia::dia::generator<rc_t> ConcurrentMasstreeIndex::coro_GetRecord(transaction *t, const varstr &key,
                                                                    varstr &value, OID *out_oid) {
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
  typename ConcurrentMasstree::node_base_type::nodeversion_type v[2];
  bool sense;

retry2:
  sense = false;
  n[sense] = lp.root_;
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

    typename ConcurrentMasstree::node_base_type::nodeversion_type oldv = v[sense];
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

  bool found = match;
  dbtuple *tuple = nullptr;
  if (found) {
// start: oid_get_version
    oid_array *oa = table_descriptor->GetTupleArray();
    TXN::xid_context *visitor_xc = t->xc;
    fat_ptr *entry = oa->get(oid);
start_over:
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
      if (config::is_backup_srv() && !config::command_log) {
        oidmgr->oid_get_version_backup(ptr, tentative_next, prev_obj, cur_obj, visitor_xc);
      } else {
        ASSERT(ptr.asi_type() == 0);
        cur_obj = (Object *)ptr.offset();
        ::prefetch((const char*)cur_obj);
        co_await std::experimental::suspend_always{};
        tentative_next = cur_obj->GetNextVolatile();
        ASSERT(tentative_next.asi_type() == 0);
      }

      bool retry = false;
      bool visible = oidmgr->TestVisibility(cur_obj, visitor_xc, retry);
      if (retry) {
        goto start_over;
      }
      if (visible) {
        tuple = cur_obj->GetPinnedTuple();
      }
      ptr = tentative_next;
      prev_obj = cur_obj;
    }
// end: oid_get_version
    if (!tuple) {
      found = false;
    }
  }

  if (found)
    volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
  else
    volatile_write(rc._val, RC_FALSE);

  ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);

  if (out_oid) {
    *out_oid = oid;
  }
  co_return rc;
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
  typename ConcurrentMasstree::node_base_type::nodeversion_type v[2];
  bool sense;

retry2:
  sense = false;
  n[sense] = lp.root_;
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

    typename ConcurrentMasstree::node_base_type::nodeversion_type oldv = v[sense];
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
    rc = t->Update(table_descriptor, oid, &key, &value);
  } else {
    rc = rc_t{RC_ABORT_INTERNAL};
  }

  co_return rc;
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

} // namespace ermia
