#include "base_txn_btree.h"
#include "masstree_btree.h"
#include "txn.h"

write_set_t tls_write_set[config::MAX_THREADS];

rc_t
base_txn_btree::do_search(transaction &t, const varstr &k, varstr *out_v, OID* out_oid) {
    t.ensure_active();

    // search the underlying btree to map k=>(btree_node|tuple)
    dbtuple * tuple{};
    OID oid;
    concurrent_btree::versioned_node_t sinfo;
    bool found = this->underlying_btree.search(k, oid, tuple, t.xc, &sinfo);
    if(out_oid) {
      *out_oid = oid;
    }
    if (found) {
        return t.do_tuple_read(tuple, out_v);
    } else if(config::phantom_prot) {
        rc_t rc = t.do_node_read(sinfo.first, sinfo.second);
        if (rc_is_abort(rc)) {
            return rc;
        }
    }
    return rc_t{RC_FALSE};
}

std::map<std::string, uint64_t>
base_txn_btree::unsafe_purge(bool dump_stats)
{
    purge_tree_walker w;
    underlying_btree.tree_walk(w);
    underlying_btree.clear();
    return std::map<std::string, uint64_t>();
}

void
base_txn_btree::purge_tree_walker::on_node_begin(const typename concurrent_btree::node_opaque_t *n)
{
    ASSERT(spec_values.empty());
    spec_values = concurrent_btree::ExtractValues(n);
}

void
base_txn_btree::purge_tree_walker::on_node_success()
{
    spec_values.clear();
}

void
base_txn_btree::purge_tree_walker::on_node_failure()
{
    spec_values.clear();
}

rc_t base_txn_btree::do_tree_put(transaction &t, const varstr *k, varstr *v,
                                 bool expect_new, bool upsert, OID* inserted_oid) {
    ASSERT(k);
    ASSERT(!expect_new || v); // makes little sense to remove() a key you expect
                                 // to not be present, so we assert this doesn't happen
                                 // for now [since this would indicate a suboptimality]
    t.ensure_active();
    if (expect_new) {
      if(t.try_insert_new_tuple(&this->underlying_btree, k, v, inserted_oid)) {
        return rc_t{RC_TRUE};
      } else if (!upsert) {
        return rc_t{RC_ABORT_INTERNAL};
      }
    }

    // do regular search
    dbtuple * bv = 0;
    OID oid = 0;
    if (!this->underlying_btree.search(*k, oid, bv, t.xc))
        return rc_t{RC_ABORT_INTERNAL};

    auto* id = this->descriptor;
    oid_array* tuple_array = id->GetTupleArray();
    FID tuple_fid = id->GetTupleFid();

    // first *updater* wins
    fat_ptr new_obj_ptr = NULL_PTR;
    fat_ptr prev_obj_ptr = oidmgr->PrimaryTupleUpdate(tuple_array, oid, v, t.xc, &new_obj_ptr);
    Object* prev_obj = (Object*)prev_obj_ptr.offset();

    if(prev_obj) { // succeeded
        dbtuple *tuple = ((Object*)new_obj_ptr.offset())->GetPinnedTuple();
        ASSERT(tuple);
        dbtuple *prev = prev_obj->GetPinnedTuple();
        ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
        ASSERT(t.xc);
#ifdef SSI
        ASSERT(prev->sstamp == NULL_PTR);
        if (t.xc->ct3) {
            // Check if we are the T2 with a committed T3 earlier than a safesnap (being T1)
            if (t.xc->ct3 <= t.xc->last_safesnap)
                return {RC_ABORT_SERIAL};

            if (volatile_read(prev->xstamp) >= t.xc->ct3 or not prev->readers_bitmap.is_empty(true)) {
                // Read-only optimization: safe if T1 is read-only (so far) and T1's begin ts
                // is before ct3.
                if (config::enable_ssi_read_only_opt) {
                    readers_bitmap_iterator readers_iter(&prev->readers_bitmap);
                    while (true) {
                        int32_t xid_idx = readers_iter.next(true);
                        if (xid_idx == -1)
                            break;

                        XID rxid = volatile_read(rlist.xids[xid_idx]);
                        ASSERT(rxid != t.xc->owner);
                        if (rxid == INVALID_XID)    // reader is gone, check xstamp in the end
                            continue;

                        XID reader_owner = INVALID_XID;
                        uint64_t reader_begin = 0;
                        xid_context *reader_xc = NULL;
                        reader_xc = xid_get_context(rxid);
                        if (not reader_xc)  // context change, consult xstamp later
                            continue;

                        // copy everything before doing anything
                        reader_begin = volatile_read(reader_xc->begin);
                        reader_owner = volatile_read(reader_xc->owner);
                        if (reader_owner != rxid)  // consult xstamp later
                            continue;

                        // we're safe if the reader is read-only (so far) and started after ct3
                        if (reader_xc->xct->write_set.size() > 0 and reader_begin <= t.xc->ct3) {
                            oidmgr->PrimaryTupleUnlink(tuple_array, oid);
                            return {RC_ABORT_SERIAL};
                        }
                    }
                }
                else {
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
        if (t.xc->pstamp < prev_xstamp)
            t.xc->pstamp = prev_xstamp;

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
        if (prev_clsn.asi_type() == fat_ptr::ASI_XID and XID::from_ptr(prev_clsn) == t.xid) {
            // updating my own updates!
            // prev's prev: previous *committed* version
            ASSERT(((Object*)prev_obj_ptr.offset())->GetAllocateEpoch() == t.xc->begin_epoch);
            prev_persistent_ptr = prev_obj->GetNextPersistent();
            MM::deallocate(prev_obj_ptr);
        }
        else {  // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN)
            volatile_write(prev->sstamp, t.xc->owner.to_ptr());
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
        if(!v) {
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
        if(is_delete) {
          t.log->log_enhanced_delete(tuple_fid, oid, fat_ptr::make((void*)v, size_code),
                                     DEFAULT_ALIGNMENT_BITS);
        } else {
          t.log->log_update(tuple_fid, oid, fat_ptr::make((void *)v, size_code),
                            DEFAULT_ALIGNMENT_BITS,
                            tuple->GetObject()->GetPersistentAddressPtr());

          if(config::log_key_for_update) {
            auto key_size = align_up(k->size() + sizeof(varstr));
            auto key_size_code = encode_size_aligned(key_size);
            t.log->log_update_key(tuple_fid, oid, fat_ptr::make((void *)k, key_size_code),
                                  DEFAULT_ALIGNMENT_BITS);
          }
        }
        return rc_t{RC_TRUE};
    }
    else {  // somebody else acted faster than we did
        return rc_t{RC_ABORT_SI_CONFLICT};
    }
}

void
base_txn_btree
  ::txn_search_range_callback
  ::on_resp_node(
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
    VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
               << ", version=" << version << ">" << std::endl);
    VERBOSE(std::cerr << "  " << concurrent_btree::NodeStringify(n) << std::endl);
    if(config::phantom_prot) {
#ifdef SSN
      if(t->flags & transaction::TXN_FLAG_READ_ONLY) {
        return;
      }
#endif
      rc_t rc = t->do_node_read(n, version);
      if(rc_is_abort(rc)) {
        caller_callback->return_code = rc;
      }
    }
}

bool
base_txn_btree
  ::txn_search_range_callback
  ::invoke(
    const concurrent_btree *btr_ptr,
    const typename concurrent_btree::string_type &k, dbtuple *v,
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
    t->ensure_active();
    VERBOSE(std::cerr << "search range k: " << util::hexify(k)
                      << " from <node=0x" << util::hexify(n)
                      << ", version=" << version << ">" << std::endl
                      << "  " << *((dbtuple *) v) << std::endl);
    varstr vv;
    caller_callback->return_code = t->do_tuple_read(v, &vv);
    if (caller_callback->return_code._val == RC_TRUE)
        return caller_callback->invoke(k, vv);
    else if (rc_is_abort(caller_callback->return_code))
        return false;   // don't continue the read if the tx should abort
                        // ^^^^^ note: see masstree_scan.hh, whose scan() calls
                        // visit_value(), which calls this function to determine
                        // if it should stop reading.
    return true;
}

void
base_txn_btree::do_search_range_call(transaction &t, const varstr &lower,
                                     const varstr *upper,
                                     search_range_callback &callback) {
    t.ensure_active();
    if (upper)
        VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                     << ")::search_range_call [" << util::hexify(lower)
                     << ", " << util::hexify(*upper) << ")" << std::endl);
    else
        VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                     << ")::search_range_call [" << util::hexify(lower)
                     << ", +inf)" << std::endl);

    if (unlikely(upper && *upper <= lower))
        return;

    txn_search_range_callback c(&t, &callback);

    varstr uppervk;
    if (upper)
        uppervk = *upper;
    this->underlying_btree.search_range_call(
        lower, upper ? &uppervk : nullptr,
        c, t.xc);
}

void
base_txn_btree::do_rsearch_range_call(transaction &t,
                                      const varstr &upper,
                                      const varstr *lower,
                                      search_range_callback &callback) {
    t.ensure_active();
    if (unlikely(lower && upper <= *lower))
        return;

    txn_search_range_callback c(&t, &callback);

    varstr lowervk;
    if (lower)
        lowervk = *lower;
    this->underlying_btree.rsearch_range_call(
        upper, lower ? &lowervk : nullptr,
        c, t.xc);
}

