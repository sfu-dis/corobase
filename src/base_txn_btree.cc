#include "base_txn_btree.h"

rc_t
base_txn_btree::do_search(transaction &t, const varstr &k, value_reader &vr)
{
    t.ensure_active();

    key_writer key_writer(&k);
    const varstr * const key_str =
    key_writer.fully_materialize(true, t.string_allocator());

    // search the underlying btree to map k=>(btree_node|tuple)
    dbtuple * tuple{};
    OID oid;
    concurrent_btree::versioned_node_t sinfo;
    const bool found = this->underlying_btree.search(varkey(key_str), oid, tuple, t.xc, &sinfo);
    if (found)
        return t.do_tuple_read(tuple, vr);
#ifdef PHANTOM_PROT_NODE_SET
    else {
        rc_t rc = t.do_node_read(sinfo.first, sinfo.second);
        if (rc_is_abort(rc))
            return rc;
    }
#endif
    return rc_t{RC_FALSE};
}

std::map<std::string, uint64_t>
base_txn_btree::unsafe_purge(bool dump_stats)
{
    ALWAYS_ASSERT(!been_destructed);
    been_destructed = true;
    purge_tree_walker w;
    underlying_btree.tree_walk(w);
    underlying_btree.clear();
    return std::map<std::string, uint64_t>();
}

void
base_txn_btree::purge_tree_walker::on_node_begin(const typename concurrent_btree::node_opaque_t *n)
{
    INVARIANT(spec_values.empty());
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

rc_t base_txn_btree::do_tree_put(
    transaction &t,
    const varstr *k,
    const varstr *v,
    bool expect_new)
{
    INVARIANT(k);
    INVARIANT(!expect_new || v); // makes little sense to remove() a key you expect
                                 // to not be present, so we assert this doesn't happen
                                 // for now [since this would indicate a suboptimality]
    t.ensure_active();
    if (expect_new) {
        if (t.try_insert_new_tuple(&this->underlying_btree, k, v, this->fid))
            return rc_t{RC_TRUE};
        // FIXME(tzwang): May 12, 2016 keep this upsert behavior for now, aborting causes
        // problem in recovery - even single-thread Payment in TPC-C aborts.
        //else
        //    return rc_t{RC_ABORT_INTERNAL};
    }

    // do regular search
    dbtuple * bv = 0;
    OID oid = 0;
    if (!this->underlying_btree.search(varkey(k), oid, bv, t.xc))
        return rc_t{RC_ABORT_INTERNAL};

    // first *updater* wins
    fat_ptr new_obj_ptr = NULL_PTR;
    fat_ptr prev_obj_ptr = oidmgr->oid_put_update(this->underlying_btree.tuple_vec(), oid, v, t.xc, &new_obj_ptr);
    dbtuple *tuple = ((object *)new_obj_ptr.offset())->tuple();
    ASSERT(tuple);

    if (prev_obj_ptr != NULL_PTR) { // succeeded
        dbtuple *prev = ((object *)prev_obj_ptr.offset())->tuple();
        ASSERT((uint64_t)prev->get_object() == prev_obj_ptr.offset());
        ASSERT(t.xc);
#ifdef USE_PARALLEL_SSI
        ASSERT(prev->sstamp == NULL_PTR);
        if (t.xc->ct3) {
            // Check if we are the T2 with a committed T3 earlier than a safesnap (being T1)
            if (t.xc->ct3 <= t.xc->last_safesnap)
                return {RC_ABORT_SERIAL};

            if (volatile_read(prev->xstamp) >= t.xc->ct3 or not prev->readers_bitmap.is_empty(true)) {
                // Read-only optimization: safe if T1 is read-only (so far) and T1's begin ts
                // is before ct3.
                if (sysconf::enable_ssi_read_only_opt) {
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
                        reader_begin = volatile_read(reader_xc->begin).offset();
                        reader_owner = volatile_read(reader_xc->owner);
                        if (reader_owner != rxid)  // consult xstamp later
                            continue;

                        // we're safe if the reader is read-only (so far) and started after ct3
                        if (sysconf::enable_ssi_read_only_opt and
                            reader_xc->xct->write_set.size() == 0 and
                            reader_begin >= t.xc->ct3) {
                            oidmgr->oid_unlink(this->underlying_btree.tuple_vec(), oid, tuple);
                            return {RC_ABORT_SERIAL};
                        }
                    }
                }
                else {
                    oidmgr->oid_unlink(this->underlying_btree.tuple_vec(), oid, tuple);
                    return {RC_ABORT_SERIAL};
                }
            }
        }
#endif
#ifdef USE_PARALLEL_SSN
        // update hi watermark
        // Overwriting a version could trigger outbound anti-dep,
        // i.e., I'll depend on some tx who has read the version that's
        // being overwritten by me. So I'll need to see the version's
        // access stamp to tell if the read happened.
        ASSERT(prev->sstamp == NULL_PTR);
        auto prev_xstamp = volatile_read(prev->xstamp);
        if (t.xc->pstamp < prev_xstamp)
            t.xc->pstamp = prev_xstamp;

#ifdef DO_EARLY_SSN_CHECKS
        if (not ssn_check_exclusion(t.xc)) {
            // unlink the version here (note abort_impl won't be able to catch
            // it because it's not yet in the write set)
            oidmgr->oid_unlink(this->underlying_btree.tuple_vec(), oid, tuple);
            return rc_t{RC_ABORT_SERIAL};
        }
#endif

        // copy access stamp to new tuple from overwritten version
        // (no need to copy sucessor lsn (slsn))
        volatile_write(tuple->xstamp, prev->xstamp);
#endif

        // read prev's clsn first, in case it's a committing XID, the clsn's state
        // might change to ASI_LOG anytime
        ASSERT((uint64_t)prev->get_object() == prev_obj_ptr.offset());
        fat_ptr prev_clsn = volatile_read(prev->get_object()->_clsn);
        if (prev_clsn.asi_type() == fat_ptr::ASI_XID and XID::from_ptr(prev_clsn) == t.xid) {
            // updating my own updates!
            // prev's prev: previous *committed* version
            ASSERT(prev->is_defunct()); // oid_put_update did this
            ASSERT(((object *)prev_obj_ptr.offset())->_alloc_epoch == t.xc->begin_epoch);
            // MM::deallocate(prev_obj_ptr);  // FIXME: recycle this
        }
        else {  // prev is committed (or precommitted but in post-commit now) head
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
            volatile_write(prev->sstamp, t.xc->owner.to_ptr());
#endif
        }

        ASSERT(not tuple->pvalue or tuple->pvalue->size() == tuple->size);
        t.add_to_write_set(new_obj_ptr, this->underlying_btree.tuple_vec(), oid);
        ASSERT(tuple->get_object()->_clsn.asi_type() == fat_ptr::ASI_XID);
        ASSERT(oidmgr->oid_get_version(fid, oid, t.xc) == tuple);
        INVARIANT(t.log);
        if (not v)
            t.log->log_delete(this->fid, oid);
        else {
            ASSERT(v);
            // the logmgr only assignspointers, instead of doing memcpy here,
            // unless the record is tooooo large.
            const size_t sz = v->size();
            ASSERT(sz == v->size());
            auto record_size = align_up(sz) + sizeof(varstr);
            auto size_code = encode_size_aligned(record_size);
            ASSERT(not ((uint64_t)v & ((uint64_t)0xf)));
            // log the whole varstr so that recovery can figure out the real size
            // of the tuple, instead of using the decoded (larger-than-real) size.
            t.log->log_update(this->fid, oid, fat_ptr::make((void *)v, size_code),
                              DEFAULT_ALIGNMENT_BITS, &tuple->get_object()->_pdest);
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
#ifdef PHANTOM_PROT_NODE_SET
#ifdef USE_PARALLEL_SSN
    if (t->flags & transaction::TXN_FLAG_READ_ONLY)
        return;
#endif
    rc_t rc = t->do_node_read(n, version);
    if (rc_is_abort(rc))
        caller_callback->return_code = rc;
#endif
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
    caller_callback->return_code = t->do_tuple_read(v, *vr);
    if (caller_callback->return_code._val == RC_TRUE)
        return caller_callback->invoke((*kr)(k), vr->results());
    else if (rc_is_abort(caller_callback->return_code))
        return false;   // don't continue the read if the tx should abort
                        // ^^^^^ note: see masstree_scan.hh, whose scan() calls
                        // visit_value(), which calls this function to determine
                        // if it should stop reading.
    return true;
}

void
base_txn_btree::do_search_range_call(
    transaction &t,
    const varstr &lower,
    const varstr *upper,
    search_range_callback &callback,
    key_reader &key_reader,
    value_reader &value_reader)
{
    t.ensure_active();
    if (upper)
        VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                     << ")::search_range_call [" << util::hexify(lower)
                     << ", " << util::hexify(*upper) << ")" << std::endl);
    else
        VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                     << ")::search_range_call [" << util::hexify(lower)
                     << ", +inf)" << std::endl);

    key_writer lower_key_writer(&lower);
    const varstr * const lower_str =
        lower_key_writer.fully_materialize(true, t.string_allocator());

    key_writer upper_key_writer(upper);
    const varstr * const upper_str =
        upper_key_writer.fully_materialize(true, t.string_allocator());

    if (unlikely(upper_str && *upper_str <= *lower_str))
        return;

    txn_search_range_callback c(&t, &callback, &key_reader, &value_reader);

    varkey uppervk;
    if (upper_str)
        uppervk = varkey(upper_str);
    this->underlying_btree.search_range_call(
        varkey(lower_str), upper_str ? &uppervk : nullptr,
        c, t.xc);
}

void
base_txn_btree::do_rsearch_range_call(
    transaction &t,
    const varstr &upper,
    const varstr *lower,
    search_range_callback &callback,
    key_reader &key_reader,
    value_reader &value_reader)
{
    t.ensure_active();

    key_writer lower_key_writer(lower);
    const varstr * const lower_str =
        lower_key_writer.fully_materialize(true, t.string_allocator());

    key_writer upper_key_writer(&upper);
    const varstr * const upper_str =
        upper_key_writer.fully_materialize(true, t.string_allocator());

    if (unlikely(lower_str && *upper_str <= *lower_str))
        return;

    txn_search_range_callback c(&t, &callback, &key_reader, &value_reader);

    varkey lowervk;
    if (lower_str)
        lowervk = varkey(lower_str);
    this->underlying_btree.rsearch_range_call(
        varkey(upper_str), lower_str ? &lowervk : nullptr,
        c, t.xc);
}

