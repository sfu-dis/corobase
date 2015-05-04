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
    const bool found = this->underlying_btree.search(varkey(key_str), this->fid, oid, tuple, t.xc, &sinfo);
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

    object *obj = object::create_tuple_object(v, false);
    dbtuple *tuple = obj->tuple();

    // initialize the version
    tuple->clsn = t.xid.to_ptr();		// XID state is set

    // FIXME: tzwang: try_insert_new_tuple only tries once (no waiting, just one cas),
    // it fails if somebody else acted faster to insert new, we then
    // (fall back to) with the normal update procedure.
    // try_insert_new_tuple should add tuple to write-set too, if succeeded.
    if (expect_new and t.try_insert_new_tuple(&this->underlying_btree, k, v, obj, this->fid))
        return rc_t{RC_TRUE};

    // do regular search
    dbtuple * bv = 0;
    OID oid = 0;
    if (!this->underlying_btree.search(varkey(k), this->fid, oid, bv, t.xc))
        return rc_t{RC_ABORT_INTERNAL};
#ifdef PHANTOM_PROT_TABLE_LOCK
    // for delete
    bool instant_lock = false;
    table_lock_t *l = NULL;
    if (not v) {
        l = this->underlying_btree.get_tuple_vector()->lock_ptr();
        transaction::table_lock_set_t::iterator it =
            std::find(t.table_locks.begin(), t.table_locks.end(), l);
        if (it == t.table_locks.end()) {
            if (not object_vector::lock(l, TABLE_LOCK_X))
                return rc_t{RC_ABORT_PHANTOM};
            instant_lock = true;
        }
        else {
            if (not object_vector::upgrade_lock(l))
                return rc_t{RC_ABORT_PHANTOM};
        }
        ASSERT((volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_X or
               (volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX);
    }
#endif

    // first *committer* wins
    dbtuple *prev = oidmgr->oid_put_update(fid, oid, obj, t.xc);

    if (prev) { // succeeded
        ASSERT(t.xc);
#ifdef USE_PARALLEL_SSI
        // if I clobberred an old tuple, then as the T2 I have to assume
        // a committed T3 exists for me; so set it to its minimum.
        if (prev->is_old(t.xc))
            volatile_write(t.xc->ct3, 1);
        // check if there's any in-flight readers of the overwritten tuple
        // (will form an inbound r:w edge to me) ie, am I the T2 (pivot)
        // with T1 in-flight and T3 committed first (ie, before T1, ie,
        // prev's creator) in the dangerous structure?
        ASSERT(prev->sstamp == NULL_PTR);
        // the read-opt makes the readers list inaccurate, so we only do
        // the check here if read-opt is not enabled
        if (not has_read_opt() and t.xc->ct3 and serial_get_tuple_readers(prev, true)) {
            // unlink the version here (note abort_impl won't be able to catch
            // it because it's not yet in the write set), same as in SSN impl.
            oidmgr->oid_unlink(this->fid, oid, tuple);
#ifdef PHANTOM_PROT_TABLE_LOCK
            if (instant_lock)
                object_vector::unlock(l);
#endif
            return rc_t{RC_ABORT_SERIAL};
        }
#endif
        object *prev_obj = (object *)((char *)prev - sizeof(object));
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
            oidmgr->oid_unlink(this->fid, oid, tuple);
#ifdef PHANTOM_PROT_TABLE_LOCK
            if (instant_lock)
                object_vector::unlock(l);
#endif
            return rc_t{RC_ABORT_SERIAL};
        }
#endif

        // copy access stamp to new tuple from overwritten version
        // (no need to copy sucessor lsn (slsn))
        volatile_write(tuple->xstamp, prev->xstamp);
#endif

        // read prev's clsn first, in case it's a committing XID, the clsn's state
        // might change to ASI_LOG anytime
        fat_ptr prev_clsn = volatile_read(prev->clsn);
        if (prev_clsn.asi_type() == fat_ptr::ASI_XID and XID::from_ptr(prev_clsn) == t.xid) {
            // updating my own updates!
            // prev's prev: previous *committed* version
            volatile_write(obj->_next._ptr, prev_obj->_next._ptr);
            prev->mark_defunct();
#if defined(ENABLE_GC) && defined(REUSE_OBJECTS)
            t.op->put(t.epoch, prev_obj);
#endif
            ASSERT(obj->_next.offset() != (uintptr_t)prev_obj);
        }
        else {  // prev is committed (or precommitted but in post-commit now) head
            volatile_write(obj->_next, fat_ptr::make(prev_obj, 0));
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
            volatile_write(prev->sstamp, t.xc->owner.to_ptr());
#endif
        }

        t.write_set.emplace_back(tuple, fid, oid);
        ASSERT(tuple->clsn.asi_type() == fat_ptr::ASI_XID);
        ASSERT(oidmgr->oid_get_version(fid, oid, t.xc) == tuple);

#ifdef PHANTOM_PROT_TABLE_LOCK
        if (instant_lock)
            object_vector::unlock(l);
#endif

        INVARIANT(t.log);
        // the logmgr only assignspointers, instead of doing memcpy here,
        // unless the record is tooooo large.
        const size_t sz = v ? v->size(): 0;
        ASSERT((not sz and not v) or (sz and v and sz == v->size()));
        auto record_size = align_up(sz);
        auto size_code = encode_size_aligned(record_size);
        ASSERT(not ((uint64_t)v & ((uint64_t)0xf)));
        t.log->log_update(this->fid, oid, fat_ptr::make(v ? (void *)v->data() : NULL, size_code),
                          DEFAULT_ALIGNMENT_BITS, NULL);
        return rc_t{RC_TRUE};
    }
    else {  // somebody else acted faster than we did
#ifdef PHANTOM_PROT_TABLE_LOCK
        if (instant_lock)
            object_vector::unlock(l);
#endif
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
    const typename concurrent_btree::string_type &k, OID o, dbtuple *v,
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

#ifdef PHANTOM_PROT_TABLE_LOCK
    table_lock_t *l = this->underlying_btree.get_tuple_vector()->lock_ptr();
    if (std::find(t.table_locks.begin(), t.table_locks.end(), l) == t.table_locks.end()) {
        if (object_vector::lock(l, TABLE_LOCK_S))
            t.table_locks.push_back(l);
        else {
            callback.return_code = rc_t{RC_ABORT_PHANTOM};
            return;
        }
    }
    else {
        ASSERT((volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_S or
            (volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX);
    }
#endif

    txn_search_range_callback c(&t, &callback, &key_reader, &value_reader);

    varkey uppervk;
    if (upper_str)
        uppervk = varkey(upper_str);
    this->underlying_btree.search_range_call(
        varkey(lower_str), upper_str ? &uppervk : nullptr,
        c, this->fid, t.xc);
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

#ifdef PHANTOM_PROT_TABLE_LOCK
    table_lock_t *l = this->underlying_btree.get_tuple_vector()->lock_ptr();
    if (std::find(t.table_locks.begin(), t.table_locks.end(), l) == t.table_locks.end()) {
        if (object_vector::lock(l, TABLE_LOCK_S))
            t.table_locks.push_back(l);
        else {
            callback.return_code = rc_t{RC_ABORT_PHANTOM};
            return;
        }
    }
    else {
        ASSERT((volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_S or
               (volatile_read(*l) & TABLE_LOCK_MODE_MASK) == TABLE_LOCK_SIX);
    }
#endif

    txn_search_range_callback c(&t, &callback, &key_reader, &value_reader);

    varkey lowervk;
    if (lower_str)
        lowervk = varkey(lower_str);
    this->underlying_btree.rsearch_range_call(
        varkey(upper_str), lower_str ? &lowervk : nullptr,
        c, this->fid, t.xc);
}

