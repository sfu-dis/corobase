#include <string>
#include "sm-log-impl.h"

using namespace RCU;

namespace {
    /* No point allocating these individually or repeatedly---they're
       thread-private with a reasonably small maximum size (~10kB).

       WARNING: this assumes that transactions are pinned to their
       worker thread at least until pre-commit. If we ever implement
       DORA we'll have to be more clever, because transactions would
       change threads frequently, but for now it works great.
     */
    static __thread log_request tls_log_requests[sm_log_recover_mgr::MAX_BLOCK_RECORDS];

    /* Same goes for the sm_tx_log_impl object we give the caller, for that matter */
    static __thread char LOG_ALIGN tls_log_space[sizeof(sm_tx_log_impl)];
    static __thread bool tls_log_space_used = false;

    static sm_tx_log_impl*
    get_log_impl(sm_tx_log *x)
    {
        DIE_IF(x != (void*) tls_log_space,
               "sm_tx_log object can only be safely used by the thread that created it");
        DIE_IF(not tls_log_space_used, "Attempt to access unallocated memory");
        return get_impl(x);
    }
}

void
sm_tx_log::log_insert_index(FID f, OID o, fat_ptr ptr, int abits, fat_ptr *pdest) {
    get_log_impl(this)->add_payload_request(LOG_INSERT_INDEX, f, o, ptr, abits, pdest);
}

void
sm_tx_log::log_insert(FID f, OID o, fat_ptr ptr, int abits, fat_ptr *pdest) {
    get_log_impl(this)->add_payload_request(LOG_INSERT, f, o, ptr, abits, pdest);
}

void
sm_tx_log::log_update(FID f, OID o, fat_ptr ptr, int abits, fat_ptr *pdest) {
    get_log_impl(this)->add_payload_request(LOG_UPDATE, f, o, ptr, abits, pdest);
}

void
sm_tx_log::log_update_key(FID f, OID o, fat_ptr ptr, int abits) {
    get_log_impl(this)->add_payload_request(LOG_UPDATE_KEY, f, o, ptr, abits, nullptr);
}

void
sm_tx_log::log_fid(FID f, const std::string &name)
{
    auto size = align_up(name.length() + 1);
    auto size_code = encode_size_aligned(size);
    char *buf = (char *)malloc(size);
    memset(buf, '\0', size);
    memcpy(buf, (char *)name.c_str(), name.length());
    ASSERT(buf[name.length()] == '\0');
    // only use the logrec's fid field, payload is name
    get_log_impl(this)->add_payload_request(LOG_FID, f, 0,
                            fat_ptr::make(buf, size_code),
                            DEFAULT_ALIGNMENT_BITS, NULL);
}

static
void
format_extra_ptr(log_request &req)
{
    auto p = req.extra_ptr = req.payload_ptr;
    req.payload_ptr = fat_ptr::make(&req.extra_ptr, p.size_code());
    req.payload_size = align_up(sizeof(req.extra_ptr));
}

static
log_request
make_log_request(log_record_type type, FID f, OID o, fat_ptr ptr, int abits) {
    log_request req;
    req.type = type;
    req.fid = f;
    req.oid = o;
    req.pdest = NULL;
    req.size_align_bits = abits;
    req.payload_ptr = ptr;
    req.payload_size = decode_size_aligned(ptr.size_code(), abits);
    return req;
}

void
sm_tx_log::log_relocate(FID f, OID o, fat_ptr ptr, int abits) {
    log_request req = make_log_request(LOG_RELOCATE, f, o, ptr, abits);
    format_extra_ptr(req);
    get_log_impl(this)->add_request(req);
}

void
sm_tx_log::log_delete(FID f, OID o) {
    log_request req = make_log_request(LOG_DELETE, f, o, NULL_PTR, 0);
    get_log_impl(this)->add_request(req);
}

LSN
sm_tx_log::get_clsn() {
    /* The caller already has a published CLSN, so if this tx still
       appears to not have a commit block it cannot possibly have an
       earlier CLSN. INVALID_LSN is an appropriate
       response. Otherwise, we have to race to finish installing a
       commit block.

       WARNING: Both this object and the commit block it points to
       might have been freed by the time a thread calls this function,
       so the caller must use an appropriate RCU transaction to avoid
       use-after-free bugs.
     */
    auto *impl = get_log_impl(this);
    if (auto *a = volatile_read(impl->_commit_block)) {
        a = impl->_install_commit_block(a);
        return a->block->next_lsn();
    }
    
    return INVALID_LSN;
}

LSN
sm_tx_log::pre_commit() {
    auto *impl = get_log_impl(this);
    if (not impl->_commit_block)
        impl->enter_precommit();
    return impl->_commit_block->block->next_lsn();
}

LSN
sm_tx_log::commit(LSN *pdest) {
    // make sure we acquired a commit block
    LSN clsn = pre_commit();

    auto *impl = get_log_impl(this);
    // now copy log record data
    impl->_populate_block(impl->_commit_block->block);

    if (pdest)
        *pdest = impl->_commit_block->block->lsn;
    
    impl->_log->_lm.release(impl->_commit_block);
    tls_log_space_used = false;
    return clsn;
}

void
sm_tx_log::discard() {
    auto *impl = get_log_impl(this);
    if (impl->_commit_block)
        impl->_log->_lm.discard(impl->_commit_block);
    tls_log_space_used = false;
}


void *
sm_tx_log_impl::alloc_storage() {
    DIE_IF(tls_log_space_used, "Only one transaction per worker thread is allowed");
    tls_log_space_used = true;
    return tls_log_space;
}
    
void
sm_tx_log_impl::add_payload_request(log_record_type type, FID f, OID o,
                                    fat_ptr p, int abits, fat_ptr *pdest)
{

    log_request req = make_log_request(type, f, o, p, abits);

    // 4GB+ for a single object is just too big. Punt.
    size_t psize = req.payload_size;
    THROW_IF(psize > UINT32_MAX, illegal_argument,
             "Log record payload must be less than 4GB (%zd requested, %zd bytes over limit)",
             psize, psize - UINT32_MAX);

    /* If the record's payload is too large (e.g. 8 of them would
       overflow the block), then embed the payload directly in the log
       (disguised as a skip record) and link the request to it.
     */
    if (sm_log_recover_mgr::MAX_BLOCK_SIZE < log_block::wrapped_size(8, 8*psize)) {
        log_allocation *a = _log->_lm.allocate(0, psize);
        DEFER_UNLESS(it_worked, _log->_lm.discard(a));
        
        log_block *b = a->block;
        ASSERT(b->nrec == 0);
        ASSERT(b->lsn != INVALID_LSN);
        ASSERT(b->records->type == LOG_SKIP);
        
        b->records->type = LOG_FAT_SKIP;
        b->records->size_code = p.size_code();
        b->records->size_align_bits = abits;

        uint32_t csum = b->body_checksum();
        b->checksum = adler32_memcpy(b->payload_begin(), p, psize, csum);

        // update the request to point to the external record
        req.type = (log_record_type) (req.type | LOG_FLAG_IS_EXT);
        p = req.payload_ptr = _log->lsn2ptr(b->payload_lsn(0), false);
        format_extra_ptr(req);
        if (pdest) {
            *pdest = p;
            pdest = NULL;
        }
        
        it_worked = true;
        _log->_lm.release(a);
    }

    
    // add to list
    req.pdest = pdest;
    add_request(req);
}

void sm_tx_log_impl::add_request(log_request const &req) {
    ASSERT (not _commit_block);
    auto new_nreq = _nreq+1;
    bool too_many = (new_nreq > sm_log_recover_mgr::MAX_BLOCK_RECORDS);
    auto new_payload_bytes = _payload_bytes + align_up(req.payload_size);
    ASSERT(is_aligned(new_payload_bytes));
    
    auto bsize = log_block::wrapped_size(new_nreq, new_payload_bytes);
    bool too_big = bsize > sm_log_recover_mgr::MAX_BLOCK_SIZE;
    if (too_many or too_big) {
        spill_overflow();
        return add_request(req);
    }

    tls_log_requests[_nreq] = req;
    _nreq = new_nreq;
    _payload_bytes = new_payload_bytes;
}

void
sm_tx_log_impl::_populate_block(log_block *b)
{
    size_t i = 0;
    uint32_t payload_end = 0;
    uint32_t csum_payload = ADLER32_CSUM_INIT;

    // link to previous overflow?
    if (_prev_overflow != INVALID_LSN) {
        log_record *r = &b->records[i++];
        r->type = LOG_OVERFLOW;
        r->size_code = INVALID_SIZE_CODE;
        r->size_align_bits = 0;
        r->payload_end = 0;
        r->next_lsn = _prev_overflow;
    }
    
    // process normal log records
    while (i < _nreq) {
        log_request *it = &tls_log_requests[i];
        DEFER(++i);
        
        log_record *r = &b->records[i];
        r->type = it->type;
        r->fid = it->fid;
        r->oid = it->oid;
        if (it->type & LOG_FLAG_HAS_PAYLOAD) {
            // fill out payload-related bits before calling payload_lsn()
            r->size_code = it->payload_ptr.size_code();
            r->size_align_bits = it->size_align_bits;
            
            // copy and checksum the payload
            if (it->pdest) {
                bool is_ext = it->type & LOG_FLAG_IS_EXT;
                *it->pdest = _log->lsn2ptr(b->payload_lsn(i), is_ext);
            }
            
            char *dest = b->payload_begin() + payload_end;
            csum_payload = adler32_memcpy(dest, it->payload_ptr, it->payload_size, csum_payload);
            payload_end += it->payload_size;
        }
        else {
            r->size_code = INVALID_SIZE_CODE;
            r->size_align_bits = -1;
        }
        r->payload_end = payload_end;
    }
    ASSERT (i == b->nrec);
    ASSERT (b->payload_end() == b->payload_begin() + payload_end);

    // finalize the checksum
    uint32_t csum = b->body_checksum();
    b->checksum = adler32_merge(csum, csum_payload, payload_end);
}

/* Transactions assign this value to their commit block as a signal of
   their intent to enter pre-commit. It should never be accessed or
   freed.
 */
static constexpr
log_allocation * const ENTERING_PRECOMMIT = (log_allocation*) 0x1;

/* Sometimes the log block overflows before a transaction completes
   and we have to spill and overflow block to the log.
 */
void sm_tx_log_impl::spill_overflow() {
    size_t inner_nreq = _nreq;
    size_t outer_nreq = 0;
    size_t pbytes = log_block::size(inner_nreq, _payload_bytes);
    
    log_allocation *a = _log->_lm.allocate(outer_nreq, pbytes);
    DEFER_UNLESS(it_worked, _log->_lm.discard(a));

    log_block *b = a->block;
    ASSERT(b->nrec == outer_nreq);
    ASSERT(b->lsn != INVALID_LSN);

    b->records->type = LOG_FAT_SKIP;
        
    auto *inner = (log_block*) b->payload(0);
    inner->nrec = inner_nreq;
    inner->lsn = b->payload_lsn(0);
    fill_skip_record(&inner->records[inner_nreq], INVALID_LSN, _payload_bytes, false);
    _populate_block(inner);
        
    uint32_t csum = b->body_checksum();
    b->checksum = adler32_merge(csum, inner->checksum, pbytes);
    _nreq = 1; // for the overflow LSN
    _prev_overflow = inner->lsn;
    _payload_bytes = 0;
        
    it_worked = true;
    _log->_lm.release(a);
}

/* This function is called by threads racing to install a commit
   block. The transaction calls it as part of the pre-commit protocol,
   and other threads call it to learn whether this transaction's CLSN
   precedes theirs or not (and if so, its value). It should never be
   called with a NULL _commit_block: the owner thread should signal
   ENTERING_PRECOMMIT, and other threads should only call this if they
   see a non-NULL value (which might be the signal, or might be an
   already-installed commit block).
 */
log_allocation *sm_tx_log_impl::_install_commit_block(log_allocation *a) {
    ASSERT(a);
    if (a == ENTERING_PRECOMMIT) {
        a = _log->_lm.allocate(_nreq, _payload_bytes);
        auto *tmp = __sync_val_compare_and_swap(&_commit_block, ENTERING_PRECOMMIT, a);
        if (tmp != ENTERING_PRECOMMIT) {
            _log->_lm.discard(a);
            a = tmp;
        }
        
        ASSERT(a != ENTERING_PRECOMMIT);
        ASSERT(a->block->nrec == _nreq);
        ASSERT(a->block->lsn != INVALID_LSN);
    }

    return a;
}

void sm_tx_log_impl::enter_precommit() {
    _commit_block = ENTERING_PRECOMMIT;
    auto *a = _install_commit_block(ENTERING_PRECOMMIT);
    DEFER_UNLESS(it_worked, _log->_lm.discard(a));

    // tzwang: avoid copying data here, commit() will do it
    //_populate_block(a->block);

    // leave these in place for late-comers to the race
    //_nreq = 0;
    //_prev_overflow = INVALID_LSN;
    //_payload_bytes = 0;
        
    it_worked = true;

    // do this late to give races plenty of time to show up
    ASSERT(_commit_block == a);
}
