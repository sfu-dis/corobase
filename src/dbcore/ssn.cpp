#include "ssn.h"

namespace TXN {

readers_registry readers_reg;

#ifdef USE_SERIAL_SSN
std::mutex ssn_commit_mutex;
#endif

bool
wait_for_commit_result(xid_context *xc) {
    while (volatile_read(xc->state) == TXN_COMMITTING);
    return volatile_read(xc->state) == TXN_CMMTD;
}

// register a tx to a version's readers list
// returns the position of the tx in the xids array
int
readers_registry::register_tx(uintptr_t tuple, XID xid)
{
    acquire_reg_lock();
    if (reg.find(tuple) == reg.end()) {
        reg[tuple] = ((dbtuple *)tuple)->rlist = new readers_list(xid);
        release_reg_lock();
        return 0;
        //return register_tx(((dbtuple *)tuple)->rlist, xid);
    }
    release_reg_lock();
    return register_tx(((dbtuple *)tuple)->rlist, xid);
}
#endif

bool
ssn_register_reader_tx(dbtuple *t, XID xid)
{
    ASSERT(t->rlist);
    bitmap_t old_bitmap = volatile_read(t->rl_bitmap);
    if (old_bitmap & tls_bitmap_entry)
        return false;
    
    int xid_pos = __builtin_ctz(tls_bitmap_entry);
    __sync_fetch_and_or(&t->rl_bitmap, tls_bitmap_entry);
    ASSERT(not t->rlist->xids[xid_pos]._val);
    volatile_write(t->rlist->xids[xid_pos]._val, xid._val);
    return true;
}

#if 0
// deregister a tx from a version's readers list
// @pos: the index in to xids array returned by register_tx
void
readers_registry::deregister_tx(uintptr_t tuple, int pos)
{
    acquire_reg_lock();
    readers_list *rl = reg[tuple];
    release_reg_lock();
    return deregister_tx(rl, pos);
}
#endif

void
ssn_deregister_reader_tx(dbtuple *t)
{
    // FIXME: xid=0 means invalid xid?
    ASSERT(tls_bitmap_entry);
    int pos = __builtin_ctz(tls_bitmap_entry);
    ASSERT(pos >= 0 and t->rlist->xids[pos]._val);
    ASSERT(t->rl_bitmap & tls_bitmap_entry);
    volatile_write(t->rlist->xids[pos]._val, 0);
    __sync_fetch_and_xor(&t->rl_bitmap, tls_bitmap_entry);
}

XID*
readers_registry::get_xid_list(uintptr_t tuple)
{
    if (reg[tuple])
        return reg[tuple]->xids;
    return NULL;
}

};  // end of namespace
