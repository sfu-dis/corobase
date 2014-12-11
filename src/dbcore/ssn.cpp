#include "ssn.h"
#include "../tuple.h"
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

int
readers_registry::register_tx(readers_list *rl, XID xid)
{
    ASSERT(rl);
    int pos = -1;
    bitmap_t curr_bitmap = -1, new_bitmap = -1;
    do {
        curr_bitmap = volatile_read(rl->bitmap);
        ALWAYS_ASSERT(curr_bitmap);
        pos = __builtin_ctz(curr_bitmap) - 1;
        ALWAYS_ASSERT(pos >= 0);
        new_bitmap = curr_bitmap | (bitmap_t(1) << pos);
    }
    while(not __sync_bool_compare_and_swap(&rl->bitmap, curr_bitmap, new_bitmap));
    int xid_pos = XIDS_PER_READER_KEY - (pos + 1);
    ASSERT(not rl->xids[xid_pos]._val);
    volatile_write(rl->xids[xid_pos]._val, xid._val);
    return xid_pos;
}

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

void
readers_registry::deregister_tx(readers_list *rl, int pos)
{
    // FIXME: xid=0 means invalid xid?
    ASSERT(pos >= 0 and rl->xids[pos]._val);
    volatile_write(rl->xids[pos]._val, 0);
    __sync_fetch_and_and(&rl->bitmap, ~(bitmap_t(1) << (XIDS_PER_READER_KEY - pos - 1)));
}

XID*
readers_registry::get_xid_list(uintptr_t tuple)
{
    if (reg[tuple])
        return reg[tuple]->xids;
    return NULL;
}

};  // end of namespace
