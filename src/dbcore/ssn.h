#pragma once
#include <unordered_map>
#include "xid.h"
#include "../tuple.h"

namespace TXN {

bool wait_for_commit_result(xid_context *xc);
void assign_reader_bitmap_entry();
void deassign_reader_bitmap_entry();    

inline bool ssn_check_exclusion(xid_context *xc) {
    if (xc->succ != INVALID_LSN and xc->pred >= xc->succ) printf("ssn exclusion failure\n");
    if (xc->succ != INVALID_LSN and xc->pred != INVALID_LSN)
        return xc->pred < xc->succ; // \eta - predecessor, \pi - sucessor
        // if predecessor >= sucessor, then predecessor might depend on sucessor => cycle
    return true;
}

class readers_registry {
public:
    typedef dbtuple::bitmap_t bitmap_t;  // _builtin_ctz needs it to be uint
    typedef dbtuple::readers_list readers_list;

private:
    // maps dbtuple* to readers_list
    std::unordered_map<uintptr_t, readers_list*> reg;
    bool reg_lock;

private:
    void acquire_reg_lock()
    {
        while (__sync_lock_test_and_set(&reg_lock, true));
    }

    void release_reg_lock()
    {
        __sync_synchronize();
        reg_lock = false;
    }

public:
    readers_registry() : reg_lock(false) {}
    int register_tx(uintptr_t tuple, XID xid);
    // return true if new entry was created
    bool register_tx(dbtuple *tup, XID xid);
    void deregister_tx(uintptr_t tuple, int pos);
    void deregister_tx(dbtuple *tup);
    XID* get_xid_list(uintptr_t tuple);
};

bool ssn_register_reader_tx(dbtuple *tup, XID xid);
void ssn_deregister_reader_tx(dbtuple *tup);

/* Return a bitmap with 1's representing active readers.
 */
static inline 
dbtuple::bitmap_t ssn_get_tuple_readers(dbtuple *tup) {
    auto mask = dbtuple::bitmap_t(1) << dbtuple::XIDS_PER_READER_KEY;
    return mask & ~volatile_read(tup->rl_bitmap);
}

extern readers_registry readers_reg;
};  // end of namespace
