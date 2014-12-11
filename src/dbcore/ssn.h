#pragma once
#include <unordered_map>
#include "xid.h"
#include "../object.h"

#define XIDS_PER_READER_KEY 24

namespace TXN {

bool wait_for_commit_result(xid_context *xc);

inline bool ssn_check_exclusion(xid_context *xc) {
    if (xc->succ != INVALID_LSN and xc->pred >= xc->succ) printf("ssn exclusion failure\n");
    if (xc->succ != INVALID_LSN and xc->pred != INVALID_LSN)
        return xc->pred < xc->succ; // \eta - predecessor, \pi - sucessor
        // if predecessor >= sucessor, then predecessor might depend on sucessor => cycle
    return true;
}

class readers_registry {
public:
    typedef unsigned int bitmap_t;  // _builtin_ctz needs it to be uint
    struct readers_list {
        // FIXME: on crossfire we basically won't have more than 24 concurrent
        // transactions running (not to mention all as readers of a single
        // version). If this doesn't hold (on some other machine e.g.), we need
        // to consider how to handle overflows (one way is to consolidate all
        // txs to one bit and let late comers to compare with this).
        XID xids[XIDS_PER_READER_KEY];
        // note: bitmap starts from msb, but we still use xids array from 0
        bitmap_t bitmap;

        readers_list(XID init_xid) {
            memset(xids, '\0', sizeof(XID) * XIDS_PER_READER_KEY);
            xids[0] = init_xid;
            bitmap = bitmap_t(3) << XIDS_PER_READER_KEY;
        }
        readers_list() {
            memset(xids, '\0', sizeof(XID) * XIDS_PER_READER_KEY);
            // mark our own msb so ctz can work correctly (bitmap=0 is undefind)
            bitmap = bitmap_t(1) << XIDS_PER_READER_KEY;
        }
        //FIXME: add_xid(XID xid);
    };

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
    int register_tx(readers_list *rl, XID xid);
    void deregister_tx(uintptr_t tuple, int pos);
    void deregister_tx(readers_list *rl, int pos);
    XID* get_xid_list(uintptr_t tuple);
};

extern readers_registry readers_reg;
};  // end of namespace
