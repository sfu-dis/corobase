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

    struct readers_registry_key {
        void *table_tree;
        oid_type oid;
        LSN clsn;   // read version's clsn
        readers_registry_key(void *t, oid_type o, LSN l) :
            table_tree(t), oid(o), clsn(l) {}
    };

private:
    struct rr_hash {
        size_t operator()(const readers_registry_key &k) const {
            return (uintptr_t)k.table_tree ^ k.oid * k.clsn._val;
        }
    };

    struct rr_equal {
        bool operator()(const readers_registry_key &k1,
                        const readers_registry_key &k2) const {
            return k1.table_tree == k2.table_tree and
                   k1.oid == k2.oid and k1.clsn == k2.clsn;
        }
    };

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

    std::unordered_map<readers_registry_key, readers_list, rr_hash, rr_equal> reg;
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
/*
    readers_registry::iterator find_reg(readers_registry_key &k)
    {
        acquire_reg_lock();
        readers_registry::iterator it = reg.find(k);
        release_reg_lock();
        return it;
    }
*/
public:
    readers_registry() : reg_lock(false) {}
    int register_tx(void *table_tree, oid_type oid, LSN clsn, XID xid);
    void deregister_tx(void *table_tree, oid_type oid, LSN clsn, int pos);
    size_t count_readers(void *table_tree, oid_type oid, LSN clsn);
    XID* get_xid_list(void *table_tree, oid_type oid, LSN clsn);
};

extern readers_registry readers_reg;
};  // end of namespace
