#include "../macros.h"
#ifdef USE_PARALLEL_SSN
#pragma once
#include <unordered_map>
#include "xid.h"
#include "../tuple.h"

namespace TXN {

bool wait_for_commit_result(xid_context *xc);
void assign_reader_bitmap_entry();
void deassign_reader_bitmap_entry();    

inline bool ssn_check_exclusion(xid_context *xc) {
    if (xc->sstamp and xc->pstamp >= xc->sstamp) printf("ssn exclusion failure\n");
    if (xc->sstamp and xc->pstamp)
        return xc->pstamp < xc->sstamp; // \eta - predecessor, \pi - sucessor
        // if predecessor >= sucessor, then predecessor might depend on sucessor => cycle
    return true;
}

struct readers_list {
public:
    typedef dbtuple::rl_bitmap_t bitmap_t;
    enum { XIDS_PER_READER_KEY=24 };

    // FIXME: on crossfire we basically won't have more than 24 concurrent
    // transactions running (not to mention all as readers of a single
    // version). If this doesn't hold (on some other machine e.g.), we need
    // to consider how to handle overflows (one way is to consolidate all
    // txs to one bit and let late comers to compare with this).
    XID xids[XIDS_PER_READER_KEY];

    readers_list() {
        memset(xids, '\0', sizeof(XID) * XIDS_PER_READER_KEY);
    }
};

bool ssn_register_reader_tx(dbtuple *tup, XID xid);
void ssn_deregister_reader_tx(dbtuple *tup);
void ssn_register_tx(XID xid);
void ssn_deregister_tx(XID xid);
/* Return a bitmap with 1's representing active readers.
 */
static inline 
readers_list::bitmap_t ssn_get_tuple_readers(dbtuple *tup) {
    return volatile_read(tup->rl_bitmap);
}

extern readers_list rlist;
};  // end of namespace
#endif
