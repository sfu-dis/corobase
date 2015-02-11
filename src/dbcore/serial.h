#include "../macros.h"
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
#pragma once
#include <unordered_map>
#include "xid.h"
#include "../tuple.h"

namespace TXN {

extern uint64_t __thread tls_serial_abort_count;
extern uint64_t __thread tls_rw_conflict_abort_count;
extern int64_t OLD_VERSION_THRESHOLD;

void assign_reader_bitmap_entry();
void deassign_reader_bitmap_entry();    

#ifdef USE_PARALLEL_SSN
// returns true if serializable, false means exclusion window violation
inline bool ssn_check_exclusion(xid_context *xc) {
#if CHECK_INVARIANTS
    if (xc->pstamp >= xc->sstamp) printf("ssn exclusion failure\n");
#endif
    // if predecessor >= sucessor, then predecessor might depend on sucessor => cycle
    // note xc->sstamp is initialized to ~0, xc->pstamp's init value is 0,
    // so don't return xc->pstamp < xc->sstamp...
    if (xc->pstamp >= xc->sstamp) {
        tls_serial_abort_count++;
        return false;
    }
    return true;
    //return not (xc->pstamp >= xc->sstamp); // \eta - predecessor, \pi - sucessor
}
#endif

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

bool serial_register_reader_tx(dbtuple *tup, XID xid);
void serial_deregister_reader_tx(dbtuple *tup);
void serial_register_tx(XID xid);
void serial_deregister_tx(XID xid);
void summarize_serial_aborts();

readers_list::bitmap_t serial_get_tuple_readers(dbtuple *tup, bool exclude_self = false);

extern readers_list rlist;
};  // end of namespace
#endif
