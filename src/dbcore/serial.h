#include "../macros.h"
#pragma once
#include <unordered_map>
#include "../tuple.h"
#include "sm-rc.h"
#include "xid.h"

namespace TXN {
// XXX(tzwang): enabling safesnap for tpcc basically halves the performance.
// perf says 30%+ of cycles are on oid_get_version, which makes me suspect
// it's because enabling safesnap makes the reader has to go deeper in the
// version chains to find the desired version. So perhaps don't enable this
// for update-intensive workloads, like tpcc. TPC-E to test and verify.
extern int enable_safesnap;
};

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
namespace TXN {

extern uint64_t OLD_VERSION_THRESHOLD;

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
        return false;
    }
    return true;
    //return not (xc->pstamp >= xc->sstamp); // \eta - predecessor, \pi - sucessor
}
#endif

struct readers_list {
public:
    typedef dbtuple::rl_bitmap_t bitmap_t;
    enum { XIDS_PER_READER_KEY=sizeof(bitmap_t)*8 };

    // FIXME: on crossfire we basically won't have more than 24 concurrent
    // transactions running (not to mention all as readers of a single
    // version). If this doesn't hold (on some other machine e.g.), we need
    // to consider how to handle overflows (one way is to consolidate all
    // txs to one bit and let late comers to compare with this).
    XID xids[XIDS_PER_READER_KEY];
    LSN last_commit_lsns[XIDS_PER_READER_KEY];

    readers_list() {
        memset(xids, '\0', sizeof(XID) * XIDS_PER_READER_KEY);
        memset(last_commit_lsns, '\0', sizeof(LSN) * XIDS_PER_READER_KEY);
    }
};

bool serial_request_abort(xid_context *xc);
uint64_t serial_get_last_cstamp(int xid_idx);
void serial_stamp_last_committed_lsn(LSN lsn);
bool serial_register_reader_tx(dbtuple *tup, XID xid);
void serial_deregister_reader_tx(dbtuple *tup);
void serial_register_tx(XID xid);
void serial_deregister_tx(XID xid);
void summarize_serial_aborts();

readers_list::bitmap_t serial_get_tuple_readers(dbtuple *tup, bool exclude_self = false);

extern readers_list rlist;
};  // end of namespace
#endif
