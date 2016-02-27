#include "../macros.h"
#pragma once
#include <unordered_map>
#include "../tuple.h"
#include "sm-rc.h"
#include "xid.h"

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
namespace TXN {
void assign_reader_bitmap_entry();
void deassign_reader_bitmap_entry();    

#ifdef USE_PARALLEL_SSN
// returns true if serializable, false means exclusion window violation
inline bool ssn_check_exclusion(xid_context *xc) {
    auto ss = xc->sstamp.load(std::memory_order_acquire) & (~xid_context::sstamp_final_mark);
#if CHECK_INVARIANTS
    if (xc->pstamp >= ss) printf("ssn exclusion failure\n");
#endif
    // if predecessor >= sucessor, then predecessor might depend on sucessor => cycle
    // note xc->sstamp is initialized to ~0, xc->pstamp's init value is 0,
    // so don't return xc->pstamp < xc->sstamp...
    if (xc->pstamp >= ss) {
        return false;
    }
    return true;
}
#endif

struct readers_list {
public:
    typedef dbtuple::rl_bitmap_t bitmap_t;
    enum { XIDS_PER_READER_KEY=sizeof(bitmap_t)*8 };

    XID xids[XIDS_PER_READER_KEY];
    LSN last_read_mostly_clsns[XIDS_PER_READER_KEY];

    readers_list() {
        memset(xids, '\0', sizeof(XID) * XIDS_PER_READER_KEY);
        memset(last_read_mostly_clsns, '\0', sizeof(LSN) * XIDS_PER_READER_KEY);
    }
};

bool ssn_ropt_set_reader_sstamp(xid_context *xc, uint64_t sstamp);
uint64_t serial_get_last_read_mostly_cstamp(int xid_idx);
void serial_stamp_last_committed_lsn(LSN lsn);
bool serial_register_reader_tx(dbtuple *tup, XID xid);
void serial_deregister_reader_tx(dbtuple *tup);
void serial_register_tx(XID xid);
void serial_deregister_tx(XID xid);
readers_list::bitmap_t serial_get_tuple_readers(dbtuple *tup, bool exclude_self = false);

extern readers_list rlist;
};  // end of namespace
#endif
