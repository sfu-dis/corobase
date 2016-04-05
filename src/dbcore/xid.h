// -*- mode:c++ -*-
#pragma once
#include <atomic>
#include <mutex>
#include <vector>

#include "epoch.h"
#include "sm-common.h"
#include "sm-config.h"
#include "../macros.h"

class transaction;
namespace TXN {

enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_COMMITTING, TXN_CMMTD, TXN_ABRTD, TXN_INVALID };

struct xid_context {
    epoch_mgr::epoch_num begin_epoch;  // tx start epoch, not owner.local()
    XID owner;
    LSN begin;
    LSN end;
#ifdef USE_PARALLEL_SSN
    uint64_t pstamp; // youngest predecessor (\eta)
    std::atomic<uint64_t> sstamp; // oldest successor (\pi)
    transaction *xct;
    bool set_sstamp(uint64_t s);
#endif
#ifdef USE_PARALLEL_SSI
    uint64_t ct3;   // smallest commit stamp of T3 in the dangerous structure
    uint64_t last_safesnap;
    transaction *xct;
#endif
    txn_state state;

#ifdef USE_PARALLEL_SSN
const static uint64_t sstamp_final_mark = 1UL << 63;
inline void finalize_sstamp() {
    std::atomic_fetch_or(&sstamp, sstamp_final_mark);
}
inline void set_pstamp(uint64_t p) {
    volatile_write(pstamp, std::max(pstamp, p));
}
#endif
};

/* Request a new XID and an associated context. The former is globally
   unique and the latter is distinct from any other transaction whose
   lifetime overlaps with this one.
 */
XID xid_alloc();

/* Release an XID and its associated context. The XID will no longer
   be associated with any context after this call returns.
 */
void xid_free(XID x);

/* Return the context associated with the givne XID.

   throw illegal_argument if [xid] is not currently associated with a context. 
 */
xid_context *xid_get_context(XID x);

#if defined(USE_PARALLEL_SSN) or defined(USE_PARALLEL_SSI)
txn_state spin_for_cstamp(XID xid, xid_context *xc);
#endif
};  // end of namespace
