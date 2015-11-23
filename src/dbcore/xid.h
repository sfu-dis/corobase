// -*- mode:c++ -*-
#pragma once

#include <mutex>
#include <vector>
#include "sm-common.h"
#include "../macros.h"
//#include "../tuple.h"

class dbtuple;
class transaction;
namespace TXN {

enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_COMMITTING, TXN_CMMTD, TXN_ABRTD, TXN_INVALID };

struct xid_context {
    XID owner;
    LSN begin;
    LSN end;
#ifdef USE_PARALLEL_SSN
    uint64_t pstamp; // youngest predecessor (\eta)
    uint64_t sstamp; // oldest successor (\pi)
    bool should_abort;
    transaction *xct;
#endif
#ifdef USE_PARALLEL_SSI
    uint64_t ct3;   // smallest commit stamp of T3 in the dangerous structure
    uint64_t last_safesnap;
    transaction *xct;
#endif
    txn_state state;

#ifdef USE_PARALLEL_SSN
inline void set_sstamp(uint64_t s) {
    volatile_write(sstamp, std::min(s, sstamp));
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
