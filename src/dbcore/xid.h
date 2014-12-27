// -*- mode:c++ -*-
#pragma once

#include <mutex>
#include "sm-common.h"
#include "../macros.h"

namespace TXN {

enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_COMMITTING, TXN_CMMTD, TXN_ABRTD };

struct xid_context {
    XID owner;
    LSN begin;
    LSN end;
#ifdef USE_PARALLEL_SSN
    uint64_t pstamp; // youngest predecessor (\eta)
    uint64_t sstamp; // oldest successor (\pi)
#endif
    txn_state state;
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

};  // end of namespace
