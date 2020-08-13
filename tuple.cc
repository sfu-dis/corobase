#include "txn.h"
#include "tuple.h"

namespace ermia {

#ifdef SSN
bool dbtuple::is_old(TXN::xid_context *visitor) {  // FOR READERS ONLY!
  return visitor->xct->is_read_mostly() &&
         age(visitor) >= config::ssn_read_opt_threshold;
}
#endif


// print sizeof(dbtuple) in compile time:
// https://stackoverflow.com/questions/20979565/how-can-i-print-the-result-of-sizeof-at-compile-time-in-c
void kaboom_print( void ) {
    char (*__kaboom)[sizeof(dbtuple)];
    printf( "%d", __kaboom );
}
#if defined(SSN)
#pragma message("Above is the size of dbtuple in SSN")
#elif defined(SSI)
#pragma message("Above is the size of dbtuple in SSI")
#else
#pragma message("Above is the size of dbtuple in SI")
#endif

}  // namespace ermia
