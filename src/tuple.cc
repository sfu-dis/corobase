#include "txn.h"

#ifdef USE_PARALLEL_SSN
bool dbtuple::is_old(xid_context *visitor) {    // FOR READERS ONLY!
  return visitor->xct->is_read_mostly() && age(visitor) >= sysconf::ssn_read_opt_threshold;
}
#endif
