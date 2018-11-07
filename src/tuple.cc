#include "txn.h"
#include "tuple.h"

namespace ermia {

#ifdef SSN
bool dbtuple::is_old(TXN::xid_context *visitor) {  // FOR READERS ONLY!
  return visitor->xct->is_read_mostly() &&
         age(visitor) >= config::ssn_read_opt_threshold;
}
#endif
}  // namespace ermia
