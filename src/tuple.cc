#include "tuple.h"
#include "txn.h"

using namespace std;
using namespace util;

dbtuple::~dbtuple()
{
  VERBOSE(cerr << "dbtuple: " << hexify(intptr_t(this)) << " is being deleted" << endl);
}

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
bool
dbtuple::is_old(xid_context *xc)
{
  return xc->xct->is_read_mostly() && age(xc) >= sysconf::ssn_read_opt_threshold;
}

#endif
