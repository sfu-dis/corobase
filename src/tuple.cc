#include "tuple.h"
#include "txn.h"

using namespace std;
using namespace util;

event_avg_counter dbtuple::g_evt_avg_dbtuple_read_retries
  ("avg_dbtuple_read_retries");

event_counter dbtuple::g_evt_dbtuple_creates("dbtuple_creates");
event_counter dbtuple::g_evt_dbtuple_bytes_allocated("dbtuple_bytes_allocated");
event_counter dbtuple::g_evt_dbtuple_bytes_freed("dbtuple_bytes_freed");

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
