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
/* return the tuple's age based on a safe_lsn provided by the calling tx.
 * safe_lsn usually = the calling tx's begin offset.
 *
 * and we need to check the type of clsn because a "committed" tx might
 * change the clsn to ASI_LOG type after changing state.
 */
int64_t
dbtuple::age(xid_context *visitor)
{
  uint64_t end = 0;
  XID owner = volatile_read(visitor->owner);

retry:
  fat_ptr cstamp = volatile_read(clsn);

  if (cstamp.asi_type() == fat_ptr::ASI_XID) {
    XID xid = XID::from_ptr(cstamp);
    if (xid == owner)   // my own update
      return 0;
    xid_context *xc = xid_get_context(xid);
    end = volatile_read(xc->end).offset();
    if (not xc or xc->owner != xid)
      goto retry;
  }
  else {
    end = cstamp.offset();
  }

  // the caller must alive...
  return volatile_read(visitor->begin).offset() - end;
}

bool
dbtuple::is_old(xid_context *xc)
{
  return age(xc) >= OLD_VERSION_THRESHOLD;
}
#endif
