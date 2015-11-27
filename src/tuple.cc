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
uint64_t
dbtuple::age(xid_context *visitor)
{
  uint64_t end = 0;
  XID owner = volatile_read(visitor->owner);

retry:
  fat_ptr cstamp = volatile_read(get_object()->_clsn);

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
  return xc->xct->is_read_mostly() && age(xc) >= sysconf::ssn_read_opt_threshold;
}

bool
dbtuple::set_persistent_reader()
{
    uint64_t pr = 0;
    do {
        pr = volatile_read(preader);
        if (pr >> 7)   // some updater already locked it
            return false;
    }
    while (volatile_read(preader) != PERSISTENT_READER_MARK and
           not __sync_bool_compare_and_swap(&preader, pr, PERSISTENT_READER_MARK));
    return true;
}

bool
dbtuple::has_persistent_reader()
{
    return volatile_read(preader) & PERSISTENT_READER_MARK;
}

// XXX: for the writer who's updating this tuple only
void
dbtuple::lockout_readers()
{
    if (not (volatile_read(preader) >> 7))
        __sync_fetch_and_xor(&preader, uint8_t{1} << 7);
    ASSERT(volatile_read(preader) >> 7);
}

// XXX: for the writer who's updating this tuple only
void
dbtuple::welcome_readers()
{
    if (volatile_read(preader) >> 7)
        __sync_fetch_and_xor(&preader, uint64_t{1} << 7);
    ASSERT(not (volatile_read(preader) >> 7));
}

#endif
