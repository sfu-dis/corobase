#include "tuple.h"
#include "txn.h"

using namespace std;
using namespace util;

event_avg_counter dbtuple::g_evt_avg_dbtuple_stable_version_spins
  ("avg_dbtuple_stable_version_spins");
event_avg_counter dbtuple::g_evt_avg_dbtuple_lock_acquire_spins
  ("avg_dbtuple_lock_acquire_spins");
event_avg_counter dbtuple::g_evt_avg_dbtuple_read_retries
  ("avg_dbtuple_read_retries");

event_counter dbtuple::g_evt_dbtuple_creates("dbtuple_creates");
event_counter dbtuple::g_evt_dbtuple_bytes_allocated("dbtuple_bytes_allocated");
event_counter dbtuple::g_evt_dbtuple_bytes_freed("dbtuple_bytes_freed");
event_counter dbtuple::g_evt_dbtuple_spills("dbtuple_spills");
event_counter dbtuple::g_evt_dbtuple_inplace_buf_insufficient("dbtuple_inplace_buf_insufficient");
event_counter dbtuple::g_evt_dbtuple_inplace_buf_insufficient_on_spill("dbtuple_inplace_buf_insufficient_on_spill");

event_avg_counter dbtuple::g_evt_avg_record_spill_len("avg_record_spill_len");
static event_avg_counter evt_avg_dbtuple_chain_length("avg_dbtuple_chain_len");

dbtuple::~dbtuple()
{
  CheckMagic();
  VERBOSE(cerr << "dbtuple: " << hexify(intptr_t(this)) << " is being deleted" << endl);
  g_evt_dbtuple_bytes_freed += (alloc_size + sizeof(dbtuple));
}

void
dbtuple::print(ostream &o, unsigned len) const
{
  o << "dbtuple:" << endl
#ifdef TUPLE_CHECK_KEY
    << endl << "  key=" << hexify(key)
    << endl << "  tree=" << tree
#endif
    << endl;
}

ostream &
operator<<(ostream &o, const dbtuple &t)
{
  t.print(o, 1);
  return o;
}
