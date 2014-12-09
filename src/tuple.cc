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

