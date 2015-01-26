#include <iostream>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <limits.h>
#include <numa.h>

#include "txn_proto2_impl.h"
#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;

aligned_padded_elem<transaction_proto2_static::hackstruct>
  transaction_proto2_static::g_hack;
aligned_padded_elem<transaction_proto2_static::flags>
  transaction_proto2_static::g_flags;
event_counter
  transaction_proto2_static::g_evt_worker_thread_wait_log_buffer(
      "worker_thread_wait_log_buffer");
event_counter
  transaction_proto2_static::g_evt_dbtuple_no_space_for_delkey(
      "dbtuple_no_space_for_delkey");
event_counter
  transaction_proto2_static::g_evt_proto_gc_delete_requeue(
      "proto_gc_delete_requeue");
event_avg_counter
  transaction_proto2_static::g_evt_avg_proto_gc_queue_len(
      "avg_proto_gc_queue_len");
