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

                /** garbage collection subsystem **/

static event_counter evt_local_chain_cleanups("local_chain_cleanups");
static event_counter evt_try_delete_unlinks("try_delete_unlinks");
static event_avg_counter evt_avg_time_inbetween_ro_epochs_usec(
    "avg_time_inbetween_ro_epochs_usec");

void
transaction_proto2_static::InitGC()
{
  g_flags->g_gc_init.store(true, memory_order_release);
}

static void
sleep_ro_epoch()
{
  const uint64_t sleep_ns = transaction_proto2_static::ReadOnlyEpochUsec * 1000;
  struct timespec t;
  t.tv_sec  = sleep_ns / ONE_SECOND_NS;
  t.tv_nsec = sleep_ns % ONE_SECOND_NS;
  nanosleep(&t, nullptr);
}

// tzwang: this is never used??
void
transaction_proto2_static::PurgeThreadOutstandingGCTasks()
{
#ifdef PROTO2_CAN_DISABLE_GC
  if (!IsGCEnabled())
    return;
#endif
  INVARIANT(!rcu::s_instance.in_rcu_region());
  threadctx &ctx = g_threadctxs.my();
  uint64_t e;
  if (!ctx.queue_.get_latest_epoch(e))
    return;
  // wait until we can clean up e
  for (;;) {
    const uint64_t last_tick_ex = ticker::s_instance.global_last_tick_exclusive();
    const uint64_t ro_tick_ex = to_read_only_tick(last_tick_ex);
    if (unlikely(!ro_tick_ex)) {
      sleep_ro_epoch();
      continue;
    }
    const uint64_t ro_tick_geq = ro_tick_ex - 1;
    if (ro_tick_geq < e) {
      sleep_ro_epoch();
      continue;
    }
    break;
  }
  clean_up_to_including(ctx, e);
  INVARIANT(ctx.queue_.empty());
}

//#ifdef CHECK_INVARIANTS
//// make sure hidden is blocked by version e, when traversing from start
//static bool
//IsBlocked(dbtuple *start, dbtuple *hidden, uint64_t e)
//{
//  dbtuple *c = start;
//  while (c) {
//    if (c == hidden)
//      return false;
//    if (c->is_not_behind(e))
//      // blocked
//      return true;
//    c = c->next;
//  }
//  ALWAYS_ASSERT(false); // hidden should be found on chain
//}
//#endif

void
transaction_proto2_static::clean_up_to_including(threadctx &ctx, uint64_t ro_tick_geq)
{
  INVARIANT(!rcu::s_instance.in_rcu_region());
  INVARIANT(ctx.last_reaped_epoch_ <= ro_tick_geq);
  INVARIANT(ctx.scratch_.empty());
  if (ctx.last_reaped_epoch_ == ro_tick_geq)
    return;

#ifdef ENABLE_EVENT_COUNTERS
  const uint64_t now = timer::cur_usec();
  if (ctx.last_reaped_timestamp_us_ > 0) {
    const uint64_t diff = now - ctx.last_reaped_timestamp_us_;
    evt_avg_time_inbetween_ro_epochs_usec.offer(diff);
  }
  ctx.last_reaped_timestamp_us_ = now;
#endif
  ctx.last_reaped_epoch_ = ro_tick_geq;

#ifdef CHECK_INVARIANTS
  const uint64_t last_tick_ex = ticker::s_instance.global_last_tick_exclusive();
  INVARIANT(last_tick_ex);
  const uint64_t last_consistent_tid = ComputeReadOnlyTid(last_tick_ex - 1);
  const uint64_t computed_last_tick_ex = ticker::s_instance.compute_global_last_tick_exclusive();
  INVARIANT(last_tick_ex <= computed_last_tick_ex);
  INVARIANT(to_read_only_tick(last_tick_ex) > ro_tick_geq);
#endif

  // XXX: hacky
  char rcu_guard[sizeof(scoped_rcu_base<false>)] = {0};
  const size_t max_niters_with_rcu = 128;
#define ENTER_RCU() \
    do { \
      new (&rcu_guard[0]) scoped_rcu_base<false>(); \
    } while (0)
#define EXIT_RCU() \
    do { \
      scoped_rcu_base<false> *px = (scoped_rcu_base<false> *) &rcu_guard[0]; \
      px->~scoped_rcu_base<false>(); \
    } while (0)

  ctx.scratch_.empty_accept_from(ctx.queue_, ro_tick_geq);
  ctx.scratch_.transfer_freelist(ctx.queue_);
  px_queue &q = ctx.scratch_;
  if (q.empty())
    return;
  bool in_rcu = false;
  size_t niters_with_rcu = 0, n = 0;
  for (auto it = q.begin(); it != q.end(); ++it, ++n, ++niters_with_rcu) {
    auto &delent = *it;
    INVARIANT(delent.tuple()->opaque.load(std::memory_order_acquire) == 1);
    if (!delent.key_.get_flags()) {
      // guaranteed to be gc-able now (even w/o RCU)
#ifdef CHECK_INVARIANTS
      if (delent.trigger_tid_ > last_consistent_tid /*|| !IsBlocked(delent.tuple_ahead_, delent.tuple(), last_consistent_tid) */) {
        cerr << "tuple ahead     : " << g_proto_version_str(delent.tuple_ahead_->version) << endl;
        cerr << "tuple ahead     : " << *delent.tuple_ahead_ << endl;
        cerr << "trigger tid     : " << g_proto_version_str(delent.trigger_tid_) << endl;
        cerr << "tuple           : " << g_proto_version_str(delent.tuple()->version) << endl;
        cerr << "last_consist_tid: " << g_proto_version_str(last_consistent_tid) << endl;
        cerr << "last_tick_ex    : " << last_tick_ex << endl;
        cerr << "ro_tick_geq     : " << ro_tick_geq << endl;
        cerr << "rcu_block_tick  : " << it.tick() << endl;
      }
      INVARIANT(delent.trigger_tid_ <= last_consistent_tid);
      delent.tuple()->opaque.store(0, std::memory_order_release);
#endif
      dbtuple::release_no_rcu(delent.tuple());
    } else {
      INVARIANT(!delent.tuple_ahead_);
      INVARIANT(delent.btr_);
      // check if an element preceeds the (deleted) tuple before doing the delete
      ::lock_guard<dbtuple> lg_tuple(delent.tuple(), false);
#ifdef CHECK_INVARIANTS
      if (!delent.tuple()->is_not_behind(last_consistent_tid)) {
        cerr << "trigger tid     : " << g_proto_version_str(delent.trigger_tid_) << endl;
        cerr << "tuple           : " << g_proto_version_str(delent.tuple()->version) << endl;
        cerr << "last_consist_tid: " << g_proto_version_str(last_consistent_tid) << endl;
        cerr << "last_tick_ex    : " << last_tick_ex << endl;
        cerr << "ro_tick_geq     : " << ro_tick_geq << endl;
        cerr << "rcu_block_tick  : " << it.tick() << endl;
      }
      INVARIANT(delent.tuple()->version == delent.trigger_tid_);
      INVARIANT(delent.tuple()->is_not_behind(last_consistent_tid));
      INVARIANT(delent.tuple()->is_deleting());
#endif
      if (unlikely(!delent.tuple()->is_latest())) {
        // requeue it up, except this time as a regular delete
        const uint64_t my_ro_tick = to_read_only_tick(
            ticker::s_instance.global_current_tick());
        ctx.queue_.enqueue(
            delete_entry(
              nullptr,
              MakeTid(CoreMask, NumIdMask >> NumIdShift, (my_ro_tick + 1) * ReadOnlyEpochMultiplier - 1),
              delent.tuple(),
              marked_ptr<string>(),
              nullptr),
            my_ro_tick);
        ++g_evt_proto_gc_delete_requeue;
        // reclaim string ptrs
        string *spx = delent.key_.get();
        if (unlikely(spx))
          ctx.pool_.emplace_back(spx);
        continue;
      }
#ifdef CHECK_INVARIANTS
      delent.tuple()->opaque.store(0, std::memory_order_release);
#endif
      // if delent.key_ is nullptr, then the key is stored in the tuple
      // record storage location, and the size field contains the length of
      // the key
      //
      // otherwise, delent.key_ is a pointer to a string containing the
      // key
      varkey k;
      string *spx = delent.key_.get();
      if (likely(!spx)) {
        k = varkey(delent.tuple()->get_value_start(), delent.tuple()->size);
      } else {
        k = varkey(*spx);
        ctx.pool_.emplace_back(spx);
      }

      if (!in_rcu) {
        ENTER_RCU();
        niters_with_rcu = 0;
        in_rcu = true;
      }
      typename concurrent_btree::value_type removed = 0;
      const bool did_remove = delent.btr_->remove(k, &removed);
      ALWAYS_ASSERT(did_remove);
      INVARIANT(removed == (typename concurrent_btree::value_type) delent.tuple());
      delent.tuple()->clear_latest();
      dbtuple::release(delent.tuple()); // rcu free it
    }

    if (in_rcu && niters_with_rcu >= max_niters_with_rcu) {
      EXIT_RCU();
      niters_with_rcu = 0;
      in_rcu = false;
    }
  }
  q.clear();
  g_evt_avg_proto_gc_queue_len.offer(n);

  if (in_rcu)
    EXIT_RCU();
  INVARIANT(!rcu::s_instance.in_rcu_region());
}

aligned_padded_elem<transaction_proto2_static::hackstruct>
  transaction_proto2_static::g_hack;
aligned_padded_elem<transaction_proto2_static::flags>
  transaction_proto2_static::g_flags;
percore_lazy<transaction_proto2_static::threadctx>
  transaction_proto2_static::g_threadctxs;
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
  transaction_proto2_static::g_evt_avg_log_entry_size(
      "avg_log_entry_size");
event_avg_counter
  transaction_proto2_static::g_evt_avg_proto_gc_queue_len(
      "avg_proto_gc_queue_len");
