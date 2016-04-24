#include "sm-alloc.h"
#include "sm-log.h"
#include "sm-thread.h"

namespace thread {

node_thread_pool *thread_pools = nullptr;

void sm_thread::idle_task() {
  ALWAYS_ASSERT(!numa_run_on_node(node));
  ALWAYS_ASSERT(!sched_yield());
  std::unique_lock<std::mutex> lock(trigger_lock);

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  TXN::assign_reader_bitmap_entry();
#endif
  // XXX. RCU register/deregister should be the outer most one b/c
  // MM::deregister_thread could call cur_lsn inside
  RCU::rcu_register();
  MM::register_thread();
  RCU::rcu_start_tls_cache( 32, 100000 );

  while (not volatile_read(shutdown)) {
    if (volatile_read(has_work)) {
      task(task_input);
      auto my_offset = logmgr->get_tls_lsn_offset();
      // Must use a while loop here instead of using logmgr->wait_for_durable();
      // otherwise the N-1 out of N threads reached here at the same time will
      // stuck - only the first guy can return from wait_for_durable() and the
      // rest will wait indefinitely because flush() always flushes up to the
      // smallest tls_lsn_offset. Invoking flush at the same time results in the
      // same smallest offset and stuck at wait_for_durable. 
      while (logmgr->durable_flushed_lsn().offset() < my_offset) {
        logmgr->flush();
      }
      logmgr->set_tls_lsn_offset(0);  // clear thread as if did nothing!
      volatile_write(has_work, false);
    }
    // FIXME(tzwang): add a work queue so we can
    // continue if there is more work to do
    trigger.wait(lock);
  }

  MM::deregister_thread();
  RCU::rcu_deregister();
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  TXN::deassign_reader_bitmap_entry();
#endif

}

}  // namespace thread
