#include "rcu-wrapper.h"
#include "txn.h"

void
RCU::pin_current_thread(size_t cpu)
{
  auto node = numa_node_of_cpu(cpu);
  // pin to node
  ALWAYS_ASSERT(!numa_run_on_node(node));
  // is numa_run_on_node() guaranteed to take effect immediately?
  ALWAYS_ASSERT(!sched_yield());
}

void*
RCU::allocate(size_t nbytes)
{
  void *mem = rcu_alloc(nbytes);
  return mem;
}
