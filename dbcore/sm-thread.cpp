#include "rcu.h"
#include "serial.h"
#include "sm-alloc.h"
#include "sm-log.h"
#include "sm-thread.h"

namespace ermia {
namespace thread {

std::atomic<uint32_t> next_thread_id(0);
PerNodeThreadPool *thread_pools = nullptr;
thread_local bool thread_initialized CACHE_ALIGNED;
uint32_t PerNodeThreadPool::threads_count = 0;

std::vector<CPUCore> cpu_cores;
bool DetectCPUCores() {
  // FIXME(tzwang): Linux-specific way of querying NUMA topology
  //
  // We used to query /sys/devices/system/node/nodeX/cpulist to get a list of
  // all cores for this node, but it could be a comma-separated list (x, y, z)
  // or a range (x-y). So we just iterate each cpu dir here until dir not
  // found.
  struct stat info;
  if (stat("/sys/devices/system/node", &info) != 0) {
    return false;
  }

  for (uint32_t node = 0; node < numa_max_node() + 1; ++node) {
    uint32_t cpu = 0;
    while (cpu < std::thread::hardware_concurrency()) {
      std::string dir_name = "/sys/devices/system/node/node" +
                              std::to_string(node) + "/cpu" + std::to_string(cpu);
      struct stat info;
      if (stat(dir_name.c_str(), &info) != 0) {
        // Doesn't exist, continue to next to get all cores in the same node
        ++cpu;
        continue;
      }
      ALWAYS_ASSERT(info.st_mode & S_IFDIR);

      // Make sure it's a physical thread, not a hyper-thread: Query
      // /sys/devices/system/cpu/cpuX/topology/thread_siblings_list, if the
      // first number matches X, then it's a physical core [1] (might not work
      // in virtualized environments like Xen).  [1]
      // https://stackoverflow.com/questions/7274585/linux-find-out-hyper-threaded-core-id
      std::string sibling_file_name = "/sys/devices/system/cpu/cpu" +
                                      std::to_string(cpu) +
                                      "/topology/thread_siblings_list";
      char cpu_buf[8];
      memset(cpu_buf, 0, 8);
      std::vector<uint32_t> threads;
      std::ifstream sibling_file(sibling_file_name);
      while (sibling_file.good()) {
        memset(cpu_buf, 0, 8);
        sibling_file.getline(cpu_buf, 256, ',');
        threads.push_back(atoi(cpu_buf));
      }

      // A physical core?
      if (cpu == threads[0]) {
        cpu_cores.emplace_back(node, threads[0]);
        for (uint32_t i = 1; i < threads.size(); ++i) {
          cpu_cores[cpu_cores.size()-1].AddLogical(threads[i]);
        }
        LOG(INFO) << "Physical core: " << cpu_cores[cpu_cores.size()-1].physical_thread;
        for (uint32_t i = 0; i < cpu_cores[cpu_cores.size()-1].logical_threads.size(); ++i) {
          LOG(INFO) << "Logical core: " << cpu_cores[cpu_cores.size()-1].logical_threads[i];
        }
      }
      ++cpu;
    }
  }
  return true;
}

Thread::Thread(uint16_t n, uint16_t c, uint32_t sys_cpu, bool is_physical)
    : node(n),
      core(c),
      sys_cpu(sys_cpu),
      shutdown(false),
      state(kStateNoWork),
      task(nullptr),
      sleep_when_idle(true),
      is_physical(is_physical) {
  int rc = pthread_attr_init (&thd_attr);
  pthread_create(&thd, &thd_attr, &Thread::StaticIdleTask, (void *)this);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(sys_cpu, &cpuset);
  rc = pthread_setaffinity_np(thd, sizeof(cpu_set_t), &cpuset);
  LOG(INFO) << "Binding thread " << core << " on node " << node << " to CPU " << sys_cpu;
  ALWAYS_ASSERT(rc == 0);
}

PerNodeThreadPool::PerNodeThreadPool(uint16_t n) : node(n), bitmap(0UL) {
  ALWAYS_ASSERT(!numa_run_on_node(node));
  threads = (Thread *)numa_alloc_onnode(sizeof(Thread) * threads_count, node);

  if (cpu_cores.size()) {
    const uint32_t total_numa_nodes = numa_max_node() + 1;
    const uint32_t node_core_count = cpu_cores.size() / total_numa_nodes;
    LOG(INFO) << "Node " << n << " has " <<  node_core_count
              << " physical cores, " << threads_count << " threads";

    // Allocate physical threads first
    uint32_t core = 0;
    for (uint32_t i = 0; i < cpu_cores.size(); ++i) {
      auto &c = cpu_cores[i];
      if (c.node == n && core < threads_count) {
        uint32_t sys_cpu = c.physical_thread;
        new (threads + core) Thread(node, core, sys_cpu, true);
        ++core;
      }
    }

    for (uint32_t i = 0; i < cpu_cores.size(); ++i) {
      auto &c = cpu_cores[i];
      if (c.node == n && core < threads_count) {
        for (auto &sib : c.logical_threads) {
          if (core >= threads_count) {
              break;
          }
          new (threads + core) Thread(node, core, sib, false);
          ++core;
        }
      }
    }
  }
}

void Initialize() {
  ALWAYS_ASSERT(config::threads > 0);
  const uint32_t numa_node_count = numa_max_node() + 1;
  const uint32_t max_threads_per_node = std::thread::hardware_concurrency() / numa_node_count;
  config::threads = std::min(config::threads, max_threads_per_node * numa_node_count);

  bool detected = thread::DetectCPUCores();
  LOG_IF(FATAL, !detected);

  // Here [threads] refers to threads (physical or logical), so use the number of physical cores
  // to calculate # of numa nodes
  if (config::numa_spread) {
    config::numa_nodes = config::threads > numa_node_count ? numa_node_count : config::threads;
  } else {
    config::numa_nodes = std::ceil(config::threads / static_cast<float>(max_threads_per_node));
  }
  ALWAYS_ASSERT(config::numa_nodes > 0);

  // For simplicity, it allocates same number of threads for each node. So the actually number of
  // threads in thread_pool may be more than config::threads
  PerNodeThreadPool::threads_count = std::ceil(config::threads / static_cast<float>(config::numa_nodes));

  thread_pools = (PerNodeThreadPool *)malloc(sizeof(PerNodeThreadPool) * config::numa_nodes);
  for (uint16_t i = 0; i < config::numa_nodes; i++) {
    new (thread_pools + i) PerNodeThreadPool(i);
  }
}

void Thread::IdleTask() {
  std::unique_lock<std::mutex> lock(trigger_lock);

#if defined(SSN) || defined(SSI)
  TXN::assign_reader_bitmap_entry();
#endif
  // XXX. RCU register/deregister should be the outer most one b/c
  // MM::deregister_thread could call cur_lsn inside
  RCU::rcu_register();
  MM::register_thread();
  RCU::rcu_start_tls_cache(32, 100000);

  while (not volatile_read(shutdown)) {
    if (volatile_read(state) == kStateHasWork) {
      task(task_input);
      if (!config::IsShutdown() && logmgr and not config::is_backup_srv()) {
        // logmgr might be null during recovery and backups will flush on their
        // own
        auto my_offset = logmgr->get_tls_lsn_offset();
        // Must use a while loop here instead of using
        // logmgr->wait_for_durable();
        // otherwise the N-1 out of N threads reached here at the same time will
        // stuck - only the first guy can return from wait_for_durable() and the
        // rest will wait indefinitely because flush() always flushes up to the
        // smallest tls_lsn_offset. Invoking flush at the same time results in
        // the
        // same smallest offset and stuck at wait_for_durable.
        while (logmgr->durable_flushed_lsn().offset() < my_offset) {
          logmgr->flush();
        }
        logmgr->set_tls_lsn_offset(0);  // clear thread as if did nothing!
      }
      COMPILER_MEMORY_FENCE;
      volatile_write(state, kStateNoWork);
    }
    if (sleep_when_idle &&
        __sync_bool_compare_and_swap(&state, kStateNoWork, kStateSleep)) {
      // FIXME(tzwang): add a work queue so we can
      // continue if there is more work to do
      trigger.wait(lock);
      volatile_write(state, kStateNoWork);
      // Somebody woke me up, wait for work to do
      while (volatile_read(state) != kStateHasWork) { /** spin **/
      }
    }  // else can't sleep, go check another round
  }

  MM::deregister_thread();
  RCU::rcu_deregister();
#if defined(SSN) || defined(SSI)
  TXN::deassign_reader_bitmap_entry();
#endif
}

Thread *PerNodeThreadPool::GetThread(bool physical) {
retry:
  uint64_t b = volatile_read(bitmap);
  uint64_t xor_pos = b ^ (~uint64_t{0});
  uint64_t pos = __builtin_ctzll(xor_pos);

  Thread *t = nullptr;
  // Find the thread that matches the preferred type
  while (true) {
    if (pos >= threads_count) {
      return nullptr;
    }
    t = &threads[pos];
    if ((!((1UL << pos) & b)) && (t->is_physical == physical)) {
      break;
    }
    ++pos;
  }

  if (not __sync_bool_compare_and_swap(&bitmap, b, b | (1UL << pos))) {
    goto retry;
  }
  ALWAYS_ASSERT(pos < threads_count);
  return t;
}

bool PerNodeThreadPool::GetThreadGroup(std::vector<Thread*> &thread_group) {
retry:
  thread_group.clear();
  uint64_t b = volatile_read(bitmap);
  uint64_t xor_pos = b ^ (~uint64_t{0});
  uint64_t pos = __builtin_ctzll(xor_pos);

  Thread *t = nullptr;
  // Find the thread that matches the preferred type
  while (true) {
    if (pos >= threads_count) {
      return false;
    }
    t = &threads[pos];
    if ((!((1UL << pos) & b)) && t->is_physical) {
      break;
    }
    ++pos;
  }

  thread_group.push_back(t);

  // Got the physical thread, now try to claim the logical ones as well
  uint64_t count = 1;  // Number of 1-bits, including the physical thread
  ++pos;
  while (true) {
    t = threads + pos;
    if (t->is_physical) {
      break;
    } else {
      thread_group.push_back(t);
      ++count;
    }
  }

  // Fill [count] bits starting from [pos]
  uint64_t bits = 0;
  for (uint32_t i = pos; i < pos + count; ++i) {
    bits |= (1UL << pos);
  }
  if (not __sync_bool_compare_and_swap(&bitmap, b, b | bits)) {
    goto retry;
  }
  ALWAYS_ASSERT(pos < threads_count);
  return true;
}

}  // namespace thread
}  // namespace ermia
