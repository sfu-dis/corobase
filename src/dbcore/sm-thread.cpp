#include "rcu.h"
#include "serial.h"
#include "sm-alloc.h"
#include "sm-log.h"
#include "sm-thread.h"

namespace ermia {
namespace thread {

uint32_t next_thread_id = 0;
node_thread_pool *thread_pools = nullptr;
__thread uint32_t thread_id CACHE_ALIGNED;
__thread bool thread_initialized CACHE_ALIGNED;

std::vector<uint32_t> phys_cores;
bool detect_phys_cores() {
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

      // Make sure it's a physical thread, not a hyper-thread Query
      // /sys/devices/system/cpu/cpuX/topology/thread_siblings_list, if the
      // first number matches X, then it's a physical core [1] (might not work
      // in virtualized environments like Xen).  [1]
      // https://stackoverflow.com/questions/7274585/linux-find-out-hyper-threaded-core-id
      std::string sibling_file_name = "/sys/devices/system/cpu/cpu" +
                                      std::to_string(cpu) +
                                      "/topology/thread_siblings_list";
      char cpu_buf[8];
      memset(cpu_buf, 0, 8);
      std::ifstream sibling_file(sibling_file_name);
      while (sibling_file.good()) {
        memset(cpu_buf, 0, 8);
        sibling_file.getline(cpu_buf, 256, ',');
        break;
      }

      // A physical core?
      if (cpu == atoi(cpu_buf)) {
        phys_cores.push_back(cpu);
        LOG(INFO) << "Physical core: " << phys_cores[phys_cores.size()-1];
      }
      ++cpu;
    }
  }
  return true;
}

void sm_thread::idle_task() {
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

}  // namespace thread
}  // namespace ermia
