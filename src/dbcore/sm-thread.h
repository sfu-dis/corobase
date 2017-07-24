#pragma once

#include <numa.h>

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <thread>

#include <sys/stat.h>

#include "sm-defs.h"
#include "xid.h"
#include "../macros.h"

namespace thread {

extern uint32_t
    next_thread_id;  // == total number of threads had so far - never decreases
extern __thread uint32_t thread_id;
extern __thread bool thread_initialized;

inline uint32_t my_id() {
  if (!thread_initialized) {
    thread_id = __sync_fetch_and_add(&next_thread_id, 1);
    thread_initialized = true;
  }
  return thread_id;
}

struct sm_thread {
  const uint8_t kStateHasWork = 1U;
  const uint8_t kStateSleep = 2U;
  const uint8_t kStateNoWork = 3U;

  typedef std::function<void(char *task_input)> task_t;
  std::thread thd;
  uint16_t node;
  uint16_t core;
  uint32_t sys_cpu;  // OS-given CPU number
  bool shutdown;
  uint8_t state;
  task_t task;
  char *task_input;
  bool sleep_when_idle;

  std::condition_variable trigger;
  std::mutex trigger_lock;

  // Not recommended: only use when there isn't /proc/devices/cpu* available.
  sm_thread(uint16_t n, uint16_t c)
      : node(n),
        core(c),
        sys_cpu(-1),
        shutdown(false),
        state(kStateNoWork),
        task(nullptr),
        sleep_when_idle(true) {
    thd = std::move(std::thread(&sm_thread::idle_task, this));
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    // Assuming a topology where all physical cores are listed first,
    //       // ie 16-core cpu, 2-socket: 0-15 will be physical cores, 16 and
    //       // beyond are hyper threads.
    CPU_SET(core + node * config::max_threads_per_node, &cpuset);
    int rc = pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t),
                                    &cpuset);
    LOG(INFO) << "Binding thread " << core << " on node " << node << " to CPU " << sys_cpu;
    ALWAYS_ASSERT(rc == 0);
  }

  // Recommended: caller figure out the physical core's OS-given CPU number.
  // Otherwise we have to guess in the other ctor that doesn't take sys_cpu.
  sm_thread(uint16_t n, uint16_t c, uint32_t sys_cpu)
      : node(n),
        core(c),
        sys_cpu(sys_cpu),
        shutdown(false),
        state(kStateNoWork),
        task(nullptr),
        sleep_when_idle(true) {
    thd = std::move(std::thread(&sm_thread::idle_task, this));
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(sys_cpu, &cpuset);
    int rc = pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t),
                                    &cpuset);
    LOG(INFO) << "Binding thread " << core << " on node " << node << " to CPU " << sys_cpu;
    ALWAYS_ASSERT(rc == 0);
  }

  ~sm_thread() {}

  void idle_task();

  // No CC whatsoever, caller must know what it's doing
  inline void start_task(task_t t, char *input = nullptr) {
    task = t;
    task_input = input;
    auto s = __sync_val_compare_and_swap(&state, kStateNoWork, kStateHasWork);
    if (s == kStateSleep) {
      while (volatile_read(state) != kStateNoWork) {
        trigger.notify_all();
      }
      volatile_write(state, kStateHasWork);
    } else {
      ALWAYS_ASSERT(s == kStateNoWork);
    }
  }

  inline void join() {
    while (volatile_read(state) == kStateHasWork) { /** spin **/
    }
  }

  inline bool try_join() { return volatile_read(state) != kStateHasWork; }

  inline void destroy() { volatile_write(shutdown, true); }
};

struct node_thread_pool {
  uint16_t node CACHE_ALIGNED;
  sm_thread *threads CACHE_ALIGNED;
  uint64_t bitmap CACHE_ALIGNED;  // max 64 threads per node, 1 - busy, 0 - free

  inline sm_thread *get_thread() {
  retry:
    uint64_t b = volatile_read(bitmap);
    auto xor_pos = b ^ (~uint64_t{0});
    uint64_t pos = __builtin_ctzll(xor_pos);
    if (pos == config::max_threads_per_node) {
      return nullptr;
    }
    if (not __sync_bool_compare_and_swap(&bitmap, b, b | (1UL << pos))) {
      goto retry;
    }
    ALWAYS_ASSERT(pos < config::max_threads_per_node);
    return threads + pos;
  }

  inline void put_thread(sm_thread *t) {
    auto b = ~uint64_t{1UL << (t - threads)};
    __sync_fetch_and_and(&bitmap, b);
  }

  node_thread_pool(uint16_t n) : node(n), bitmap(0UL) {
    ALWAYS_ASSERT(!numa_run_on_node(node));
    threads = (sm_thread *)numa_alloc_onnode(
        sizeof(sm_thread) * config::max_threads_per_node, node);

    // FIXME(tzwang): Linux-specific way of querying NUMA topology
    //
    // We used to query /sys/devices/system/node/nodeX/cpulist to get a list of
    // all cores for this node, but it could be a comma-separated list (x, y,
    // z) or a range (x-y). So we just iterate each cpu dir here until dir not
    // found.
    uint32_t cpu = 0;
    std::vector<uint32_t> phy_cores;
    struct stat info;
    if (stat("/sys/devices/system/node", &info) == 0) {
      while (cpu < std::thread::hardware_concurrency()) {
        std::string dir_name = "/sys/devices/system/node/node" +
                                std::to_string(n) + "/cpu" + std::to_string(cpu);
        struct stat info;
        if (stat(dir_name.c_str(), &info) != 0) {
          // Doesn't exist, continue to next so we can get all cores in the
          // same node
          ++cpu;
          continue;
        }
        ALWAYS_ASSERT(info.st_mode & S_IFDIR);

        // Make sure it's a physical thread, not a hyper-thread
        // Query /sys/devices/system/cpu/cpuX/topology/thread_siblings_list,
        // if the first number matches X, then it's a physical core [1]
        // (might not work in virtualized environments like Xen).
        // [1] https://stackoverflow.com/questions/7274585/linux-find-out-hyper-threaded-core-id
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
          phy_cores.push_back(cpu);
          LOG(INFO) << "Physical core: " << phy_cores[phy_cores.size()-1];
        }
        ++cpu;
      }
      for (uint core = 0; core < config::max_threads_per_node; core++) {
        new (threads + core) sm_thread(node, core, phy_cores[core]);
      }
    } else {
      // No desired /sys/devices interface, fallback to our assumption
      for (uint core = 0; core < config::max_threads_per_node; core++) {
        new (threads + core) sm_thread(node, core);
      }
    }
  }
};

extern node_thread_pool *thread_pools;

inline void init() {
  thread_pools =
      (node_thread_pool *)malloc(sizeof(node_thread_pool) * config::numa_nodes);
  for (uint16_t i = 0; i < config::numa_nodes; i++) {
    new (thread_pools + i) node_thread_pool(i);
  }
}

inline sm_thread *get_thread(uint16_t from) {
  return thread_pools[from].get_thread();
}

inline sm_thread *get_thread(/* don't care where */) {
  for (uint16_t i = 0; i < config::numa_nodes; i++) {
    auto *t = thread_pools[i].get_thread();
    if (t) {
      return t;
    }
  }
  return nullptr;
}

inline void put_thread(sm_thread *t) { thread_pools[t->node].put_thread(t); }

// A wrapper that includes sm_thread for user code to use.
// Benchmark and log replay threads deal with this only,
// not with sm_thread.
struct sm_runner {
  sm_runner() : me(nullptr) {}
  ~sm_runner() {
    if (me) {
      join();
    }
  }

  virtual void my_work(char *) = 0;

  inline void start() {
    ALWAYS_ASSERT(me);
    thread::sm_thread::task_t t =
        std::bind(&sm_runner::my_work, this, std::placeholders::_1);
    me->start_task(t);
  }

  inline bool try_impersonate(bool sleep_when_idle = true) {
    ALWAYS_ASSERT(not me);
    me = thread::get_thread();
    if (me) {
      me->sleep_when_idle = sleep_when_idle;
    }
    return me != nullptr;
  }

  inline void join() {
    me->join();
    put_thread(me);
    me = nullptr;
  }
  // Same as join(), but don't return the thread
  inline void wait() { me->join(); }
  inline bool try_wait() { return me->try_join(); }
  inline bool is_impersonated() { return me != nullptr; }
  inline bool try_join() {
    if (me->try_join()) {
      put_thread(me);
      me = nullptr;
      return true;
    }
    return false;
  }

  sm_thread *me;
};
}  // namespace thread
