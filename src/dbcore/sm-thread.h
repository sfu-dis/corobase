#pragma once

#include <numa.h>

#include <condition_variable>
#include <mutex>
#include <thread>

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
  bool shutdown;
  uint8_t state;
  task_t task;
  char *task_input;
  bool sleep_when_idle;

  std::condition_variable trigger;
  std::mutex trigger_lock;

  sm_thread(uint16_t n, uint16_t c)
      : node(n),
        core(c),
        shutdown(false),
        state(kStateNoWork),
        task(nullptr),
        sleep_when_idle(true) {
    thd = std::move(std::thread(&sm_thread::idle_task, this));
    // Make sure we run on a physical core, not a hyper thread
    if (config::htt_is_on) {
      // Assuming a topology where all physical cores are listed first,
      // ie 16-core cpu, 2-socket: 0-15 will be physical cores, 16 and
      // beyond are hyper threads.
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      //CPU_SET(core + node * config::max_threads_per_node, &cpuset);
      CPU_SET(node + 4 * core, &cpuset);
      int rc = pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t),
                                      &cpuset);
      ALWAYS_ASSERT(rc == 0);
    }
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
    for (uint core = 0; core < config::max_threads_per_node; core++) {
      new (threads + core) sm_thread(node, core);
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
