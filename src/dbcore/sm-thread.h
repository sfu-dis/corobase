#pragma once

#include <numa.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#include "sm-defs.h"
#include "xid.h"
#include "../macros.h"

namespace thread {

struct sm_thread {
  typedef std::function<void(char *task_input)> task_t;
  std::thread thd;
  uint16_t node;
  uint16_t core;
  XID current_xid;
  bool shutdown;
  bool has_work;
  task_t task;
  char *task_input;

  std::condition_variable trigger;
  std::mutex trigger_lock;

  sm_thread(uint16_t n, uint16_t c) :
    node(n),
    core(c),
    current_xid(INVALID_XID),
    shutdown(false),
    has_work(false),
    task(nullptr) {
    thd = std::move(std::thread(&sm_thread::idle_task, this));
  }

  ~sm_thread() {}

  void idle_task();

  // No CC whatsoever, caller must know what it's doing
  inline void start_task(task_t t, char* input = nullptr) {
    task = t;
    task_input = input;
    has_work = true;
    __sync_synchronize();
    trigger.notify_all();
  }

  inline void join() {
    while (volatile_read(has_work)) { /** spin **/}
  }

  inline bool try_join() {
    return not volatile_read(has_work);
  }

  inline void destroy() {
    volatile_write(shutdown, true);
  }
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
    if (pos == sysconf::max_threads_per_node) {
      return nullptr;
    }
    if (not __sync_bool_compare_and_swap(&bitmap, b, b | (1UL << pos))) {
      goto retry;
    }
    ALWAYS_ASSERT(pos < sysconf::max_threads_per_node);
    //std::cout << "get_thread(): node " << threads[pos].node << ", " << pos << std::endl;
    return threads + pos;
  }

  inline void put_thread(sm_thread *t) {
    ASSERT(not t->has_work);
    auto b = ~uint64_t{1UL << (t - threads)};
    //std::cout << "put_thread(): node " << t->node << ", " << t->core << std::endl;
    __sync_fetch_and_and(&bitmap, b);
  }

  node_thread_pool(uint16_t n) : node(n), bitmap(0UL) {
    ALWAYS_ASSERT(!numa_run_on_node(node));
    threads = (sm_thread *)numa_alloc_onnode(
      sizeof(sm_thread) * sysconf::max_threads_per_node, node);
    for (uint core = 0; core < sysconf::max_threads_per_node; core++) {
      new (threads + core) sm_thread(node, core);
    }
  }
};

extern node_thread_pool *thread_pools;

inline void init() {
  thread_pools = (node_thread_pool *)malloc(sizeof(node_thread_pool) * sysconf::numa_nodes);
  for (uint16_t i = 0; i < sysconf::numa_nodes; i++) {
    new (thread_pools + i) node_thread_pool(i);
  }
}

inline sm_thread *get_thread(uint16_t from) {
  return thread_pools[from].get_thread();
}

inline sm_thread *get_thread(/* don't care where */) {
  for (uint16_t i = 0; i < sysconf::numa_nodes; i++) {
    auto* t = thread_pools[i].get_thread();
    if (t) {
      return t;
    }
  }
  return nullptr;
}

inline void put_thread(sm_thread *t) {
  thread_pools[t->node].put_thread(t);
}

// A wrapper that includes sm_thread for user code to use.
// Benchmark and log replay threads deal with this only,
// not with sm_thread.
class sm_runner {
public:
  sm_runner() : me(nullptr) {}
  ~sm_runner() {
    if (me) {
      join();
    }
  }

  virtual void my_work(char *) = 0;

  inline void start() {
    ALWAYS_ASSERT(me);
    thread::sm_thread::task_t t = std::bind(&sm_runner::my_work, this, std::placeholders::_1);
    me->start_task(t);
  }

  inline bool try_impersonate() {
    ALWAYS_ASSERT(not me);
    me = thread::get_thread();
    return me != nullptr;
  }

  inline void join() {
    me->join();
    put_thread(me);
    me = nullptr;
  }
  inline bool is_impersonated() { return me != nullptr; }
  inline bool try_join() {
    if (me->try_join()) {
      put_thread(me);
      me = nullptr;
      return true;
    }
    return false;
  }

private:
  sm_thread *me;
};
}  // namespace thread

