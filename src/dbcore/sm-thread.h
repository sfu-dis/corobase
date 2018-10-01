#pragma once

#include <numa.h>

#include <condition_variable>
#include <fstream>
#include <functional>
#include <mutex>
#include <thread>

#include <sys/stat.h>

#include "sm-defs.h"
#include "xid.h"
#include "../util.h"

namespace ermia {
namespace thread {

struct CPUCore {
  uint32_t node;
  uint32_t physical_thread;
  std::vector<uint32_t> logical_threads;
  CPUCore(uint32_t n, uint32_t phys) : node(n), physical_thread(phys) {}
  void AddLogical(uint32_t t) { logical_threads.push_back(t); }
};

extern std::vector<CPUCore> cpu_cores;

bool DetectCPUCores();
void Initialize();

extern uint32_t
    next_thread_id;  // == total number of threads had so far - never decreases
extern __thread uint32_t thread_id;
extern __thread bool thread_initialized;

inline uint32_t MyId() {
  if (!thread_initialized) {
    thread_id = __sync_fetch_and_add(&next_thread_id, 1);
    thread_initialized = true;
  }
  return thread_id;
}

struct Thread {
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
  bool is_physical;

  std::condition_variable trigger;
  std::mutex trigger_lock;

  Thread(uint16_t n, uint16_t c, uint32_t sys_cpu, bool is_physical);
  ~Thread() {}

  void IdleTask();

  // No CC whatsoever, caller must know what it's doing
  inline void StartTask(task_t t, char *input = nullptr) {
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

  inline void Join() { while (volatile_read(state) == kStateHasWork) {} }
  inline bool TryJoin() { return volatile_read(state) != kStateHasWork; }
  inline void Destroy() { volatile_write(shutdown, true); }
};

struct PerNodeThreadPool {
  static uint32_t max_threads_per_node;
  uint16_t node CACHE_ALIGNED;
  Thread *threads CACHE_ALIGNED;
  uint64_t bitmap CACHE_ALIGNED;  // max 64 threads per node, 1 - busy, 0 - free

  inline Thread *GetThread(bool physical = true) {
  retry:
    uint64_t b = volatile_read(bitmap);
    uint64_t xor_pos = b ^ (~uint64_t{0});
    uint64_t pos = __builtin_ctzll(xor_pos);
    if (pos == max_threads_per_node) {
      return nullptr;
    }

    Thread *t = threads + pos;
    // Find the thread that matches the preferred type 
    bool found = (t->is_physical != physical);
    while (true) {
      ++pos;
      if (pos >= max_threads_per_node) {
        return nullptr;
      }
      t = &threads[pos];
      if ((!((1UL << pos) & b)) && (t->is_physical == physical)) {
        break;
      }
    }

    if (not __sync_bool_compare_and_swap(&bitmap, b, b | (1UL << pos))) {
      goto retry;
    }
    ALWAYS_ASSERT(pos < max_threads_per_node);
    return t;
  }

  inline void PutThread(Thread *t) {
    auto b = ~uint64_t{1UL << (t - threads)};
    __sync_fetch_and_and(&bitmap, b);
  }

  PerNodeThreadPool(uint16_t n);
};

extern PerNodeThreadPool *thread_pools;

inline Thread *GetThread(uint16_t from) {
  return thread_pools[from].GetThread();
}

inline Thread *GetThread(/* don't care where */) {
  for (uint16_t i = 0; i < config::numa_nodes; i++) {
    auto *t = thread_pools[i].GetThread();
    if (t) {
      return t;
    }
  }
  return nullptr;
}

inline void PutThread(Thread *t) { thread_pools[t->node].PutThread(t); }

// A wrapper that includes Thread for user code to use.
// Benchmark and log replay threads deal with this only,
// not with Thread.
struct Runner {
  Runner() : me(nullptr) {}
  ~Runner() {
    if (me) {
      Join();
    }
  }

  virtual void MyWork(char *) = 0;

  inline void Start() {
    ALWAYS_ASSERT(me);
    thread::Thread::task_t t =
        std::bind(&Runner::MyWork, this, std::placeholders::_1);
    me->StartTask(t);
  }

  inline bool TryImpersonate(bool sleep_when_idle = true) {
    ALWAYS_ASSERT(not me);
    me = thread::GetThread();
    if (me) {
      me->sleep_when_idle = sleep_when_idle;
    }
    return me != nullptr;
  }

  inline void Join() {
    me->Join();
    PutThread(me);
    me = nullptr;
  }
  // Same as Join(), but don't return the thread
  inline void Wait() { me->Join(); }
  inline bool TryWait() { return me->TryJoin(); }
  inline bool IsImpersonated() { return me != nullptr; }
  inline bool TryJoin() {
    if (me->TryJoin()) {
      PutThread(me);
      me = nullptr;
      return true;
    }
    return false;
  }

  Thread *me;
};
}  // namespace thread
}  // namespace ermia
