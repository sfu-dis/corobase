#include "gc.h"

namespace GC {
  gc_thread GC_thread;
  percore<size_t, false, false> allocated_memory;
};

size_t
GC::sum_allocated_memory()
{
  size_t s = 0;
  for (size_t i = 0; i < coreid::NMaxCores; i++)
    s += allocated_memory[i];
  return s;
}

void
GC::gc_thread::do_gc()
{
}

void
GC::gc_thread::gc_func()
{
  // FIXME: tzwang: change this to use some cond var so that
  // at tx boundary we can poke it to wake up GC. In addition,
  // the GC thread should also wakeup some every x seconds.
  while (1) {
    size_t sum = sum_allocated_memory();
    if (sum >= WATERMARK) {
      //std::cout << "memory allocated: " << sum << std::endl;
      // reset individual counters - not accurate but should be ok
      for (size_t i = 0; i < GC::allocated_memory.size(); i++)
        GC::allocated_memory[i] = 0;
      do_gc();
    }
  }
}

GC::gc_thread::gc_thread()
{
  std::cout << "GC thread start" << std::endl;
  std::thread t(gc_func);
  t.detach();
}

