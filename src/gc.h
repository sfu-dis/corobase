#ifndef __ERMIA_GC_
#define __ERMIA_GC_

#include <type_traits>
#include <thread>
#include <numa.h>
#include "macros.h"
#include "core.h"
#include "util.h"

/*
 * The garbage collector for ERMIA to remove dead tuple versions.
 */

namespace GC {
  // percore memory allocation counter for GC
  extern percore<size_t, false, false> allocated_memory;

  // if _allocated_memory reaches this many, start GC
  static const size_t WATERMARK = 65536; // in bytes
  size_t sum_allocated_memory();

  class gc_thread {
    public:
      gc_thread();
    private:
      static void do_gc();
      static void gc_func();
  };

  extern gc_thread GC_thread;

  inline void
  record_mem_alloc(size_t nbytes)
  {
    allocated_memory.my() += nbytes;
  }
};  // end of namespace
#endif

