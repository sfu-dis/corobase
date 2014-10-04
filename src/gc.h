#ifndef __ERMIA_GC_
#define __ERMIA_GC_

#include <type_traits>
#include<numa.h>
#include "macros.h"
#include "core.h"
#include "rcu/rcu.h"

/*
 * The garbage collector for ERMIA to remove dead tuple versions.
 */

namespace ERMIA_GC {
  // percore memory allocation counter for GC
  static percore_lazy<size_t> _allocated_memory;
};

class gc_thread {
private:
  size_t sum_allocated_memory();
};

#endif
