#ifndef __RCU_WRAPPER_
#define __RCU_WRAPPER_

#include "rcu/rcu.h"
#include "pxqueue.h"  // tzwang: maybe don't need this

// things won't work directly:
// free_with_fn()
// free_array() (used by imstring)
// silo's rcu_register()

class scoped_rcu_region {
public:
  scoped_rcu_region()
  {
    RCU::rcu_register();
    RCU::rcu_enter();
  }
  ~scoped_rcu_region()
  {
    RCU::rcu_quiesce();
  }
};

namespace RCU {
  // FIXME: tzwang: no-op for now
  static const size_t NQueueGroups = 32;
  typedef void (*deleter_t)(void *);
  inline void
  free_with_fn(void *p, deleter_t fn)
  {
  }
};
class disabled_rcu_region {};
#endif
