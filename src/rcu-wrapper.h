#ifndef __RCU_WRAPPER_
#define __RCU_WRAPPER_

#include <type_traits>
#include <numa.h>
#include "macros.h"
#include "core.h"
#include "dbcore/rcu.h"
#include "dbcore/sm-gc.h"

// FIXME: tzwang: seems to be working
// things won't work directly:
// free_with_fn()
// free_array() (used by imstring)
// silo's rcu_register()

namespace RCU {
  typedef void (*deleter_t)(void *);

  inline void *allocate(size_t nbytes) {
    return rcu_alloc_gc(nbytes);
  }

  template <typename T>
  static inline void
  deleter_array(void *p)
  {
    delete [] (T *) p;
  }

  inline void free_with_fn(void *p, deleter_t fn)
  {
    // stuff the pointer to delete_entry in the size area of p
    rcu_pointer u = {p};
    (--u.p)->size |= (((unsigned long)fn) << 8);
    rcu_free(p);  // don't call rcu_free in deleter
  }

  template <typename T>
  inline void
  free_array(T *p)
  {
    free_with_fn(p, deleter_array<T>);
  }

  void pin_current_thread(size_t cpu);
};

class scoped_rcu_region {
private:
  // tls depth field to flag when we should do deregister and exit
  // (inherited from silo's depth_ field of the sync class)
  static percore_lazy<int> _depths;

public:
  scoped_rcu_region()
  {
    int &d = scoped_rcu_region::_depths.my();
    ++d;
    INVARIANT(d > 0);
    if (d == 1) {
      if (!RCU::rcu_is_registered())
        RCU::rcu_register();
      if (!RCU::rcu_is_active())
        RCU::rcu_enter();
    }
  }

/* Notes on silo's RCU: it seems to be just doing GC, anything more? */

  ~scoped_rcu_region()
  {
    INVARIANT(RCU::rcu_is_active());
    int &d = _depths.my();
    --d;
    INVARIANT(d >= 0);
    RCU::rcu_quiesce();  // ?
    if (d == 0) {
      RCU::rcu_exit();  // or should do rcu_quiesce()?
      //RCU::rcu_gc_info info = RCU::rcu_get_gc_info();
      //if (info.objects_freed)
      //  fprintf(stderr, "RCU subsystem freed %zd objects in %zd passes\n",
      //          info.objects_freed, info.gc_passes);
    }
  }

  int depth(void) { return scoped_rcu_region::_depths.my(); }
};

class disabled_rcu_region {};
#endif
