#ifndef __RCU_WRAPPER_
#define __RCU_WRAPPER_

#include "macros.h"
#include "core.h"
#include "rcu/rcu.h"

// things won't work directly:
// free_with_fn()
// free_array() (used by imstring)
// silo's rcu_register()

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

  ~scoped_rcu_region()
  {
    INVARIANT(RCU::rcu_is_active());
    int &d = _depths.my();
    --d;
    INVARIANT(d >= 0);
    if (d == 0) {
      RCU::rcu_exit();  // or should do rcu_quiesce()?
      RCU::rcu_gc_info info = RCU::rcu_get_gc_info();
      fprintf(stderr, "RCU subsystem freed %zd objects in %zd passes\n",
              info.objects_freed, info.gc_passes);
    }
  }
};

namespace RCU {
  // FIXME: tzwang: no-op for now
  // static const size_t NQueueGroups = 32;
  typedef void (*deleter_t)(void *);

  struct delete_entry {
      void* ptr;
      intptr_t action;

      inline delete_entry(void* ptr, size_t sz)
          : ptr(ptr), action(-sz) {
          INVARIANT(action < 0);
      }
      inline delete_entry(void* ptr, deleter_t fn)
          : ptr(ptr), action(reinterpret_cast<uintptr_t>(fn)) {
          INVARIANT(action > 0);
      }
      void run() {
          if (action < 0)
              RCU::rcu_free(ptr); // FIXME: tzwang: should be back to slab
          else
              (*reinterpret_cast<deleter_t>(action))(ptr);
      }
      bool operator==(const delete_entry& x) const {
          return ptr == x.ptr && action == x.action;
      }
      bool operator!=(const delete_entry& x) const {
          return !(*this == x);
      }
      bool operator<(const delete_entry& x) const {
          return ptr < x.ptr || (ptr == x.ptr && action < x.action);
      }
  };

// FIXME: tzwang: why there're two versions of delete_entry
/*
  struct delete_entry {
#ifdef CHECK_INVARIANTS
    dbtuple *tuple_ahead_;
    uint64_t trigger_tid_;
#endif

    dbtuple *tuple_;
    marked_ptr<std::string> key_;
    concurrent_btree *btr_;

    delete_entry()
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(nullptr),
        trigger_tid_(0),
#endif
        tuple_(),
        key_(),
        btr_(nullptr) {}

    delete_entry(dbtuple *tuple_ahead,
                 uint64_t trigger_tid,
                 dbtuple *tuple,
                 const marked_ptr<std::string> &key,
                 concurrent_btree *btr)
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(tuple_ahead),
        trigger_tid_(trigger_tid),
#endif
        tuple_(tuple),
        key_(key),
        btr_(btr) {}

    inline dbtuple *
    tuple()
    {
      return tuple_;
    }
  };
*/

  void free_with_fn(void *p, deleter_t fn);

  template <typename T>
  static inline void
  deleter_array(void *p)
  {
    delete [] (T *) p;
  }

  template <typename T>
  inline void
  free_array(T *p)
  {
    free_with_fn(p, deleter_array<T>);
  }
};
class disabled_rcu_region {};
#endif
