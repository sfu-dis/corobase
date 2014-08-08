#include "rcu-wrapper.h"

void
RCU::free_with_fn(void *p, deleter_t fn)
{
  // stuff the pointer to delete_entry in the size area of p
  rcu_pointer u = {p};
  // FIXME: tzwang: replace new with something better, and
  // remember to free delete_entry itself after rcu_free.
  // (--u.p)->size |= (((size_t)(new delete_entry(p, fn))) << 8);
  void *de = RCU::rcu_alloc(sizeof(delete_entry));
  new (de) delete_entry(p, fn);
  (--u.p)->size |= (((size_t)de) << 8);
  // rcu_free then should handle this delete_entry
}

