#include "mcs_lock.h"

void mcs_lock::spin_on_waiting(qnode* me) {
  while (me->vthis()->_waiting)
    ;
}

mcs_lock::qnode* mcs_lock::spin_on_next(qnode* me) {
  qnode* next;
  while (!(next = me->vthis()->_next))
    ;
  return next;
}
