#include "rcu-wrapper.h"
percore_lazy<int> scoped_rcu_region::_depths;

