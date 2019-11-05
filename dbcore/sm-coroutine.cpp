#include "sm-coroutine.h"

namespace ermia {

namespace dia {

namespace coro_task_private{

  thread_local memory_pool *memory_pool::instance_ = nullptr;

};

};

};
