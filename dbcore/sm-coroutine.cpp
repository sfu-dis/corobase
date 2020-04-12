#include "sm-coroutine.h"

namespace ermia {
namespace dia {

thread_local tcalloc allocator;
thread_local scheduler_queue query_scheduler;

} // namespace dia
} // namespace ermia
