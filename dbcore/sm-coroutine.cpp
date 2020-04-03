#include "sm-coroutine.h"

namespace ermia {
namespace dia {

thread_local tcalloc allocator;
thread_local scheduler_queue query_scheduler;
thread_local intra_scheduler_queue intra_query_scheduler;

} // namespace dia
} // namespace ermia
