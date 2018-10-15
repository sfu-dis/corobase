#include "sm-dia.h"

namespace ermia {
namespace dia {

std::vector<ermia::thread::Runner *> index_threads;

void Request::Execute() {
}

// Prepare the extra index threads needed by DIA. The other compute threads
// should already be initialized by sm-config.
void Initialize() {
  LOG_IF(FATAL, thread::cpu_cores.size() == 0) << "No logical thread information available";

  // Need [config::worker_threads] number of logical threads, each corresponds to
  // to a physical worker thread
  for (uint32_t i = 0; i < ermia::config::worker_threads; ++i) {
    index_threads.emplace_back(new IndexThread());
  }

  for (auto t : index_threads) {
    while (!t->TryImpersonate()) {}
  }
}

// The actual index access goes here
void IndexThread::MyWork(char *) {
  // FIXME(tzwang): Process requests in batches
  while (true) {
    Request &req = queue.GetNextRequest();
    ermia::transaction *t = volatile_read(req.transaction);
    if (t) {
      // TODO: process it
      queue.Dequeue();
    }
  }
}

}  // namespace dia
}  // namespace ermia
