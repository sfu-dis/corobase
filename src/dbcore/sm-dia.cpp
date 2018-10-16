#include "../ermia.h"
#include "sm-dia.h"

namespace ermia {
namespace dia {

std::vector<IndexThread *> index_threads;

void Request::Execute() {
}

void SendReadRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, varstr *value, OID *oid, bool *finished) {
  // FIXME(tzwang): find the right index thread using some partitioning scheme
  index_threads[0]->AddRequest(t, index, key, value, oid, true, finished);
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
    t->Start();
  }
}

// The actual index access goes here
void IndexThread::MyWork(char *) {
  LOG(INFO) << "Index thread started";
  // FIXME(tzwang): Process requests in batches
  while (true) {
    Request &req = queue.GetNextRequest();
    ermia::transaction *t = volatile_read(req.transaction);
    ALWAYS_ASSERT(t);
    ALWAYS_ASSERT(req.is_read);
    if (req.is_read) {
      req.index->DiaGet(req.transaction, *req.key, *req.value, req.oid_ptr);
      volatile_write(*req.finished, true);
    }
    queue.Dequeue();
  }
}

}  // namespace dia
}  // namespace ermia
