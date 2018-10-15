#pragma once
#include "../dbcore/sm-oid.h"
#include "../varstr.h"
#include "../txn.h"

// Decoupled Index Access (DIA)

namespace ermia {
namespace dia {

void Initialize();
void SendReadRequest(ermia::transaction *t, OrderedIndex *index,
                     const varstr *key, varstr *value, OID *oid);

// Structure that represents an index access request
struct Request {
  ermia::transaction *transaction;
  OrderedIndex *index;
  varstr *key;
  varstr *value;
  bool read;

  // Point read/write request
  Request(ermia::transaction *t,
          OrderedIndex *index,
          varstr *key,
          varstr *value,
          bool read)
    : transaction(t)
    , index(index)
    , key(key)
    , value(value)
    , read(read) {}

  Request()
    : transaction(nullptr)
    , index(nullptr)
    , key(nullptr)
    , value(nullptr)
    , read(false) {}
  void Execute();
};

// Request queue (multi-producer, single-consumer)
class RequestQueue {
private:
  const static uint32_t kMaxSize = 32768;
  Request requests[kMaxSize];
  uint32_t start;
  std::atomic<uint32_t> next_free_pos;

public:
  RequestQueue() : start(0), next_free_pos(0) {
    ALWAYS_ASSERT(kMaxSize >= ermia::config::worker_threads);
  }
  ~RequestQueue() { start = next_free_pos = 0; }
  inline Request &GetNextRequest() {
    Request *req = nullptr;
    do {
      req = &requests[start];
    } while (!req->transaction);
  }
  inline void Enqueue(ermia::transaction *t, OrderedIndex *index,
                      const varstr *key, varstr *value, bool is_read) {
    bool success = false;
    while (!success) {
      // tzwang: simple dumb solution; may get fancier if needed later.
      // First try to get a possible slot in the queue, then wait for the slot to
      // become available - there might be multiple threads (very rare) that got
      // the same slot number, so use a CAS to claim it.
      uint32_t pos = next_free_pos.fetch_add(1, std::memory_order_release) % kMaxSize;
      Request &req = requests[pos];
      while (volatile_read(req.transaction)) {}
      success = __sync_bool_compare_and_swap(&req.transaction, nullptr, t);
    }
  }
  // Only one guy can call this
  inline void Dequeue() {
    uint32_t pos = volatile_read(start);
    Request &req = requests[pos];
    volatile_write(req.transaction, nullptr);
    volatile_write(start, (pos + 1) % kMaxSize);
  }
};

class IndexThread : public ermia::thread::Runner {
private:
  RequestQueue queue;

public:
  IndexThread() : ermia::thread::Runner(false /* asking for a logical thread */) {}

  inline bool AddRequest(ermia::transaction *t, OrderedIndex *index,
                         const varstr *key, varstr *value, OID *oid, bool is_read) {
    queue.Enqueue(t, index, key, value, is_read);
  }
  void MyWork(char *);
};

}  // namespace dia
}  // namespace ermia
