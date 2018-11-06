#pragma once
#include "../dbcore/sm-oid.h"
#include "../varstr.h"
#include "../txn.h"

// Decoupled Index Access (DIA)

namespace ermia {
namespace dia {

void Initialize();
void SendReadRequest(ermia::transaction *t, OrderedIndex *index,
                     const varstr *key, varstr *value,
                     OID *oid, rc_t *rc);

// Structure that represents an index access request
struct Request {
  ermia::transaction *transaction;
  OrderedIndex *index;
  const varstr *key;
  varstr *value;
  OID *oid_ptr;
  bool is_read;
  rc_t *rc;  // Return result of the index operation

  // Point read/write request
  Request(ermia::transaction *t,
          OrderedIndex *index,
          varstr *key,
          varstr *value,
          bool is_read,
          OID *oid,
          rc_t *rc)
    : transaction(t)
    , index(index)
    , key(key)
    , value(value)
    , oid_ptr(oid)
    , is_read(is_read)
    , rc(rc)
  {}

  Request()
    : transaction(nullptr)
    , index(nullptr)
    , key(nullptr)
    , value(nullptr)
    , oid_ptr(nullptr)
    , is_read(false)
    , rc(nullptr)
  {}
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
    } while (!volatile_read(req->transaction));
    // Wait for the busy bit to be reset (shuold be very rare)
    while ((uint64_t)(volatile_read(req->transaction)) & (1UL << 63)) {}
    return *req;
  }
  inline void Enqueue(ermia::transaction *t, OrderedIndex *index,
                      const varstr *key, varstr *value,
                      bool is_read, rc_t *rc) {
    // tzwang: simple dumb solution; may get fancier if needed later.
    // First try to get a possible slot in the queue, then wait for the slot to
    // become available - there might be multiple threads (very rare) that got
    // the same slot number, so use a CAS to claim it.
    uint32_t pos = next_free_pos.fetch_add(1, std::memory_order_release) % kMaxSize;
    Request &req = requests[pos];
    while (volatile_read(req.transaction)) {}

    // We have more than the transaction to update in the slot, so mark it in
    // the MSB as 'busy'
    req.transaction = (ermia::transaction *)((uint64_t)t | (1UL << 63));
    COMPILER_MEMORY_FENCE;

    req.index = index;
    req.key = key;
    req.value = value;
    req.is_read = is_read;
    req.rc = rc;

    // Now toggle the busy bit so it's really ready
    COMPILER_MEMORY_FENCE;
    uint64_t new_val = (uint64_t)req.transaction & (~(1UL << 63));
    req.transaction = (ermia::transaction *)new_val;
  }

  // Only one guy can call this
  // Note: should only call this after GetNextRequest unless the processing of a
  // queue entry (Request) isn't needed.
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

  inline void AddRequest(ermia::transaction *t, OrderedIndex *index,
                         const varstr *key, varstr *value, OID *oid,
                         bool is_read, rc_t *rc) {
    queue.Enqueue(t, index, key, value, is_read, rc);
  }
  void MyWork(char *);
};

}  // namespace dia
}  // namespace ermia
