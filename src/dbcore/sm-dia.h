#pragma once
#include "../dbcore/sm-oid.h"
#include "../varstr.h"
#include "../txn.h"

// Decoupled Index Access (DIA)

namespace ermia {
namespace dia {

void Initialize();
void SendGetRequest(ermia::transaction *t, OrderedIndex *index,
                     const varstr *key, OID *oid, rc_t *rc);
void SendInsertRequest(ermia::transaction *t, OrderedIndex *index,
                       const varstr *key, OID *oid, rc_t *rc);

// Structure that represents an index access request
struct Request {
  static const uint8_t kTypeInvalid= 0x0;
  static const uint8_t kTypeGet = 0x1;
  static const uint8_t kTypeInsert = 0x2;
  ermia::transaction *transaction;
  OrderedIndex *index;
  const varstr *key;
  OID *oid_ptr;  // output for Get, input for Put
  uint8_t type;
  rc_t *rc;  // Return result of the index operation

  // Point read/write request
  Request(ermia::transaction *t,
          OrderedIndex *index,
          varstr *key,
          uint8_t type,
          OID *oid,
          rc_t *rc)
    : transaction(t)
    , index(index)
    , key(key)
    , oid_ptr(oid)
    , type(type)
    , rc(rc)
  {}

  Request()
    : transaction(nullptr)
    , index(nullptr)
    , key(nullptr)
    , oid_ptr(nullptr)
    , type(kTypeInvalid)
    , rc(nullptr)
  {}
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

  inline uint32_t getPos() {
    uint32_t pos = volatile_read(start);
    return pos;
  }
  
  inline Request &GetRequestByPos(uint32_t pos) {
    Request *req = nullptr;
    do {
      req = &requests[pos];
    } while (!volatile_read(req->transaction));
    // Wait for the busy bit to be reset (shuold be very rare)
    while ((uint64_t)(volatile_read(req->transaction)) & (1UL << 63)) {}
    return *req;
  }

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
                      const varstr *key, uint8_t type, rc_t *rc, OID *oid) {
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
    req.type = type;
    req.rc = rc;
    req.oid_ptr = oid;

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
                         const varstr *key, OID *oid, uint8_t type, rc_t *rc) {
    queue.Enqueue(t, index, key, type, rc, oid);
  }
  void MyWork(char *);
};

}  // namespace dia
}  // namespace ermia
