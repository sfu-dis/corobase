#pragma once
#include <string>

#include "../dbcore/sm-oid.h"
#include "../txn.h"
#include "../varstr.h"

// Decoupled Index Access (DIA)

namespace ermia {
namespace dia {

void Initialize();
void SendGetRequest(ermia::transaction *t, OrderedIndex *index,
                    const varstr *key, OID *oid, rc_t *rc, uint32_t idx_no);
void SendInsertRequest(ermia::transaction *t, OrderedIndex *index,
                       const varstr *key, OID *oid, rc_t *rc, uint32_t idx_no);
void SendScanRequest(ermia::transaction *t, OrderedIndex *index,
                     const varstr *start_key, const varstr *end_key,
                     OID *dia_callback, rc_t *rc, uint32_t idx_no);
void SendReverseScanRequest(ermia::transaction *t, OrderedIndex *index,
                            const varstr *start_key, const varstr *end_key,
                            OID *dia_callback, rc_t *rc, uint32_t idx_no);

// Structure that represents an index access request
struct Request {
  static const uint8_t kTypeInvalid = 0x0;
  static const uint8_t kTypeGet = 0x1;
  static const uint8_t kTypeInsert = 0x2;
  static const uint8_t kTypeScan = 0x3;
  static const uint8_t kTypeReverseScan = 0x4;
  ermia::transaction *transaction;
  OrderedIndex *index;
  const varstr *key;
  const varstr *end_key;
  OID *oid_ptr; // output for Get, input for Put
  uint8_t type;
  rc_t *rc; // Return result of the index operation

  // Point read/write request
  Request(ermia::transaction *t, OrderedIndex *index, varstr *key,
          varstr *end_key, uint8_t type, OID *oid, rc_t *rc)
      : transaction(t), index(index), key(key), end_key(end_key), oid_ptr(oid),
        type(type), rc(rc) {}

  Request()
      : transaction(nullptr), index(nullptr), key(nullptr), end_key(nullptr),
        oid_ptr(nullptr), type(kTypeInvalid), rc(nullptr) {}
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

  inline Request *GetRequestByPos(uint32_t pos, bool wait = true) {
    Request *req = &requests[pos % kMaxSize];

  get_request:
    bool exists = (volatile_read(req->transaction) != nullptr);

    if (exists) {
      // Wait for the busy bit to be reset (should be very rare)
      while ((uint64_t)(volatile_read(req->transaction)) & (1UL << 63)) {
      }
      return req;
    } else {
      if (wait) {
        goto get_request;
      } else {
        return nullptr;
      }
    }
  }

  inline Request &GetNextRequest() {
    Request *req = nullptr;
    do {
      req = &requests[start];
    } while (!volatile_read(req->transaction));
    // Wait for the busy bit to be reset (shuold be very rare)
    while ((uint64_t)(volatile_read(req->transaction)) & (1UL << 63)) {
    }
    return *req;
  }

  inline void Enqueue(ermia::transaction *t, OrderedIndex *index,
                      const varstr *key, uint8_t type, rc_t *rc, OID *oid,
                      const varstr *end_key = nullptr) {
  retry:
    // tzwang: simple dumb solution; may get fancier if needed later.  First try
    // to get a possible slot in the queue, then wait for the slot to become
    // available - there might be multiple threads (very rare) that got the same
    // slot number (when number of threads > number of request slots such that
    // we may wrap around very quickly), so later we use a CAS to really claim
    // it by swapping in the transaction pointer.
    uint32_t pos =
        next_free_pos.fetch_add(1, std::memory_order_release) % kMaxSize;
    Request &req = requests[pos];
    while (volatile_read(req.transaction)) {
    }

    // We have more than the transaction to update in the slot, so mark it in
    // the MSB as 'busy'
    if (!__sync_bool_compare_and_swap((uint64_t *)&req.transaction, 0,
                                      (uint64_t)t | (1UL << 63))) {
      goto retry;
    }

    req.index = index;
    req.key = key;
    req.end_key = end_key;
    req.type = type;
    req.rc = rc;
    req.oid_ptr = oid;

    // Now toggle the busy bit so it's really ready
    COMPILER_MEMORY_FENCE;
    volatile_write(req.transaction, t);
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

  inline void MultiDequeue(uint32_t dequeue_size) {
    uint32_t pos = volatile_read(start);
    for (int i = 0; i < dequeue_size; ++i) {
      Request &req = requests[(pos + i) % kMaxSize];
      volatile_write(req.transaction, nullptr);
    }
    volatile_write(start, (pos + dequeue_size) % kMaxSize);
  }
};

// Structure that stores the result of an index access locally
struct Result {
public:
  OID oid;        // output for Get, input for Put
  bool insert_ok; // Record if we have previously done an insert for the key.
  rc_t rc;        // Return result of the index operation
  Result(OID oid = 0, bool insert_ok = false, rc_t rc = {RC_INVALID})
      : oid(oid), insert_ok(insert_ok), rc(rc) {}
};

class IndexThread : public ermia::thread::Runner {
private:
  RequestQueue queue;
  std::function<void()> request_handler;
  static const uint32_t kBatchSize = 60;

public:
  IndexThread(bool physical = false)
      : ermia::thread::Runner(physical /* default thread is logical*/) {
    if (config::dia_req_handler == "serial") {
      if (ermia::config::dia_req_coalesce)
        request_handler = std::bind(&IndexThread::SerialCoalesceHandler, this);
      else
        request_handler = std::bind(&IndexThread::SerialHandler, this);
    } else if (config::dia_req_handler == "coroutine") {
      if (ermia::config::dia_req_coalesce)
        request_handler =
            std::bind(&IndexThread::CoroutineCoalesceHandler, this);
      else
        request_handler = std::bind(&IndexThread::CoroutineHandler, this);
    } else if (config::dia_req_handler == "amac") {
      request_handler = std::bind(&IndexThread::AmacHandler, this);
    } else {
      LOG(FATAL) << "Wrong handler type: " << config::dia_req_handler;
    }
  }

  inline void AddRequest(ermia::transaction *t, OrderedIndex *index,
                         const varstr *key, const varstr *end_key, OID *oid,
                         uint8_t type, rc_t *rc) {
    queue.Enqueue(t, index, key, type, rc, oid, end_key);
  }
  void MyWork(char *);

private:
  void SerialHandler();
  void SerialCoalesceHandler();
  void CoroutineHandler();
  void CoroutineCoalesceHandler();
  void AmacHandler();
  uint32_t CoalesceRequests(
      std::unordered_map<std::string, std::vector<int>> &request_map);
};

} // namespace dia
} // namespace ermia
