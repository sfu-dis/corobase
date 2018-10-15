#pragma once
#include "../varstr.h"
#include "../txn.h"

// Decoupled Index Access (DIA)

namespace ermia {
namespace dia {

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

// Request queue (single-consumer, single producer)
class RequestQueue {
private:
  const static uint32_t kMaxSize = 32768;
  Request requests[kMaxSize];
  uint32_t start;
  uint32_t next_free_pos;

public:
  RequestQueue() : start(0), next_free_pos(0) {}
  ~RequestQueue() { start = next_free_pos = 0; }
  inline bool Enqueue(ermia::transaction *t, OrderedIndex *index, varstr *key, varstr *value, bool read) {
    uint32_t pos = volatile_read(next_free_pos);
    volatile_write(next_free_pos, (pos + 1) % kMaxSize);
    Request &req = requests[pos];
    while (volatile_read(req.transaction)) {}
  }
  inline void Dequeue() {
    uint32_t pos = volatile_read(start);
    Request &req = requests[pos];
    volatile_write(req.transaction, nullptr);
    volatile_write(start, (pos + 1) % kMaxSize);
  }
};

class IndexThread : public ermia::thread::Runner {
public:
  IndexThread() : ermia::thread::Runner(false /* asking for a logical thread */) {}
  void MyWork(char *);
};

void Initialize();

}  // namespace dia
}  // namespace ermia
