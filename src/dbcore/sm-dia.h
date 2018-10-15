#pragma once
#include "../varstr.h"
#include "../txn.h"

// Decoupled Index Access (DIA)

namespace ermia {
namespace dia {

// Structure that represents an index access request
class Request {
private:
  ermia::transaction *transaction;
  OrderedIndex *index;
  varstr *key;
  varstr *value;
  bool read;

public:
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
  const static uint32_t kMaxSize = 10000;
  Request requests[kMaxSize];
  uint32_t size;

public:
  RequestQueue() : size(0) {}
  ~RequestQueue() { size = 0; }
  inline void Enqueue(ermia::transaction *t, OrderedIndex *index, varstr *key, varstr *value, bool read) {
  }
  inline void Dequeue() {
  }
};

// Spawn index access threads
void Initialize();

// Index thread routine
void IndexThreadTask(char *);

}  // namespace dia
}  // namespace ermia
