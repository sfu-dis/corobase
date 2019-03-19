#include "../ermia.h"
#include "sm-dia.h"
#include "sm-coroutine.h"

namespace ermia {
namespace dia {

std::vector<IndexThread *> index_threads;
std::function<uint32_t(const varstr *)> routing;

void SendGetRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, OID *oid, rc_t *rc) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  uint32_t index_thread_id = routing(key);
  index_threads[index_thread_id]->AddRequest(t, index, key, oid, Request::kTypeGet, rc);

}

void SendInsertRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, OID *oid, rc_t *rc) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  uint32_t index_thread_id = routing(key);
  index_threads[index_thread_id]->AddRequest(t, index, key, oid, Request::kTypeInsert, rc);
}

void SendScanRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, OID *oids, rc_t *rc) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  uint32_t index_thread_id = routing(key);
  index_threads[index_thread_id]->AddRequest(t, index, key, oids, Request::kTypeScan, rc);
}

uint32_t RoutingYcsb(const varstr *key) {
  uint32_t worker_id = (uint32_t)(*((uint64_t*)(*key).data()) >> 32);
  return worker_id % index_threads.size();
}

uint32_t RoutingTpcc(const varstr *key) {
  return 0;
}

// Prepare the extra index threads needed by DIA. The other compute threads
// should already be initialized by sm-config.
void Initialize() {
  if (config::benchmark == "ycsb") {
    routing = std::bind(RoutingYcsb, std::placeholders::_1);
  } else if (config::benchmark == "tpcc") {
    routing = std::bind(RoutingTpcc, std::placeholders::_1);
  } else {
    LOG(FATAL) << "Wrong routing type";
  }

  LOG_IF(FATAL, thread::cpu_cores.size() == 0) << "No logical thread information available";

  // Need [config::dia_logical_index_threads] number of logical threads, each corresponds to
  // to a physical worker thread
  for (uint32_t i = 0; i < ermia::config::dia_logical_index_threads; ++i) {
    index_threads.emplace_back(new IndexThread(false));
  }

  // Need [config::dia_physical_index_threads] number of physical threads, each corresponds to
  // to a physical node
  for (uint32_t i = 0; i < ermia::config::dia_physical_index_threads; ++i) {
    index_threads.emplace_back(new IndexThread(true));
  }

  for (auto t : index_threads) {
    while (!t->TryImpersonate()) {}
    t->Start();
  }
}

// The actual index access goes here
void IndexThread::MyWork(char *) {
  LOG(INFO) << "Index thread started";
  request_handler();
}

uint32_t IndexThread::CoalesceRequests(std::unordered_map<std::string, std::vector<int> > &request_map) {
  uint32_t pos = queue.getPos();
  uint32_t nrequests = 0;

  // Group requests by key
  for (int i = 0; i < kBatchSize; ++i) {
    Request *req = queue.GetRequestByPos(pos + i, false);
    if (!req) {
      break;
    }
    ermia::transaction *t = req->transaction;
    ALWAYS_ASSERT(t);
    ALWAYS_ASSERT(!((uint64_t)t & (1UL << 63)));  // make sure we got a ready transaction
    ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
    ASSERT(req->oid_ptr);

    std::string current_key((*req->key).data(), (*req->key).data() + (*req->key).size());
    if (request_map.find(current_key) != request_map.end()) {
      request_map[current_key].push_back(i);
    } else {
      std::vector<int> offsets;
      offsets.push_back(i);
      request_map.emplace(current_key, offsets);
    }
    ++nrequests;
  }
  return nrequests;
}

void IndexThread::SerialHandler() {
  while (true) {
    Request &req = queue.GetNextRequest();
    ermia::transaction *t = volatile_read(req.transaction);
    ALWAYS_ASSERT(t);
    ALWAYS_ASSERT(req.type != Request::kTypeInvalid);
    *req.rc = rc_t{RC_INVALID};
    ASSERT(req.oid_ptr);
    switch (req.type) {
      // Regardless the request is for record read or update, we only need to get
      // the OID, i.e., a Get operation on the index. For updating OID, we need
      // to use the Put interface
      case Request::kTypeGet:
        req.index->GetOID(*req.key, *req.rc, req.transaction->GetXIDContext(), *req.oid_ptr);
        break;
      case Request::kTypeInsert:
        if (req.index->InsertIfAbsent(req.transaction, *req.key, *req.oid_ptr)) {
          volatile_write(req.rc->_val, RC_TRUE);
        } else {
          volatile_write(req.rc->_val, RC_FALSE);
        }
        break;
      default:
        LOG(FATAL) << "Wrong request type";
    }
    queue.Dequeue();
  }
}

void IndexThread::OnepassSerialCoalesceHandler() {
  while (true) {
    thread_local std::unordered_map<std::string, Result> tls_results;
    tls_results.clear();

    uint32_t pos = queue.getPos();
    uint32_t dequeue_size = 0;
    // Group requests by key
    for (int i = 0; i < kBatchSize; ++i) {
      Request *req = queue.GetRequestByPos(pos + i, false);
      if (!req) {
        break;
      }
      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(!((uint64_t)t & (1UL << 63)));  // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);

      std::string current_key((*req->key).data(), (*req->key).data() + (*req->key).size());
      if (tls_results.find(current_key) != tls_results.end()) {
        Result result = tls_results[current_key];
        switch (req->type) {
          case Request::kTypeGet:
            // If we have already a successful insert, rc would be true ->
            // fill in rc directly.
            // If we have already a successful read, rc would be true -> fill
            // in rc directly.
            ALWAYS_ASSERT(result.rc._val != RC_INVALID);
            volatile_write(*req->oid_ptr, result.oid);
            volatile_write(req->rc->_val, result.rc._val);
            break;
          case Request::kTypeInsert:
            if (result.insert_ok) {
              // Already inserted, fail this request
              volatile_write(req->rc->_val, RC_FALSE);
            } else {
              // Either we haven't done any insert or a previous insert failed.
              if (result.rc._val == RC_TRUE) {
                // No insert before and previous reads succeeded: fail this
                // insert
                volatile_write(req->rc->_val, RC_FALSE);
              } else {
                // Previous insert failed or previous reads returned false, do
                // it again (serially)
                result.insert_ok = req->index->InsertIfAbsent(req->transaction, *req->key, *req->oid_ptr);
                // Now if insert_ok becomes true, then subsequent reads will
                // also succeed; otherwise, subsequent reads will automatically
                // fail without having to issue new read requests (rc will be
                // RC_FALSE).
                if (result.insert_ok) {
                  result.rc._val = RC_TRUE;
                  result.oid = *req->oid_ptr;  // Store the OID for future reads
                }
                volatile_write(req->rc->_val, result.rc._val);
                tls_results[current_key] = result;
              }
            }
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }
      } else {
        Result result;
        switch (req->type) {
          case Request::kTypeGet:
            req->index->GetOID(*req->key, result.rc, req->transaction->GetXIDContext(), *req->oid_ptr);
            break;
          case Request::kTypeInsert:
            result.insert_ok = req->index->InsertIfAbsent(req->transaction, *req->key, *req->oid_ptr);
            if (result.insert_ok)
              result.rc._val = RC_TRUE;
            else
              result.rc._val = RC_FALSE;
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }
        if (result.rc._val == RC_TRUE)
          result.oid = *req->oid_ptr;
        volatile_write(req->rc->_val, result.rc._val);
        tls_results.emplace(current_key, result);
      }
      ++dequeue_size;
    }
    queue.MultiDequeue(dequeue_size);
  }
}

void IndexThread::TwopassSerialCoalesceHandler() {
  while (true) {
    thread_local std::unordered_map<std::string, std::vector<int> > coalesced_requests;
    coalesced_requests.clear();
    int dequeue_size = CoalesceRequests(coalesced_requests);

    // Handle requests for each key
    for (auto iter = coalesced_requests.begin(); iter!= coalesced_requests.end(); ++iter) {
      std::vector<int> &offsets = iter->second;

      // Must store results locally (instead of using the first request's rc
      // and oid as they might get reused by the application (benchmark).
      ermia::OID oid = 0;
      rc_t rc = {RC_INVALID};

      // Record if we have previously done an insert for the key. If we have
      // insert_ok == true then that means subsequent reads will always succeed
      // automatically. This can save us future calls into the index for reads.
      //
      // Note: here we don't deal with deletes which is handled
      // by the upper layer version chain traversal ops done after index ops.
      bool insert_ok = false;

      // Handle each request for the same key - for the first one we issue a
      // request, the latter ones will use the previous result; in case of the
      // read-insert-read pattern, we issue the insert when we see it.
      uint32_t pos = queue.getPos();
      for (int i = 0; i < offsets.size(); ++i) {
        Request *req = queue.GetRequestByPos(pos + offsets[i], true);
        ALWAYS_ASSERT(req);
        ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
        ASSERT(req->oid_ptr);
        *req->rc = rc_t{RC_INVALID};

        switch (req->type) {
          case Request::kTypeGet:
            if (insert_ok || rc._val != RC_INVALID) {
              // Two cases here that allow us to fill in the results directly:
              // 1. Previously inserted the key
              // 2. Previously read this key
              ASSERT((!insert_ok && rc._val != RC_INVALID) || (insert_ok && oid > 0 && rc._val == RC_TRUE));
            } else {
              // Haven't done any insert or read
              req->index->GetOID(*req->key, rc, req->transaction->GetXIDContext(), oid);
              // Now subsequent reads (before any insert) will use the result
              // here, and if there is an latter insert it will automatically
              // fail
            }

            // Fill in results
            ALWAYS_ASSERT(rc._val != RC_INVALID);
            volatile_write(*req->oid_ptr, oid);
            volatile_write(req->rc->_val, rc._val);
            break;

          case Request::kTypeInsert:
            if (insert_ok) {
              volatile_write(req->rc->_val, RC_FALSE);
              ASSERT(rc._val == RC_TRUE);
            } else {
              // Either we haven't done any insert or a previous insert failed.
              if (rc._val == RC_TRUE) {
                // No insert before and previous reads succeeded: fail this
                // insert
                volatile_write(req->rc->_val, RC_FALSE);
              } else {
                // Previous insert failed or previous reads returned false
                insert_ok = req->index->InsertIfAbsent(req->transaction, *req->key, *req->oid_ptr);
                // Now if insert_ok becomes true, then subsequent reads will
                // also succeed; otherwise, subsequent reads will automatically
                // fail without having to issue new read requests (rc will be
                // RC_FALSE).
                if (insert_ok) {
                  rc._val = RC_TRUE;
                  oid = *req->oid_ptr;  // Store the OID for future reads
                } else {
                  rc._val = RC_FALSE;
                }
                volatile_write(req->rc->_val, rc._val);
              }
            }
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }
      }
    }

    queue.MultiDequeue(dequeue_size);
  }
}

void IndexThread::CoroutineHandler() {
  while (true) {
    thread_local std::vector<ermia::dia::generator<bool> *> coroutines;
    coroutines.clear();
    uint32_t pos = queue.getPos();
    for (int i = 0; i < kBatchSize; ++i){
      Request *req = queue.GetRequestByPos(pos + i, false);
      if (!req) {
        break;
      }
      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(!((uint64_t)t & (1UL << 63)));  // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      *req->rc = rc_t{RC_INVALID};
      ASSERT(req->oid_ptr);

      switch (req->type) {
        // Regardless the request is for record read or update, we only need to get
        // the OID, i.e., a Get operation on the index. For updating OID, we need
        // to use the Put interface
        case Request::kTypeGet:
          coroutines.push_back(new ermia::dia::generator<bool>(req->index->coro_GetOID(*req->key, *req->rc, t->GetXIDContext(), *req->oid_ptr)));
          break;
        case Request::kTypeInsert:
          coroutines.push_back(new ermia::dia::generator<bool>(req->index->coro_InsertIfAbsent(t, *req->key, *req->rc, *req->oid_ptr)));
          break;
        default:
          LOG(FATAL) << "Wrong request type";
      }
    }

    // Issued the requests in the scheduler
    uint32_t finished = 0;
    for (auto &c : coroutines) {
      while (c->advance()) { }
      delete c;
      ++finished;
    }

    queue.MultiDequeue(finished);
  }
}

void IndexThread::OnepassCoroutineCoalesceHandler() {
  while (true) {
    thread_local std::unordered_map<std::string, std::vector<int>> coalesced_requests;
    coalesced_requests.clear();
    thread_local std::vector<ermia::dia::generator<bool> *> coroutines;
    coroutines.clear();

    // Must store return codes locally (instead of using the first request's rc)
    // as they might get reused by the application (benchmark).
    thread_local rc_t tls_rcs[kBatchSize];
    uint32_t pos = queue.getPos();

    uint32_t count = 0;
    uint32_t dequeue_size = 0;
    // Group requests by key
    for (int i = 0; i < kBatchSize; ++i) {
      Request *req = queue.GetRequestByPos(pos + i, false);
      if (!req) {
        break;
      }
      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(!((uint64_t)t & (1UL << 63)));  // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);

      std::string current_key((*req->key).data(), (*req->key).data() + (*req->key).size());
      if (coalesced_requests.find(current_key) != coalesced_requests.end()) {
        coalesced_requests[current_key].push_back(i);
      } else {
        std::vector<int> offsets;
        offsets.push_back(i);
        coalesced_requests.emplace(current_key, offsets);
        // Push the first request of each key to the scheduler of coroutines
        switch (req->type) {
          case Request::kTypeGet:
            coroutines.push_back(new ermia::dia::generator<bool>(req->index->coro_GetOID(*req->key, tls_rcs[count], t->GetXIDContext(), *req->oid_ptr)));
            break;
          case Request::kTypeInsert:
            // Need to use the request's real OID
            coroutines.push_back(new ermia::dia::generator<bool>(req->index->coro_InsertIfAbsent(t, *req->key, tls_rcs[count], *req->oid_ptr)));
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }
        ++count;
      }
      ++dequeue_size;
    }

    memset(tls_rcs, 0, sizeof(rc_t) * count); // #define RC_INVALID 0x0
    // Issued the requests in the scheduler
    uint32_t finished = 0;
    while (finished < coroutines.size()) {
      for (auto &c : coroutines) {
        if (c && !c->advance()) {
          delete c;
          c = nullptr;
          ++finished;
        }
      }
    }

    // Done with index accesses, now fill in the results
    count = 0;
    for (auto iter = coalesced_requests.begin(); iter!= coalesced_requests.end(); ++iter) {
      std::vector<int> &offsets = iter->second;

      // Handle the first request first
      Request *req = queue.GetRequestByPos(pos + offsets[0], true);
      ALWAYS_ASSERT(req);
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);

      ermia::OID oid = *req->oid_ptr;
      bool insert_ok = false;
      if (req->type == Request::kTypeInsert) {
        insert_ok = (tls_rcs[count]._val == RC_TRUE);
      } else {
        ALWAYS_ASSERT(req->type == Request::kTypeGet);
      }
      volatile_write(req->rc->_val, tls_rcs[count]._val);

      // Handle the rest requests
      for (int i = 1; i < offsets.size(); ++i){
        req = queue.GetRequestByPos(pos + offsets[i]);
        switch (req->type) {
          case Request::kTypeGet:
            // If we have already a successful insert, rc would be true ->
            // fill in rc directly.
            // If we have already a successful read, rc would be true -> fill
            // in rc directly.
            ALWAYS_ASSERT(tls_rcs[count]._val != RC_INVALID);
            volatile_write(*req->oid_ptr, oid);
            volatile_write(req->rc->_val, tls_rcs[count]._val);
            break;
          case Request::kTypeInsert:
            if (insert_ok) {
              // Already inserted, fail this request
              volatile_write(req->rc->_val, RC_FALSE);
            } else {
              // Either we haven't done any insert or a previous insert failed.
              if (tls_rcs[i]._val == RC_TRUE) {
                // No insert before and previous reads succeeded: fail this
                // insert
                volatile_write(req->rc->_val, RC_FALSE);
              } else {
                // Previous insert failed or previous reads returned false, do
                // it again (serially)
                insert_ok = req->index->InsertIfAbsent(req->transaction, *req->key, *req->oid_ptr);
                // Now if insert_ok becomes true, then subsequent reads will
                // also succeed; otherwise, subsequent reads will automatically
                // fail without having to issue new read requests (rc will be
                // RC_FALSE).
                if (insert_ok) {
                  tls_rcs[count]._val = RC_TRUE;
                  oid = *req->oid_ptr;  // Store the OID for future reads
                }
                volatile_write(req->rc->_val, tls_rcs[count]._val);
              }
            }
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }
      }
      ++count;
    }

    queue.MultiDequeue(dequeue_size);
  }
}

void IndexThread::TwopassCoroutineCoalesceHandler() {
  while (true) {
    thread_local std::unordered_map<std::string, std::vector<int>> coalesced_requests;
    coalesced_requests.clear();
    uint32_t dequeue_size = CoalesceRequests(coalesced_requests);

    // Must store return codes locally (instead of using the first request's rc)
    // as they might get reused by the application (benchmark).
    thread_local rc_t tls_rcs[kBatchSize];

    // Push the first request of each key to the scheduler of coroutines
    uint32_t pos = queue.getPos();
    thread_local std::vector<ermia::dia::generator<bool> *> coroutines;
    coroutines.clear();

    uint32_t count = 0;
    for (auto &r : coalesced_requests) {
      Request *req = queue.GetRequestByPos(pos + r.second[0], true);
      ALWAYS_ASSERT(req);
      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(!((uint64_t)t & (1UL << 63)));  // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);

      switch (req->type) {
        case Request::kTypeGet:
          coroutines.push_back(new ermia::dia::generator<bool>(req->index->coro_GetOID(*req->key, tls_rcs[count], t->GetXIDContext(), *req->oid_ptr)));
          break;
        case Request::kTypeInsert:
          // Need to use the request's real OID
          coroutines.push_back(new ermia::dia::generator<bool>(req->index->coro_InsertIfAbsent(t, *req->key, tls_rcs[count], *req->oid_ptr)));
          break;
        default:
          LOG(FATAL) << "Wrong request type";
      }
      ++count;
    }

    memset(tls_rcs, 0, sizeof(rc_t) * count); // #define RC_INVALID 0x0
    // Issued the requests in the scheduler
    uint32_t finished = 0;
    while (finished < coroutines.size()) {
      for (auto &c : coroutines) {
        if (c && !c->advance()) {
          delete c;
          c = nullptr;
          ++finished;
        }
      }
    }

    // Done with index accesses, now fill in the results
    count = 0;
    for (auto iter = coalesced_requests.begin(); iter!= coalesced_requests.end(); ++iter) {
      std::vector<int> &offsets = iter->second;

      // Handle the first request first
      Request *req = queue.GetRequestByPos(pos + offsets[0], true);
      ALWAYS_ASSERT(req);
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);

      ermia::OID oid = *req->oid_ptr;
      bool insert_ok = false;
      if (req->type == Request::kTypeInsert) {
        insert_ok = (tls_rcs[count]._val == RC_TRUE);
      } else {
        ALWAYS_ASSERT(req->type == Request::kTypeGet);
      }
      volatile_write(req->rc->_val, tls_rcs[count]._val);

      // Handle the rest requests
      for (int i = 1; i < offsets.size(); ++i){
        req = queue.GetRequestByPos(pos + offsets[i]);
        switch (req->type) {
          case Request::kTypeGet:
            // If we have already a successful insert, rc would be true ->
            // fill in rc directly.
            // If we have already a successful read, rc would be true -> fill
            // in rc directly.
            ALWAYS_ASSERT(tls_rcs[count]._val != RC_INVALID);
            volatile_write(*req->oid_ptr, oid);
            volatile_write(req->rc->_val, tls_rcs[count]._val);
            break;
          case Request::kTypeInsert:
            if (insert_ok) {
              // Already inserted, fail this request
              volatile_write(req->rc->_val, RC_FALSE);
            } else {
              // Either we haven't done any insert or a previous insert failed.
              if (tls_rcs[i]._val == RC_TRUE) {
                // No insert before and previous reads succeeded: fail this
                // insert
                volatile_write(req->rc->_val, RC_FALSE);
              } else {
                // Previous insert failed or previous reads returned false, do
                // it again (serially)
                insert_ok = req->index->InsertIfAbsent(req->transaction, *req->key, *req->oid_ptr);
                // Now if insert_ok becomes true, then subsequent reads will
                // also succeed; otherwise, subsequent reads will automatically
                // fail without having to issue new read requests (rc will be
                // RC_FALSE).
                if (insert_ok) {
                  tls_rcs[count]._val = RC_TRUE;
                  oid = *req->oid_ptr;  // Store the OID for future reads
                }
                volatile_write(req->rc->_val, tls_rcs[count]._val);
              }
            }
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }
      }
      ++count;
    }

    queue.MultiDequeue(dequeue_size);
  }
}

}  // namespace dia
}  // namespace ermia
