#include "../ermia.h"
#include "sm-dia.h"
#include "sm-coroutine.h"
#include <vector>
#include <map>

namespace ermia {
namespace dia {

std::vector<IndexThread *> index_threads;

void SendGetRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, OID *oid, rc_t *rc) {
  // FIXME(tzwang): find the right index thread using some partitioning scheme
  uint32_t worker_id = 0;
  switch (ermia::config::benchmark[0]) {
    case 'y':
      worker_id = (uint32_t)(*((uint64_t*)(*key).data()) >> 32);
      ALWAYS_ASSERT(rc->_val == RC_INVALID);
      index_threads[worker_id % index_threads.size()]->AddRequest(t, index, key, oid, Request::kTypeGet, rc);
      break;
    
    default:
      LOG(FATAL) << "Not implemented";
      break;
  }
}

void SendInsertRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, OID *oid, rc_t *rc) {
  // FIXME(tzwang): find the right index thread using some partitioning scheme
  switch (ermia::config::benchmark[0]) {
    case 'y': {
      uint32_t worker_id = (uint32_t)(*((uint64_t*)(*key).data()) >> 32);
      index_threads[worker_id%index_threads.size()]->AddRequest(t, index, key, oid, Request::kTypeInsert, rc);
      }
      break;

    default:
      LOG(FATAL) << "Not implemented";
      break;
  }
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
  request_handler();
}

void IndexThread::SerialHandler() {
  if (ermia::config::dia_req_coalesce) {
    while (true) {
      thread_local std::map<uint64_t, std::vector<int> > coalesced_requests;
      coalesced_requests.clear();
      uint32_t pos = queue.getPos();
      int dequeueSize = 0;
      // Requests coalescing
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

        uint64_t current_key = *((uint64_t*)(*req->key).data());
        if (coalesced_requests.count(current_key)) {
          coalesced_requests[current_key].push_back(i);
        } else {
          std::vector<int> offsets;
          offsets.push_back(i);
          coalesced_requests.insert(std::make_pair(current_key, offsets));
        }
        ++dequeueSize;
      }

      for (auto iter = coalesced_requests.begin(); iter!= coalesced_requests.end(); ++iter) {
        std::vector<int> offsets = iter->second;
        // issue the first request
        Request *req = queue.GetRequestByPos(pos + offsets[0], true);
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
            req->index->GetOID(*req->key, *req->rc, req->transaction->GetXIDContext(), *req->oid_ptr);
            break;
          case Request::kTypeInsert:
            if (req->index->InsertIfAbsent(req->transaction, *req->key, *req->oid_ptr)) {
              volatile_write(req->rc->_val, RC_TRUE);
            } else {
              volatile_write(req->rc->_val, RC_FALSE);
            }
            break;
          default:
            LOG(FATAL) << "Wrong request type";
        }

        // issue the subsequent requests
        for (int i = 1; i < offsets.size(); ++i) {
          Request *tmp_req = queue.GetRequestByPos(pos + offsets[i], true);
          if (!tmp_req) {
            break;
          }
          ermia::transaction *tmp_t = tmp_req->transaction;
          ALWAYS_ASSERT(tmp_t);
          ALWAYS_ASSERT(!((uint64_t)tmp_t & (1UL << 63)));  // make sure we got a ready transaction
          ALWAYS_ASSERT(tmp_req->type != Request::kTypeInvalid);
          ASSERT(tmp_req->oid_ptr);
          *tmp_req->rc = rc_t{RC_INVALID};
          switch (tmp_req->type) {
            case Request::kTypeGet:
              if (req->rc->_val == RC_TRUE) {
                *tmp_req->oid_ptr = *req->oid_ptr;
                volatile_write(tmp_req->rc->_val, RC_TRUE);
              } else if (req->rc->_val == RC_FALSE) {
                volatile_write(tmp_req->rc->_val, RC_FALSE);
              } else {
                tmp_req->index->GetOID(*tmp_req->key, *tmp_req->rc, tmp_req->transaction->GetXIDContext(), *tmp_req->oid_ptr);
              }
              break;
            case Request::kTypeInsert:
              volatile_write(tmp_req->rc->_val, RC_FALSE);
              break;
            default:
              LOG(FATAL) << "Wrong request type";
          }
        }
      }

      for (int i = 0; i < dequeueSize; ++i) {
        queue.Dequeue();
      }
    }
  } else {
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
}

void IndexThread::CoroutineHandler() {
  if (ermia::config::dia_req_coalesce) {
    while (true) {
      thread_local std::vector<ermia::dia::generator<bool> *> coroutines;
      coroutines.clear();
      thread_local std::map<uint64_t, std::vector<int> > coalesced_requests;
      coalesced_requests.clear();
      uint32_t pos = queue.getPos();
      int dequeueSize = 0;
      // Requests coalescing
      for (int i = 0; i < kBatchSize; ++i) {
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

        uint64_t current_key = *((uint64_t*)(*req->key).data());
        if (coalesced_requests.count(current_key)) {
          coalesced_requests[current_key].push_back(i);
        } else {
          std::vector<int> offsets;
          offsets.push_back(i);
          coalesced_requests.insert(std::make_pair(current_key, offsets));
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
        ++dequeueSize;
      }

      while (coroutines.size()) {
        for (auto it = coroutines.begin(); it != coroutines.end();) {
          if ((*it)->advance()){
            ++it;
          }else{
            delete (*it);
            it = coroutines.erase(it);
          }
        }
      }

      for (auto iter = coalesced_requests.begin(); iter!= coalesced_requests.end(); ++iter) {
        std::vector<int> offsets = iter->second;
        // get the first request
        Request *req = queue.GetRequestByPos(pos + offsets[0], false);
        if (!req) {
          break;
        }
        ermia::transaction *t = req->transaction;
        ALWAYS_ASSERT(t);
        ALWAYS_ASSERT(!((uint64_t)t & (1UL << 63)));  // make sure we got a ready transaction
        ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
        ASSERT(req->oid_ptr);

        // issue the subsequent requests
        for (int i = 1; i < offsets.size(); ++i){
          Request *tmp_req = queue.GetRequestByPos(pos + offsets[i], false);
          if (!tmp_req) {
            break;
          }
          ermia::transaction *tmp_t = tmp_req->transaction;
          ALWAYS_ASSERT(tmp_t);
          ALWAYS_ASSERT(!((uint64_t)tmp_t & (1UL << 63)));  // make sure we got a ready transaction
          ALWAYS_ASSERT(tmp_req->type != Request::kTypeInvalid);
          ASSERT(tmp_req->oid_ptr);
          switch (tmp_req->type) {
            case Request::kTypeGet:
              if (req->rc->_val == RC_TRUE) {
                *tmp_req->oid_ptr = *req->oid_ptr;
                volatile_write(tmp_req->rc->_val, RC_TRUE);
              } else if (req->rc->_val == RC_FALSE) {
                volatile_write(tmp_req->rc->_val, RC_FALSE);
              } else {
                tmp_req->index->GetOID(*tmp_req->key, *tmp_req->rc, tmp_req->transaction->GetXIDContext(), *tmp_req->oid_ptr);
              }
              break;
            case Request::kTypeInsert:
              volatile_write(tmp_req->rc->_val, RC_FALSE);
              break;
            default:
              LOG(FATAL) << "Wrong request type";
          }
        }
      }

      for (int i = 0; i < dequeueSize; ++i)
        queue.Dequeue();
    }
  } else {
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

      int dequeueSize = coroutines.size();
      while (coroutines.size()){
        for (auto it = coroutines.begin(); it != coroutines.end();) {
          if ((*it)->advance()){
            ++it;
          }else{
            delete (*it);
            it = coroutines.erase(it);
          }
        }
      }

      for (int i = 0; i < dequeueSize; ++i)
        queue.Dequeue();
    }
  }
}

}  // namespace dia
}  // namespace ermia
