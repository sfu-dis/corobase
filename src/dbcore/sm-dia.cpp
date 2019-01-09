#include <map>
#include "../ermia.h"
#include "sm-dia.h"
#include "sm-coroutine.h"

namespace ermia {
namespace dia {

std::vector<IndexThread *> index_threads;

void SendGetRequest(ermia::transaction *t, OrderedIndex *index, const varstr *key, OID *oid, rc_t *rc) {
  // FIXME(tzwang): find the right index thread using some partitioning scheme
  switch (ermia::config::benchmark[0]) {
    case 'y': {
      uint32_t worker_id = (uint32_t)(*((uint64_t*)(*key).data()) >> 32);
      index_threads[worker_id % index_threads.size()]->AddRequest(t, index, key, oid, Request::kTypeGet, rc);
      }
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
  // FIXME(tzwang): Process requests in batches

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

/*
  while (true) {
    uint32_t pos = queue.getPos();
    std::map<int, ermia::dia::generator<bool>> coro_map;
    for (int i = 0; i < 50; i++){
      Request &req = queue.GetRequestByPos(pos);
      ermia::transaction *t = volatile_read(req.transaction);
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(req.type != Request::kTypeInvalid);
      *req.rc = rc_t{RC_INVALID};
      ASSERT(req.oid_ptr);
      switch (req.type) {
        // Regardless the request is for record read or update, we only need to get
        // the OID, i.e., a Get operation on the index. For updating OID, we need
        // to use the Put interface
	case Request::kTypeGet:{
          ermia::dia::generator<bool> cG = req.index->coro_GetOID(*req.key, *req.rc, req.transaction->GetXIDContext(), *req.oid_ptr);
          coro_map.insert(std::pair<int, ermia::dia::generator<bool>>(i, cG));
          }
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
      pos = (pos + 1) % 32768;  
    }
    
    std::map<int, ermia::dia::generator<bool>>::iterator it;
    while (true) {
      for(it=coro_map.begin();it!= coro_map.end();) {  
        if(!(it->second.advance()))
          it=coro_map.erase(it);
        else
          ++it;
      }
      if(coro_map.empty())
        break;  
    }
   
    for (int i = 0; i < 50; i++)
      queue.Dequeue();
  }
*/
}


}  // namespace dia
}  // namespace ermia
