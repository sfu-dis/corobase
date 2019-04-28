#include "sm-dia.h"
#include "../ermia.h"
#include "sm-coroutine.h"

namespace ermia {
namespace dia {

std::vector<IndexThread *> index_threads;

void SendGetRequest(ermia::transaction *t, OrderedIndex *index,
                    const varstr *key, OID *oid, rc_t *rc, uint32_t idx_no) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  index_threads[idx_no]->AddRequest(t, index, key, nullptr, oid,
                                    Request::kTypeGet, rc);
}

void SendInsertRequest(ermia::transaction *t, OrderedIndex *index,
                       const varstr *key, OID *oid, rc_t *rc, uint32_t idx_no) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  index_threads[idx_no]->AddRequest(t, index, key, nullptr, oid,
                                    Request::kTypeInsert, rc);
}

void SendScanRequest(ermia::transaction *t, OrderedIndex *index,
                     const varstr *start_key, const varstr *end_key,
                     OID *dia_callback, rc_t *rc, uint32_t idx_no) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  index_threads[idx_no]->AddRequest(t, index, start_key, end_key, dia_callback,
                                    Request::kTypeScan, rc);
}

void SendReverseScanRequest(ermia::transaction *t, OrderedIndex *index,
                            const varstr *start_key, const varstr *end_key,
                            OID *dia_callback, rc_t *rc, uint32_t idx_no) {
  ALWAYS_ASSERT(rc->_val == RC_INVALID);
  index_threads[idx_no]->AddRequest(t, index, start_key, end_key, dia_callback,
                                    Request::kTypeReverseScan, rc);
}

// Prepare the extra index threads needed by DIA. The other compute threads
// should already be initialized by sm-config.
void Initialize() {
  LOG_IF(FATAL, thread::cpu_cores.size() == 0)
      << "No logical thread information available";

  // Need [config::dia_logical_index_threads] number of logical threads, each
  // corresponds to to a physical worker thread
  for (uint32_t i = 0; i < ermia::config::dia_logical_index_threads; ++i) {
    index_threads.emplace_back(new IndexThread(false));
  }

  // Need [config::dia_physical_index_threads] number of physical threads, each
  // corresponds to to a physical node
  for (uint32_t i = 0; i < ermia::config::dia_physical_index_threads; ++i) {
    index_threads.emplace_back(new IndexThread(true));
  }

  for (auto t : index_threads) {
    while (!t->TryImpersonate()) {
    }
    t->Start();
  }
}

// The actual index access goes here
void IndexThread::MyWork(char *) {
  LOG(INFO) << "Index thread started";
  request_handler();
}

uint32_t IndexThread::CoalesceRequests(
    std::unordered_map<std::string, std::vector<int>> &request_map) {
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
    ALWAYS_ASSERT(
        !((uint64_t)t & (1UL << 63))); // make sure we got a ready transaction
    ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
    ASSERT(req->oid_ptr);

    std::string current_key((*req->key).data(),
                            (*req->key).data() + (*req->key).size());
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
      req.index->GetOID(*req.key, *req.rc, req.transaction->GetXIDContext(),
                        *req.oid_ptr);
      break;
    case Request::kTypeInsert:
      if (req.index->InsertIfAbsent(req.transaction, *req.key, *req.oid_ptr)) {
        volatile_write(req.rc->_val, RC_TRUE);
      } else {
        volatile_write(req.rc->_val, RC_FALSE);
      }
      break;
    case Request::kTypeScan:
      req.index->ScanOID(req.transaction, *req.key, req.end_key, *req.rc,
                         req.oid_ptr);
      break;
    case Request::kTypeReverseScan:
      req.index->ReverseScanOID(req.transaction, *req.key, req.end_key, *req.rc,
                                req.oid_ptr);
      break;
    default:
      LOG(FATAL) << "Wrong request type";
    }
    queue.Dequeue();
  }
}

void IndexThread::SerialCoalesceHandler() {
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
      ALWAYS_ASSERT(
          !((uint64_t)t & (1UL << 63))); // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);

      std::string current_key((*req->key).data(),
                              (*req->key).data() + (*req->key).size());
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
              result.insert_ok = req->index->InsertIfAbsent(
                  req->transaction, *req->key, *req->oid_ptr);
              // Now if insert_ok becomes true, then subsequent reads will
              // also succeed; otherwise, subsequent reads will automatically
              // fail without having to issue new read requests (rc will be
              // RC_FALSE).
              if (result.insert_ok) {
                result.rc._val = RC_TRUE;
                result.oid = *req->oid_ptr; // Store the OID for future reads
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
          req->index->GetOID(*req->key, result.rc,
                             req->transaction->GetXIDContext(), *req->oid_ptr);
          break;
        case Request::kTypeInsert:
          result.insert_ok = req->index->InsertIfAbsent(
              req->transaction, *req->key, *req->oid_ptr);
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

void IndexThread::CoroutineHandler() {
  while (true) {
    thread_local std::vector<ermia::dia::generator<bool> *> coroutines;
    coroutines.clear();
    uint32_t pos = queue.getPos();
    for (int i = 0; i < kBatchSize; ++i) {
      Request *req = queue.GetRequestByPos(pos + i, false);
      if (!req) {
        break;
      }
      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(
          !((uint64_t)t & (1UL << 63))); // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      *req->rc = rc_t{RC_INVALID};
      ASSERT(req->oid_ptr);

      switch (req->type) {
      // Regardless the request is for record read or update, we only need to
      // get the OID, i.e., a Get operation on the index. For updating OID, we
      // need to use the Put interface
      case Request::kTypeGet:
        coroutines.push_back(
            new ermia::dia::generator<bool>(req->index->coro_GetOID(
                *req->key, *req->rc, t->GetXIDContext(), *req->oid_ptr)));
        break;
      case Request::kTypeInsert:
        coroutines.push_back(
            new ermia::dia::generator<bool>(req->index->coro_InsertIfAbsent(
                t, *req->key, *req->rc, *req->oid_ptr)));
        break;
      default:
        LOG(FATAL) << "Wrong request type";
      }
    }

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

    queue.MultiDequeue(finished);
  }
}

void IndexThread::CoroutineCoalesceHandler() {
  while (true) {
    thread_local std::unordered_map<std::string, std::vector<int>>
        coalesced_requests;
    coalesced_requests.clear();
    thread_local std::vector<ermia::dia::generator<bool> *> coroutines;
    coroutines.clear();

    // Must store return codes locally (instead of using the first request's rc)
    // as they might get reused by the application (benchmark).
    rc_t tls_rcs[kBatchSize];
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
      ALWAYS_ASSERT(
          !((uint64_t)t & (1UL << 63))); // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);

      std::string current_key((*req->key).data(),
                              (*req->key).data() + (*req->key).size());
      if (coalesced_requests.find(current_key) != coalesced_requests.end()) {
        coalesced_requests[current_key].push_back(i);
      } else {
        std::vector<int> offsets;
        offsets.push_back(i);
        coalesced_requests.emplace(current_key, offsets);
        // Push the first request of each key to the scheduler of coroutines
        switch (req->type) {
        case Request::kTypeGet:
          coroutines.push_back(new ermia::dia::generator<bool>(
              req->index->coro_GetOID(*req->key, tls_rcs[count],
                                      t->GetXIDContext(), *req->oid_ptr)));
          break;
        case Request::kTypeInsert:
          // Need to use the request's real OID
          coroutines.push_back(
              new ermia::dia::generator<bool>(req->index->coro_InsertIfAbsent(
                  t, *req->key, tls_rcs[count], *req->oid_ptr)));
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
    for (auto iter = coalesced_requests.begin();
         iter != coalesced_requests.end(); ++iter) {
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
      for (int i = 1; i < offsets.size(); ++i) {
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
              insert_ok = req->index->InsertIfAbsent(req->transaction,
                                                     *req->key, *req->oid_ptr);
              // Now if insert_ok becomes true, then subsequent reads will
              // also succeed; otherwise, subsequent reads will automatically
              // fail without having to issue new read requests (rc will be
              // RC_FALSE).
              if (insert_ok) {
                tls_rcs[count]._val = RC_TRUE;
                oid = *req->oid_ptr; // Store the OID for future reads
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

void IndexThread::AmacHandler() {
  std::vector<DiaAMACState> tls_das;
  for (int i = 0; i < kBatchSize; ++i) {
    tls_das.emplace_back(nullptr);
  }

  auto get_request = [this](DiaAMACState &s) {
    uint32_t pos = queue.getPos();
    Request *req = queue.GetRequestByPos(pos, false);
    if (req) {
      s.reset(req);
      queue.Dequeue();
      req = &s.req;

      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(!((uint64_t)t &
                      (1UL << 63))); // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);
      if (req->type == Request::kTypeInsert) {
        if (req->index->InsertIfAbsent(req->transaction, *req->key,
                                       *req->oid_ptr)) {
          volatile_write(req->rc->_val, RC_TRUE);
        } else {
          volatile_write(req->rc->_val, RC_FALSE);
        }
        s.reset(nullptr);
      }
    }
  };

  while (true) {
    for (auto &s : tls_das) {
      if (s.done) {
        get_request(s);
        if (s.done) {
          continue;
        }
      }
    retry:
      if (s.stage == 3) {
      stage3:
        s.lp.perm_ = s.lp.n_->permutation();
        s.kx = ermia::ConcurrentMasstree::leaf_type::bound_type::lower(s.lp.ka_,
                                                                       s.lp);
        int match;
        if (s.kx.p >= 0) {
          s.lp.lv_ = s.lp.n_->lv_[s.kx.p];
          s.lp.lv_.prefetch(s.lp.n_->keylenx_[s.kx.p]);
          match = s.lp.n_->ksuf_matches(s.kx.p, s.lp.ka_);
        } else {
          match = 0;
        }
        if (s.lp.n_->has_changed(s.lp.v_)) {
          s.lp.n_ = s.lp.n_->advance_to_key(s.lp.ka_, s.lp.v_, s.ti);
          // go to stage 2
          if (s.lp.v_.deleted()) {
            s.stage = 0;
            goto stage0;
          } else {
            // Prefetch the node
            s.lp.n_->prefetch();
            s.stage = 3;
          }
        } else {
          if (match < 0) {
            s.lp.ka_.shift_by(-match);
            s.lp.root_ = s.lp.lv_.layer();
            s.stage = 0;
            goto stage0;
          } else {
            // Done!
            s.found = match;
            if (s.found) {
              *(s.req.oid_ptr) = s.lp.value();
              volatile_write(s.req.rc->_val, RC_TRUE);
            } else {
              volatile_write(s.req.rc->_val, RC_FALSE);
            }
            s.done = true;
            get_request(s);
            if (!s.done) {
              goto stage0;
            }
          }
        }
      } else if (s.stage == 2) {
      stage2:
        // Continue after reach_leaf in find_unlocked
        if (s.lp.v_.deleted()) {
          s.stage = 0;
          goto stage0;
        } else {
          // Prefetch the node
          s.lp.n_->prefetch();
          s.stage = 3;
        }
      } else if (s.stage == 1) {
      stage1:
        ermia::ConcurrentMasstree::internode_type *in =
            (ermia::ConcurrentMasstree::internode_type *)s.ptr;
        int kp = ermia::ConcurrentMasstree::internode_type::bound_type::upper(
            s.lp.ka_, *in);
        s.n[!s.sense] = in->child_[kp];
        if (!s.n[!s.sense]) {
          s.stage = 0;
          goto stage0;
        } else {
          s.v[!s.sense] = s.n[!s.sense]->stable_annotated(s.ti.stable_fence());
          if (likely(!in->has_changed(s.v[s.sense]))) {
            s.sense = !s.sense;
            if (s.v[s.sense].isleaf()) {
              // Done reach_leaf, enter stage 2
              s.lp.v_ = s.v[s.sense];
              s.lp.n_ = const_cast<ermia::ConcurrentMasstree::leaf_type *>(
                  static_cast<const ermia::ConcurrentMasstree::leaf_type *>(
                      s.n[s.sense]));
              s.stage = 2;
              goto stage2;
              // continue;
            } else {
              // Prepare the next node to prefetch
              in = (ermia::ConcurrentMasstree::internode_type *)s.n[s.sense];
              in->prefetch();
              s.ptr = in;
              assert(s.stage == 1);
            }
          } else {
            typename ermia::ConcurrentMasstree::nodeversion_type oldv =
                s.v[s.sense];
            s.v[s.sense] = in->stable_annotated(s.ti.stable_fence());
            if (oldv.has_split(s.v[s.sense]) &&
                in->stable_last_key_compare(s.lp.ka_, s.v[s.sense], s.ti) > 0) {
              s.stage = 0;
              goto stage0;
            } else {
              if (s.v[s.sense].isleaf()) {
                // Done reach_leaf, enter stage 2
                s.lp.v_ = s.v[s.sense];
                s.lp.n_ = const_cast<ermia::ConcurrentMasstree::leaf_type *>(
                    static_cast<const ermia::ConcurrentMasstree::leaf_type *>(
                        s.n[s.sense]));
                s.stage = 2;
                goto stage2;
                // continue;
              } else {
                // Prepare the next node to prefetch
                ermia::ConcurrentMasstree::internode_type *in =
                    (ermia::ConcurrentMasstree::internode_type *)s.n[s.sense];
                in->prefetch();
                s.ptr = in;
                assert(s.stage == 1);
              }
            }
          }
        }
      } else if (s.stage == 0) {
      stage0:
        // Stage 0 - get and prefetch root
        s.req.transaction->ensure_active();
        new (&s.lp) ermia::ConcurrentMasstree::unlocked_tcursor_type(
            *(ConcurrentMasstree::basic_table_type *)(s.req.index->GetTable()),
            s.req.key->data(), s.req.key->size());
        s.sense = false;
        s.n[s.sense] = s.lp.root_;
        while (1) {
          s.v[s.sense] = s.n[s.sense]->stable_annotated(s.ti.stable_fence());
          if (!s.v[s.sense].has_split())
            break;
          s.n[s.sense] = s.n[s.sense]->unsplit_ancestor();
        }

        if (s.v[s.sense].isleaf()) {
          s.lp.v_ = s.v[s.sense];
          s.lp.n_ = const_cast<ermia::ConcurrentMasstree::leaf_type *>(
              static_cast<const ermia::ConcurrentMasstree::leaf_type *>(
                  s.n[s.sense]));
          s.stage = 2;
          // continue;
          goto stage2;
        } else {
          auto *in = (ermia::ConcurrentMasstree::internode_type *)s.n[s.sense];
          in->prefetch();
          s.ptr = in;
          s.stage = 1;
        }
      }
    }
  }
}

} // namespace dia
} // namespace ermia
