#include "sm-dia.h"
#include "../ermia.h"

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
      if (sync_wait_coro(req.index->InsertIfAbsent(req.transaction, *req.key, *req.oid_ptr))) {
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
              result.insert_ok = sync_wait_coro(req->index->InsertIfAbsent(
                  req->transaction, *req->key, *req->oid_ptr));
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
          result.insert_ok = sync_wait_coro(req->index->InsertIfAbsent(
              req->transaction, *req->key, *req->oid_ptr));
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

void IndexThread::CoroutineHandler() {}

void IndexThread::CoroutineCoalesceHandler() {}

void IndexThread::AmacHandler() {
  auto get_request = [this](DiaAMACState &s) {
    uint32_t pos = queue.getPos();
    Request *req = queue.GetRequestByPos(pos, false);
    if (req) {
      s.reset(req);
      queue.Dequeue();
      req = &s.req;

      ermia::transaction *t = req->transaction;
      ALWAYS_ASSERT(t);
      ALWAYS_ASSERT(
          !((uint64_t)t & (1UL << 63))); // make sure we got a ready transaction
      ALWAYS_ASSERT(req->type != Request::kTypeInvalid);
      ASSERT(req->oid_ptr);
      if (req->type == Request::kTypeInsert) {
        if (sync_wait_coro(req->index->InsertIfAbsent(req->transaction, *req->key,
                                       *req->oid_ptr))) {
          volatile_write(req->rc->_val, RC_TRUE);
        } else {
          volatile_write(req->rc->_val, RC_FALSE);
        }
        s.reset(nullptr);
      }
    }
  };

  std::vector<DiaAMACState> tls_das;
  for (int i = 0; i < kBatchSize; ++i) {
    tls_das.emplace_back(nullptr);
  }
  int match, kp;
  ermia::ConcurrentMasstree::internode_type *in = nullptr;
  key_indexed_position kx;

  while (true) {
    for (auto &s : tls_das) {
      switch (s.stage) {
      case DiaAMACState::kInvalidStage:
        get_request(s);
        break;
      case 2:
        s.lp.perm_ = s.lp.n_->permutation();
        kx = ermia::ConcurrentMasstree::leaf_type::bound_type::lower(s.lp.ka_,
                                                                     s.lp);
        if (kx.p >= 0) {
          s.lp.lv_ = s.lp.n_->lv_[kx.p];
          if (s.lp.n_->keylenx_[kx.p]) {
            s.lp.lv_.prefetch(s.lp.n_->keylenx_[kx.p]);
          }
        }
        if (s.lp.n_->has_changed(s.lp.v_)) {
          s.lp.n_ = s.lp.n_->advance_to_key(s.lp.ka_, s.lp.v_, s.ti);
          if (s.lp.v_.deleted()) {
            s.stage = 0;
            goto stage0;
          } else {
            s.lp.n_->prefetch();
            s.stage = 2;
          }
        } else {
          match = kx.p >= 0 ? s.lp.n_->ksuf_matches(kx.p, s.lp.ka_) : 0;
          if (match < 0) {
            s.lp.ka_.shift_by(-match);
            s.lp.root_ = s.lp.lv_.layer();
            s.stage = 0;
            goto stage0;
          } else {
            // Done!
            if (match) {
              *(s.req.oid_ptr) = s.lp.value();
              volatile_write(s.req.rc->_val, RC_TRUE);
            } else {
              volatile_write(s.req.rc->_val, RC_FALSE);
            }
            s.stage = DiaAMACState::kInvalidStage;
          }
        }
        break;
      case 1:
        in = (ermia::ConcurrentMasstree::internode_type *)s.ptr;
        kp = ermia::ConcurrentMasstree::internode_type ::bound_type::upper(
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
              s.lp.v_ = s.v[s.sense];
              s.lp.n_ = const_cast<ermia::ConcurrentMasstree::leaf_type *>(
                  static_cast<const ermia::ConcurrentMasstree::leaf_type *>(
                      s.n[s.sense]));
              if (s.lp.v_.deleted()) {
                s.stage = 0;
                goto stage0;
              } else {
                s.lp.n_->prefetch();
                s.stage = 2;
              }
            } else {
              in = (ermia::ConcurrentMasstree::internode_type *)s.n[s.sense];
              in->prefetch();
              s.ptr = in;
              assert(s.stage == 1);
            }
          } else {
            typename ermia::ConcurrentMasstree::node_base_type::nodeversion_type
                oldv = s.v[s.sense];
            s.v[s.sense] = in->stable_annotated(s.ti.stable_fence());
            if (oldv.has_split(s.v[s.sense]) &&
                in->stable_last_key_compare(s.lp.ka_, s.v[s.sense], s.ti) > 0) {
              s.stage = 0;
              goto stage0;
            } else {
              if (s.v[s.sense].isleaf()) {
                s.lp.v_ = s.v[s.sense];
                s.lp.n_ = const_cast<ermia::ConcurrentMasstree::leaf_type *>(
                    static_cast<const ermia::ConcurrentMasstree::leaf_type *>(
                        s.n[s.sense]));
                if (s.lp.v_.deleted()) {
                  s.stage = 0;
                  goto stage0;
                } else {
                  s.lp.n_->prefetch();
                  s.stage = 2;
                }
              } else {
                ermia::ConcurrentMasstree::internode_type *in =
                    (ermia::ConcurrentMasstree::internode_type *)s.n[s.sense];
                in->prefetch();
                s.ptr = in;
                assert(s.stage == 1);
              }
            }
          }
        }
        break;
      case 0:
      stage0:
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
          if (s.lp.v_.deleted()) {
            s.stage = 0;
            goto stage0;
          } else {
            s.lp.n_->prefetch();
            s.stage = 2;
          }
        } else {
          auto *in = (ermia::ConcurrentMasstree::internode_type *)s.n[s.sense];
          in->prefetch();
          s.ptr = in;
          s.stage = 1;
        }
        break;
      }
    }
  }
}

} // namespace dia
} // namespace ermia
