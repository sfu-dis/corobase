#include "sm-alloc.h"
#include "sm-chkpt.h"
#include "sm-log.h"
#include "sm-log-recover.h"
#include "sm-object.h"
#include "../tuple.h"

namespace ermia {

// Dig out the payload from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
void Object::Pin(bool load_from_logbuf) {
  uint32_t status = volatile_read(status_);
  if (status != kStatusStorage) {
    if (status == kStatusLoading) {
      while (volatile_read(status_) != kStatusMemory) {
      }
    }
    ALWAYS_ASSERT(volatile_read(status_) == kStatusMemory ||
                  volatile_read(status_) == kStatusDeleted);
    return;
  }

  // Try to 'lock' the status
  // TODO(tzwang): have the thread do something else while waiting?
  uint32_t val =
      __sync_val_compare_and_swap(&status_, kStatusStorage, kStatusLoading);
  if (val == kStatusMemory) {
    return;
  } else if (val == kStatusLoading) {
    while (volatile_read(status_) != kStatusMemory) {
    }
    return;
  } else {
    ASSERT(val == kStatusStorage);
    ASSERT(volatile_read(status_) == kStatusLoading);
  }

  uint32_t final_status = kStatusMemory;

  // Now we can load it from the durable log
  ALWAYS_ASSERT(pdest_.offset());
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  // Already pre-allocated space when creating the object
  dbtuple *tuple = (dbtuple *)GetPayload();
  new (tuple) dbtuple(0);  // set the correct size later

  size_t data_sz = decode_size_aligned(pdest_.size_code());
  if (where == fat_ptr::ASI_LOG) {
    ASSERT(logmgr);
    // Not safe to dig out from the log buffer as it might be receiving a
    // new batch from the primary, unless we have NVRAM as log buffer.
    // XXX(tzwang): for now we can't flush - need coordinate with backup daemon
    if (config::is_backup_srv() && !config::nvram_log_buffer) {
      while (pdest_.offset() >= logmgr->durable_flushed_lsn().offset()) {
      }
    }

    // Load tuple varstr from the log
    if (load_from_logbuf) {
      logmgr->load_object_from_logbuf((char *)tuple->get_value_start(), data_sz,
                                      pdest_);
    } else {
      logmgr->load_object((char *)tuple->get_value_start(), data_sz, pdest_);
    }
    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    // Fill in the overwritten version's pdest if needed
    if (config::is_backup_srv() && next_pdest_ == NULL_PTR) {
      next_pdest_ = ((varstr *)tuple->get_value_start())->ptr;
    }
    // Could be a delete
    ASSERT(tuple->size < data_sz);
    if (tuple->size == 0) {
      final_status = kStatusDeleted;
      ASSERT(next_pdest_.offset());
    }
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr), tuple->size);
    SetClsn(LSN::make(pdest_.offset(), 0).to_log_ptr());
    ALWAYS_ASSERT(pdest_.offset() == clsn_.offset());
  } else {
    // Load tuple data form the chkpt file
    ASSERT(sm_chkpt_mgr::base_chkpt_fd);
    ALWAYS_ASSERT(pdest_.offset());
    ASSERT(volatile_read(status_) == kStatusLoading);
    // Skip the status_ and alloc_epoch_ fields
    static const uint32_t skip = sizeof(status_) + sizeof(alloc_epoch_);
    uint32_t read_size = data_sz - skip;
    auto n = os_pread(sm_chkpt_mgr::base_chkpt_fd, (char *)this + skip,
                      read_size, pdest_.offset() + skip);
    ALWAYS_ASSERT(n == read_size);
    ASSERT(tuple->size <= read_size - sizeof(dbtuple));
    next_pdest_ = NULL_PTR;
  }
  ASSERT(clsn_.asi_type() == fat_ptr::ASI_LOG);
  ALWAYS_ASSERT(pdest_.offset());
  ALWAYS_ASSERT(clsn_.offset());
  ASSERT(volatile_read(status_) == kStatusLoading);
  SetStatus(final_status);
}

fat_ptr Object::Create(const varstr *tuple_value, bool do_write,
                       epoch_num epoch) {
  if (tuple_value) {
    do_write = true;
  }

  // Calculate tuple size
  const uint32_t data_sz = tuple_value ? tuple_value->size() : 0;
  size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;

  // Allocate a version
  Object *obj = new (MM::allocate(alloc_sz)) Object();
  // In case we got it from the tls reuse pool
  ASSERT(obj->GetAllocateEpoch() <= epoch - 4);
  obj->SetAllocateEpoch(epoch);

  // Tuple setup
  dbtuple *tuple = (dbtuple *)obj->GetPayload();
  new (tuple) dbtuple(data_sz);
  ASSERT(tuple->pvalue == NULL);
  tuple->pvalue = (varstr *)tuple_value;
  if (do_write) {
    tuple->DoWrite();
  }

  size_t size_code = encode_size_aligned(alloc_sz);
  ASSERT(size_code != INVALID_SIZE_CODE);
  return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}

// Make sure the object has a valid clsn/pdest
fat_ptr Object::GenerateClsnPtr(uint64_t clsn) {
  fat_ptr clsn_ptr = NULL_PTR;
  uint64_t tuple_off = GetPersistentAddress().offset();
  if (tuple_off == 0) {
    // Must be a delete record
    ASSERT(GetPinnedTuple()->size == 0);
    ASSERT(GetPersistentAddress() == NULL_PTR);
    tuple_off = clsn;
    clsn_ptr = LSN::make(tuple_off, 0).to_log_ptr();
    // Set pdest here which wasn't set by log_delete
    pdest_ = clsn_ptr;
  } else {
    clsn_ptr = LSN::make(tuple_off, 0).to_log_ptr();
  }
  return clsn_ptr;
}

#if defined(NOWAIT) || defined(WAITDIE)
bool Object::ReadLock(TXN::xid_context *xc) {
#ifdef WAITDIE
  ASSERT(xc);
  xc->read_req = false;
  mcs_lock::qnode mutex;

  olock_.acquire(&mutex);
  uint64_t exp = lock_.load(std::memory_order_acquire);
  bool granted = false;
  if (exp & kLockX) {
    ASSERT(owners.size() == 1);
    ASSERT((uint64_t)owners.front() == (exp & ~kLockX));

    // Can't get lock, see if allowed to wait
    bool canwait = owners.front()->owner._val > xc->owner._val;

    // Check waiters
    for (auto &w : waiters) {
      if (w->owner._val < xc->owner._val) {
        canwait = false;
        break;
      }
    }

    if (canwait) {
      xc->lock_ready = false;
      waiters.push_back(xc);
      olock_.release(&mutex);
      while (!xc->lock_ready) {}
      ALWAYS_ASSERT(lock_ > 0);
      granted = true;
    } else {
      olock_.release(&mutex);
      granted = false;
    }
  } else {
    if (exp == 0) {
      owners.push_back(xc);
      ++lock_;
      olock_.release(&mutex);
      granted = true;
    } else {
      // Readers only - see if there are writers waiting
      bool has_writer = false;
      for (auto &w : waiters) {
        if (!w->read_req) {
          has_writer = true;
          break;
        }
      }
      if (has_writer) {
        // See if allowed to wait
        bool canwait = true;
        for (auto &o : owners) {
          if (o->owner._val < xc->owner._val) {
            canwait = false;
            break;
          }
        }

        // Check waiters
        for (auto &w : waiters) {
          if (w->owner._val < xc->owner._val) {
            canwait = false;
            break;
          }
        }

        if (canwait) {
          waiters.push_back(xc);
          xc->lock_ready = false;
          olock_.release(&mutex);
          while (!xc->lock_ready) {}
          granted = true;
        } else {
          olock_.release(&mutex);
          granted = false;
        }
      } else {
        // No need to wait - all readers
        ++lock_;
        owners.push_back(xc);
        olock_.release(&mutex);
        granted = true;
      }
    }
  }
  return granted;
#elif defined NOWAIT
  ASSERT(!xc);
  while (true) {
    uint64_t exp = lock_.load(std::memory_order_acquire);
    if (exp & kLockX) {
      return false;
    } else if (lock_.compare_exchange_strong(exp, exp + 1)) {
      return true;
    }
  }
#endif
}

bool Object::WriteLock(TXN::xid_context *xc) {
#ifdef NOWAIT
  uint64_t lock = lock_.load(std::memory_order_acquire);
  if (lock == 0) {
    return lock_.compare_exchange_strong(lock, kLockX | (uint64_t)xc);
  } else if ((lock & ~kLockX) == (uint64_t)xc) {  // Already holding the lock
    return true;
  } else {
    return false;
  }
#elif defined(WAITDIE)
  uint64_t lock = lock_.load(std::memory_order_acquire);
  if ((lock & ~kLockX) == (uint64_t)xc) {  // Already holding the lock
    return true;
  }

  xc->read_req = false;
  mcs_lock::qnode mutex;
  olock_.acquire(&mutex);
  bool granted = false;

  if (owners.empty()) {
    ALWAYS_ASSERT(waiters.empty());
    // Lock can be granted
    lock_ = kLockX | (uint64_t)xc;
    owners.push_back(xc);
    olock_.release(&mutex);
    granted = true;
  } else {
    // Can't get lock immediately, see if we are allowed to wait
    bool canwait = true;
    for (auto &o : owners) {
      ASSERT(o->owner._val != xc->owner._val);
      if (o->owner._val < xc->owner._val){
        canwait = false;
        break;
      }
    }

    for (auto &w : waiters) {
      if (w->owner._val < xc->owner._val){
        canwait = false;
        break;
      }
    }

    if (canwait) {
      xc->lock_ready = false;
      bool inserted = false;
      waiters.push_back(xc);
      olock_.release(&mutex);

      // Now wait
      while (!xc->lock_ready) {}
      granted = true;
      ASSERT(lock_ == (kLockX | (uint64_t)xc));
    } else {
      olock_.release(&mutex);
      granted = false;
    }
  }
  return granted;
#endif
}

void Object::ReleaseLock(TXN::xid_context *xc) {
#if defined(WAITDIE)
  mcs_lock::qnode mutex;
  olock_.acquire(&mutex);
  uint64_t lock = lock_.load(std::memory_order_acquire);
  ALWAYS_ASSERT(lock);
  
  bool erased = false;
  for (auto it = owners.begin(); it != owners.end(); ++it) {
    if ((*it)->owner._val == xc->owner._val) {
      owners.erase(it);
      erased = true;
      break;
    }
  }
  ALWAYS_ASSERT(erased);

  if (waiters.empty()){
    if (lock & kLockX) {
      lock_ = 0;
    } else {
      --lock_;
    }
  } else {
    bool is_reader = false;
    lock_ = 0;
check_reader:
    auto &w = waiters.front();
    if (w->read_req) {
      is_reader = true;
      ++lock_;
    } else {
      lock_ = kLockX | (uint64_t)w;
      is_reader = false;
    }
    w->lock_ready = true;
    owners.push_back(w);
    waiters.pop_front();

    if (is_reader) {
      goto check_reader;
    }
  }
  olock_.release(&mutex);
#else
  uint64_t lock = lock_.load(std::memory_order_acquire);
  ALWAYS_ASSERT(lock);
  
  if (lock & kLockX) {
    ALWAYS_ASSERT((lock & ~kLockX) == (uint64_t)xc);
    lock_.store(0, std::memory_order_release);
  } else {
    lock_.fetch_sub(1, std::memory_order_release);
  }
#endif
}
#endif

}  // namespace ermia
