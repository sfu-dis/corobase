#pragma once

#include "epoch.h"
#include "sm-common.h"
#include "../varstr.h"

struct dbtuple;
class sm_log_recover_mgr;

class Object {
private:
  typedef epoch_mgr::epoch_num epoch_num;
  static const uint32_t kStatusMemory  = 0;
  static const uint32_t kStatusStorage = 1;
  static const uint32_t kStatusLoading = 2;

  // alloc_epoch_ and status_ must be the first two fields

  // When did we create this object?
  epoch_num alloc_epoch_;

  // Where exactly is the payload?
  uint32_t status_;

  // The object's permanent home in the log/chkpt
  fat_ptr pdest_;

  // The permanent home of the older version that's overwritten by me
  fat_ptr next_pdest_;

  // Volatile pointer to the next older version that's in memory.
  // There might be a gap between the versions represented by next_pdest_
  // and next_volatile_.
  fat_ptr next_volatile_;

  // Commit timestamp of this version. Type is XID (LOG) before (after)
  // commit. size_code refers to the whole object including header
  fat_ptr clsn_;

public:
  static fat_ptr Create(const varstr *tuple_value, bool do_write, epoch_num epoch);

  Object() : pdest_(NULL_PTR), next_pdest_(NULL_PTR), next_volatile_(NULL_PTR),
             clsn_(NULL_PTR), alloc_epoch_(0), status_(kStatusMemory) {}

  Object(fat_ptr pdest, fat_ptr next, epoch_num e, bool in_memory) :
    pdest_(pdest), next_pdest_(next), next_volatile_(NULL_PTR),
    clsn_(NULL_PTR), alloc_epoch_(e) {
    status_ = in_memory ? kStatusMemory : kStatusStorage;
  }

  inline bool IsInMemory() { return status_ == kStatusMemory; }
  inline fat_ptr* GetPersistentAddressPtr() { return &pdest_; }
  inline fat_ptr GetPersistentAddress() { return pdest_; }
  inline fat_ptr GetClsn() { return volatile_read(clsn_); }
  inline void SetClsn(fat_ptr clsn) { volatile_write(clsn_, clsn); }
  inline fat_ptr GetNextPersistent() { return volatile_read(next_pdest_); }
  inline fat_ptr* GetNextPersistentPtr() { return &next_pdest_; }
  inline fat_ptr GetNextVolatile() { return volatile_read(next_volatile_); }
  inline fat_ptr* GetNextVolatilePtr() { return &next_volatile_; }
  inline void SetNextPersistent(fat_ptr next) { volatile_write(next_pdest_, next); }
  inline void SetNextVolatile(fat_ptr next) { volatile_write(next_volatile_, next); }
  inline epoch_num GetAllocateEpoch() { return alloc_epoch_; }
  inline void SetAllocateEpoch(epoch_num e) { alloc_epoch_ = e; }
  inline char* GetPayload() { return (char*)((char*)this + sizeof(Object)); }
  inline void SetStatus(uint32_t s) { volatile_write(status_, s); }
  inline dbtuple* GetPinnedTuple() {
    if(!IsInMemory()) {
      Pin();
    }
    return (dbtuple*)GetPayload();
  }
  void Pin(sm_log_recover_mgr* lm = nullptr);  // Make sure the payload is in memory
};
