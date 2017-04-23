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
  epoch_num alloc_epoch_;  // When did we create this object?
  uint32_t status_;  // Where exactly is the payload?
  fat_ptr pdest_; // The object's permanent home in the log/chkpt
  fat_ptr next_;  // The older version
  fat_ptr clsn_;  // size_code refers to the whole object including header

public:
  static fat_ptr Create(const varstr *tuple_value, bool do_write, epoch_num epoch);

  Object() : pdest_(NULL_PTR), next_(NULL_PTR), clsn_(NULL_PTR),
             alloc_epoch_(0), status_(kStatusMemory) {}

  Object(fat_ptr pdest, fat_ptr next, epoch_num e, bool in_memory) :
    pdest_(pdest), next_(next), clsn_(NULL_PTR), alloc_epoch_(e) {
    status_ = in_memory ? kStatusMemory : kStatusStorage;
  }

  inline bool IsInMemory() { return status_ == kStatusMemory; }
  inline fat_ptr* GetPersistentAddressPtr() { return &pdest_; }
  inline fat_ptr GetPersistentAddress() { return pdest_; }
  inline fat_ptr GetClsn() { return volatile_read(clsn_); }
  inline void SetClsn(fat_ptr clsn) { volatile_write(clsn_, clsn); }
  inline fat_ptr GetNext() { return volatile_read(next_); }
  inline fat_ptr* GetNextPtr() { return &next_; }
  inline void SetNext(fat_ptr next) { volatile_write(next_, next); }
  inline epoch_num GetAllocateEpoch() { return alloc_epoch_; }
  inline void SetAllocateEpoch(epoch_num e) { alloc_epoch_ = e; }
  inline char* GetPayload() { return (char*)((char*)this + sizeof(Object)); }
  inline dbtuple* GetPinnedTuple() {
    if(!IsInMemory()) {
      Pin();
    }
    return (dbtuple*)GetPayload();
  }
  void Pin(sm_log_recover_mgr* lm = nullptr);  // Make sure the payload is in memory
  void SetStatus(uint32_t s) { volatile_write(status_, s); }
};
