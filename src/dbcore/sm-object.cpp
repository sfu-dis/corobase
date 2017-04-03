#include "sm-chkpt.h"
#include "sm-log.h"
#include "sm-log-recover.h"
#include "sm-object.h"
#include "../tuple.h"

// Dig out the payload from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
void Object::Pin(sm_log_recover_mgr* lm) {
  uint32_t status = status_;
  if(status == kStatusMemory) {
    return;
  }

  // Try to 'lock' the status
  // TODO(tzwang): have the thread do something else while waiting?
  if(!__sync_bool_compare_and_swap(&status_, kStatusLog, kStatusLoading)) {
    while(status_ != kStatusMemory) {}
    return;
  }

  // Now we can load it from the durable log
  ALWAYS_ASSERT(clsn_.offset());
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  // Already pre-allocated space when creating the object
  dbtuple* tuple = GetTuple();
  new (tuple) dbtuple(0);  // set the correct size later

  size_t data_sz = decode_size_aligned(pdest_.size_code());
  if(where == fat_ptr::ASI_LOG) {
   ASSERT(logmgr);
    // If this is a backup server with NVRAM log buffer, then we might be
    // trying to read a tuple that appears to be available in the OID array
    // but its data is still in the buffer - not safe to dig out from the
    // log buffer as it might be receiving a new batch from the primary.
    // So spin here until the tuple is flushed from NVRAM to disk.
    // XXX(tzwang): for now we can't flush - need coordinate with backup daemon
    while(config::is_backup_srv() &&
          pdest_.offset() > logmgr->durable_flushed_lsn().offset()) {}

    // Load tuple varstr from the log
    if (lm) {
      lm->load_object((char *)tuple->get_value_start(), data_sz, pdest_);
    } else {
      logmgr->load_object((char *)tuple->get_value_start(), data_sz, pdest_);
    }
    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    ASSERT(tuple->size > 0);
    ASSERT(tuple->size < data_sz);
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr),
            tuple->size);
  } else {
    // Load tuple data form the chkpt file
    ASSERT(sm_chkpt_mgr::base_chkpt_fd);
    ALWAYS_ASSERT(pdest_.offset());
    auto n = os_pread(sm_chkpt_mgr::base_chkpt_fd, (char*)this, data_sz, pdest_.offset());
    ALWAYS_ASSERT(n == data_sz);
    ALWAYS_ASSERT(tuple->size <= data_sz - sizeof(Object) - sizeof(dbtuple));
  }
  ASSERT(clsn_.asi_type() == fat_ptr::ASI_LOG);
  ALWAYS_ASSERT(clsn_.offset());
  status_ = kStatusMemory;
}

fat_ptr Object::Create(const varstr *tuple_value, bool do_write, epoch_num epoch) {
  if(tuple_value) {
    do_write = true;
  }

  // Calculate tuple size
  const uint32_t data_sz = tuple_value ? tuple_value->size(): 0;
  size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;

  // Allocate a version
  Object *obj = new(MM::allocate(alloc_sz, epoch)) Object();
  // In case we got it from the tls reuse pool
  ASSERT(obj->GetAllocateEpoch() <= epoch - 4);
  obj->SetAllocateEpoch(epoch);

  // Tuple setup
  dbtuple* tuple = obj->GetTuple();
  new(tuple) dbtuple(data_sz);
  ASSERT(tuple->pvalue == NULL);
  tuple->pvalue = (varstr *)tuple_value;
  if(do_write) {
    tuple->do_write();
  }

  size_t size_code = encode_size_aligned(alloc_sz);
  ASSERT(size_code != INVALID_SIZE_CODE);
  return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}

