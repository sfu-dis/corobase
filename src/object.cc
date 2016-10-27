#include "object.h"
#include "tuple.h"
#include "dbcore/sm-chkpt.h"
#include "dbcore/sm-log-recover.h"
#include "dbcore/sm-log.h"

fat_ptr
object::create_tuple_object(const varstr *tuple_value, bool do_write, epoch_num epoch)
{
    if (tuple_value)
        do_write = true;
    // Calculate Tuple Size
    const uint32_t data_sz = tuple_value ? tuple_value->size(): 0;
    size_t alloc_sz = sizeof(dbtuple) + sizeof(object) + data_sz;

    // Allocate a version
    object *obj = new (MM::allocate(alloc_sz, epoch)) object();
    // In case we got it from the tls reuse pool
    ASSERT(obj->_alloc_epoch <= epoch - 4);
    obj->_alloc_epoch = epoch;

    // Tuple setup
    dbtuple* tuple = obj->tuple();
    new (tuple) dbtuple(data_sz);
    ASSERT(tuple->pvalue == NULL);
    tuple->pvalue = (varstr *)tuple_value;
    if (do_write)
        tuple->do_write();

    size_t size_code = encode_size_aligned(alloc_sz);
    ASSERT(size_code != INVALID_SIZE_CODE);
    return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}

// Dig out a tuple from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
fat_ptr object::load_durable_object(
  fat_ptr ptr, fat_ptr nxt, epoch_num epoch, object* obj, sm_log_recover_mgr *lm) {
  uint16_t asi_type = ptr.asi_type();
  ASSERT(asi_type == fat_ptr::ASI_LOG || asi_type == fat_ptr::ASI_CHK);
  size_t data_sz = decode_size_aligned(ptr.size_code());
  size_t sz = data_sz;
  if(asi_type == fat_ptr::ASI_LOG) {
    sz += sizeof(object) + sizeof(dbtuple);
  }
  if(!obj) {
    obj = (object*)MM::allocate(sz, 0);
  }
  new (obj) object(ptr, nxt, epoch);

  // If this is a backup server with NVRAM log buffer, then we might be
  // trying to read a tuple that appears to be available in the OID array
  // but its data is still in the buffer - not safe to dig out from the
  // log buffer as it might be receiving a new batch from the primary.
  // So spin here until the tuple is flushed from NVRAM to disk.
  // XXX(tzwang): for now we can't flush - need coordinate with backup daemon
  while (config::is_backup_srv() && ptr.offset() > logmgr->durable_flushed_lsn().offset()) {}

  dbtuple* tuple = obj->tuple();
  new (tuple) dbtuple(0);  // set the correct size later

  auto size_code = ptr.size_code();
  if(asi_type == fat_ptr::ASI_LOG) {
    ASSERT(logmgr);
    // Load tuple varstr from the log
    if (lm) {
      lm->load_object((char *)tuple->get_value_start(), data_sz, ptr);
    } else {
      logmgr->load_object((char *)tuple->get_value_start(), data_sz, ptr);
    }
    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    ASSERT(tuple->size < data_sz);
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr),
            tuple->size);
    size_code = encode_size_aligned(sz);
    obj->_clsn = fat_ptr::make(ptr.offset(), size_code, fat_ptr::ASI_LOG_FLAG);
    ASSERT(obj->_clsn.asi_type() == fat_ptr::ASI_LOG);
  } else {
    // Load tuple data form the chkpt file
    ASSERT(sm_chkpt_mgr::base_chkpt_fd);
    auto n = os_pread(sm_chkpt_mgr::base_chkpt_fd, (char*)obj, data_sz, ptr.offset());
    ALWAYS_ASSERT(n == data_sz);
    ASSERT(obj->_clsn.asi_type() == fat_ptr::ASI_LOG);
    ALWAYS_ASSERT(obj->_clsn.offset());
    ALWAYS_ASSERT(obj->_pdest.offset());
    obj->_next = nxt;
    ALWAYS_ASSERT(obj->tuple()->size <= data_sz - sizeof(object) - sizeof(dbtuple));
  }
  // XXX (tzwang): use the tx's cstamp!
  return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}
