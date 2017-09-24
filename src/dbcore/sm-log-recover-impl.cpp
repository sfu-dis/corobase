#include "../benchmarks/ndb_wrapper.h"
#include "../util.h"
#include "sm-index.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-oid-alloc-impl.h"
#include "sm-rep.h"

// Returns something that we will install on the OID entry.
fat_ptr sm_log_recover_impl::PrepareObject(
    sm_log_scan_mgr::record_scan* logrec) {
  // Regardless of the replay/warm-up policy (ie whether to load tuples from
  // storage to memory), here we need a wrapper that points to the ``real''
  // localtion and the next version.
  //
  // Note: payload_size() includes the whole varstr. See do_tree_put's
  // log_update call.
  size_t sz = sizeof(Object);

  // Pre-allocate space for the payload
  sz += (sizeof(dbtuple) + logrec->payload_size());
  sz = align_up(sz);

  Object* obj = new (MM::allocate(sz, 0))
      Object(logrec->payload_ptr(), NULL_PTR, 0, config::eager_warm_up());
  obj->SetClsn(logrec->payload_ptr());
  ASSERT(obj->GetClsn().asi_type() == fat_ptr::ASI_LOG);

  if (config::eager_warm_up()) {
    obj->Pin();
  }
  return fat_ptr::make(obj, encode_size_aligned(sz), 0);
}

void sm_log_recover_impl::recover_insert(sm_log_scan_mgr::record_scan* logrec,
                                         bool latest) {
  FID f = logrec->fid();
  OID o = logrec->oid();
  if (config::is_backup_srv()) {
    if (config::full_replay) {
      oid_array* oa = get_impl(oidmgr)->get_array(f);
      oa->ensure_size(o);
      fat_ptr* entry_ptr = oa->get(o);
      if (volatile_read(entry_ptr->_ptr) == 0) {
        fat_ptr ptr = PrepareObject(logrec);
        Object *obj = (Object*)ptr.offset();
        // Fully instantiate the version
        obj->Pin(config::persist_policy != config::kPersistAsync);
        if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, 0, ptr._ptr)) {
          MM::deallocate(ptr);
        }
      }
    } else {
      // Install a fat_ptr in the persistent array directly
      fat_ptr ptr = logrec->payload_ptr();
      FID pf = IndexDescriptor::Get(f)->GetPersistentAddressFid();
      oid_array* oa = get_impl(oidmgr)->get_array(pf);
      oa->ensure_size(o);
      // Skip if a newer one is already there
      fat_ptr* entry_ptr = oa->get(o);
      if (volatile_read(entry_ptr->_ptr) == 0) {
        __sync_bool_compare_and_swap(&entry_ptr->_ptr, 0, ptr._ptr);
      }
    }
  } else {
    fat_ptr ptr = PrepareObject(logrec);
    ASSERT(oidmgr->file_exists(f));
    oid_array* oa = get_impl(oidmgr)->get_array(f);
    oa->ensure_size(o);
    // The chkpt recovery process might have picked up this tuple already
    if (latest) {
      fat_ptr* entry_ptr = oa->get(o);
      fat_ptr expected = *entry_ptr;
      if (expected == NULL_PTR) {
        __sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr, ptr._ptr);
      } else {
        MM::deallocate(ptr);
      }
    } else {
      oidmgr->oid_put_new_if_absent(f, o, ptr);
      ASSERT(oidmgr->oid_get(oa, o) == ptr);
    }
  }
}

void sm_log_recover_impl::recover_index_insert(
    sm_log_scan_mgr::record_scan* logrec) {
  // No need if the chkpt recovery already picked up this tuple
  FID fid = logrec->fid();
  IndexDescriptor* id = IndexDescriptor::Get(fid);
  if (config::is_backup_srv() ||
      oidmgr->oid_get(id->GetKeyArray(), logrec->oid()).offset() == 0) {
    recover_index_insert(logrec, id->GetIndex());
  }
}

void sm_log_recover_impl::recover_index_insert(
    sm_log_scan_mgr::record_scan* logrec, OrderedIndex* index) {
  static const uint32_t kBufferSize = 8 * config::MB;
  ASSERT(index);
  auto sz = align_up(logrec->payload_size());
  static __thread char* buf;
  if (!buf) {
    buf = (char*)malloc(kBufferSize);
  }
  char* payload_buf = nullptr;
  ALWAYS_ASSERT(sz < kBufferSize);
  if (config::is_backup_srv() ||
      logrec->payload_lsn().offset() >= logmgr->durable_flushed_lsn_offset()) {
    // In the log buffer, point directly to it without memcpy
    ASSERT(config::is_backup_srv());
    auto* logrec_impl = get_impl(logrec);
    logrec_impl->scan.has_payloads =
        true;  // FIXME(tzwang): do this in a better way
    payload_buf = (char*)logrec_impl->scan.payload();
    ALWAYS_ASSERT(payload_buf);
  } else {
    logrec->load_object(buf, sz);
    payload_buf = buf;
  }

  // Extract the real key length (don't use varstr.data()!)
  size_t len = ((varstr*)payload_buf)->size();
  ASSERT(align_up(len + sizeof(varstr)) == sz);

  oid_array* ka = get_impl(oidmgr)->get_array(logrec->fid());
  if (!config::is_backup_srv() &&
      volatile_read(*ka->get(logrec->oid())) != NULL_PTR) {
    return;
  }

  varstr payload_key((char*)payload_buf + sizeof(varstr), len);
  if (index->tree_.underlying_btree.insert_if_absent(payload_key, logrec->oid(),
                                                     NULL)) {
    // Don't add the key on backup - on backup chkpt will traverse OID arrays
    if (!config::is_backup_srv()) {
      // Construct the varkey to be inserted in the oid array
      // (skip the varstr struct then it's data)
      varstr* key = (varstr*)MM::allocate(sizeof(varstr) + len, 0);
      new (key) varstr((char*)key + sizeof(varstr), len);
      key->copy_from((char*)payload_buf + sizeof(varstr), len);
      volatile_write(*ka->get(logrec->oid()),
                     fat_ptr::make((void*)key, INVALID_SIZE_CODE));
    }
  }
}

void sm_log_recover_impl::recover_update(sm_log_scan_mgr::record_scan* logrec,
                                         bool is_delete, bool latest) {
  FID f = logrec->fid();
  OID o = logrec->oid();
  ASSERT(oidmgr->file_exists(f));

  if (config::is_backup_srv()) {
    // Deletes on backups are handled the same way as updates, just
    // with an empty payload
    if (config::full_replay) {
      auto* oa = IndexDescriptor::Get(f)->GetTupleArray();
      fat_ptr* entry_ptr = oa->get(o);
      fat_ptr ptr = NULL_PTR;
      bool success = false;
    retry_install_obj:
      fat_ptr expected = volatile_read(*entry_ptr);
      Object* head_obj = (Object*)expected.offset();
      if (!head_obj ||
          head_obj->GetClsn().offset() < logrec->payload_ptr().offset()) {
        Object* new_obj = nullptr;
        if (ptr == NULL_PTR) {
          ptr = PrepareObject(logrec);
          new_obj = (Object*)ptr.offset();
          // Fully instantiate the version
          new_obj->Pin(config::persist_policy != config::kPersistAsync);
        }
        new_obj = (Object*)ptr.offset();
        new_obj->SetNextVolatile(expected);
        if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                          ptr._ptr)) {
          goto retry_install_obj;
        }
        success = true;
      }
      if (ptr != NULL_PTR && !success) {
        MM::deallocate(ptr);
      }
    } else {
      FID pf = IndexDescriptor::Get(f)->GetPersistentAddressFid();
      oid_array* oa = get_impl(oidmgr)->get_array(pf);
      fat_ptr* entry_ptr = oa->get(o);
      fat_ptr ptr = logrec->payload_ptr();
    retry_backup:
      fat_ptr expected = *entry_ptr;
      ASSERT(expected.asi_type() == 0 ||
             expected.asi_type() == fat_ptr::ASI_LOG);
      if (expected.offset() < ptr.offset()) {
        if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                          ptr._ptr)) {
          goto retry_backup;
        }
      }
    }
  } else {
    ALWAYS_ASSERT(!is_delete);  // Primary ignores delete before reaching here
    // FIXME(tzwang): during recovery the primary only uses parallel OID replay,
    // so no write-write-conflicts possible, so we can simply skip deletes here.
    auto* oa = IndexDescriptor::Get(f)->GetTupleArray();
    fat_ptr head_ptr = *oa->get(o);
    fat_ptr ptr = PrepareObject(logrec);
    Object* new_object = (Object*)ptr.offset();
    if (latest) {
    retry_primary:
      fat_ptr* entry_ptr = oa->get(o);
      fat_ptr expected = *entry_ptr;
      new_object->SetNextVolatile(expected);
      // Go in to see LSN
      ASSERT(expected.offset());
      Object* obj = (Object*)expected.offset();
      if (obj->GetPersistentAddress().offset() <
          logrec->payload_lsn().offset()) {
        if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                          ptr._ptr)) {
          goto retry_primary;
        }
      } else {
        MM::deallocate(ptr);
      }
    } else {
      oidmgr->oid_put(oa, o, ptr);
    }
  }
}

void sm_log_recover_impl::recover_update_key(
    sm_log_scan_mgr::record_scan* logrec) {
  return;
// Disabled for now, fix later
#if 0
  // Used when emulating the case where we didn't have OID arrays - must update tree leaf nodes
  auto* index = sm_index_mgr::get_index(logrec->fid());
  ASSERT(index);
  static const uint32_t kBufferSize = 128 * config::MB;
  auto sz = align_up(logrec->payload_size());
  static __thread char *buf;
  if (unlikely(not buf)) {
    buf = (char *)malloc(kBufferSize);
  }
  char* payload_buf = nullptr;
  ALWAYS_ASSERT(sz < kBufferSize);
  if(logrec->payload_lsn().offset() >= logmgr->durable_flushed_lsn_offset()) {
    // In the log buffer, point directly to it without memcpy
    ASSERT(config::is_backup_srv());
    auto *logrec_impl = get_impl(logrec);
    logrec_impl->scan.has_payloads = true;  // FIXME(tzwang): do this in a better way
    payload_buf = (char*)logrec_impl->scan.payload();
    ALWAYS_ASSERT(payload_buf);
  } else {
    logrec->load_object(buf, sz);
    payload_buf = buf;
  }

  // Extract the real key length (don't use varstr.data()!)
  size_t len = ((varstr *)payload_buf)->size();
  ASSERT(align_up(len + sizeof(varstr)) == sz);

  varstr key((char*)payload_buf + sizeof(varstr), len);
  OID old_oid = 0;
  index->btr.underlying_btree.insert(key, logrec->oid(), nullptr, &old_oid, nullptr);
  ASSERT(old_oid == logrec->oid());
#endif
}

OrderedIndex* sm_log_recover_impl::recover_fid(
    sm_log_scan_mgr::record_scan* logrec) {
  // XXX(tzwang): no support for dynamically created tables for now
  char buf[256];
  auto sz = logrec->payload_size();
  ALWAYS_ASSERT(sz <= 256);  // 256 should be enough, revisit later if not
  logrec->load_object(buf, sz);
  FID key_fid = *(FID*)buf;
  std::string name(buf + sizeof(FID));

  // The benchmark should have registered the table with the engine
  ALWAYS_ASSERT(IndexDescriptor::NameExists(name));
  FID tuple_fid = logrec->fid();
  IndexDescriptor::Get(name)->Recover(tuple_fid, key_fid);
  LOG(INFO) << "[Recovery] " << name << "(" << tuple_fid << ", " << key_fid
            << ")";
  return IndexDescriptor::GetIndex(name);
}
