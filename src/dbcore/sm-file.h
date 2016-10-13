#pragma once

#include <string>
#include "../txn.h"
#include "sm-common.h"
#include "sm-oid.h"

// Maintains a mapping among FID, table name, index pointer, RDMA-register
// memory region index, and the pdest array.
//
// pdest_array points to the extra OID array needed by a standby server
// (under the push model). It maintains only the durable version addresses
// in the log and is written only by the primary through RDMA; it is only
// read by the standby server. The pdest array is always kept the same size
// as the major in-memory OID array. On primary, pdest_array is not used.
struct sm_file_descriptor {
  FID fid;
  std::string name;
  ndb_ordered_index *index;
  uint32_t pdest_array_mr_index;
  oid_array* pdest_array;
  oid_array* main_array;
  sm_file_descriptor() : fid(0), name(""), index(nullptr),
                         pdest_array_mr_index(-1), pdest_array(nullptr), main_array(nullptr) {}
  sm_file_descriptor(FID f, std::string n, ndb_ordered_index *i, oid_array* oa) :
    fid(f), name(n), index(i), pdest_array_mr_index(-1), pdest_array(nullptr), main_array(oa) {}

  inline void init_pdest_array() {
    fat_ptr p = oid_array::make();
    pdest_array = (oid_array*)p.offset();
    ALWAYS_ASSERT(pdest_array);
    ALWAYS_ASSERT(oidmgr);
    oid_array* oa = oidmgr->get_array(fid);
    ALWAYS_ASSERT(oa);
    pdest_array->ensure_size(oa->_backing_store.size());
  }
};

// WARNING: No CC, the user should know what to do.
struct sm_file_mgr {
  static std::unordered_map<std::string, sm_file_descriptor*> name_map;
  static std::unordered_map<FID, sm_file_descriptor*> fid_map;

  static inline sm_file_descriptor* get_file(FID f) {
    return fid_map[f];
  }

  static inline sm_file_descriptor* get_file(std::string& n) {
    return name_map[n];
  }

  static inline ndb_ordered_index *get_index(FID f) {
    return fid_map[f]->index;
  }

  static inline oid_array* get_pdest_array(FID f) {
    return fid_map[f]->pdest_array;
  }

  static inline ndb_ordered_index *get_index(const std::string& n) {
    return name_map[n]->index;
  }
};
