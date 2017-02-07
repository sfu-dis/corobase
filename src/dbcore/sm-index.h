#pragma once

#include <string>
#include "../txn.h"
#include "sm-common.h"
#include "sm-oid.h"

// Maintains a mapping among FID, table name, and index pointer
struct sm_index_descriptor {
  FID fid;
  std::string name;
  std::string primary_idx_name;  // only set if this is a secondary index
  ndb_ordered_index *index;
  oid_array* array;

  sm_index_descriptor() : 
    fid(0), name(""), primary_idx_name(""), index(nullptr), array(nullptr) {}
  sm_index_descriptor(FID f, std::string n, std::string pn,
                      ndb_ordered_index *i, oid_array* oa) :
    fid(f), name(n), primary_idx_name(pn), index(i), array(oa) {}

  inline bool is_primary_idx() { return primary_idx_name.size() == 0; }
};

// WARNING: No CC, the user should know what to do.
struct sm_index_mgr {
  static std::unordered_map<std::string, sm_index_descriptor*> name_map;
  static std::unordered_map<FID, sm_index_descriptor*> fid_map;

  static inline sm_index_descriptor* get_file(FID f) {
    return fid_map[f];
  }

  static inline sm_index_descriptor* get_file(std::string& n) {
    return name_map[n];
  }

  static inline ndb_ordered_index *get_index(FID f) {
    return fid_map[f]->index;
  }

  static inline ndb_ordered_index *get_index(const std::string& n) {
    return name_map[n]->index;
  }

  static inline FID get_fid(std::string& n) {
    return name_map[n]->fid;
  }

  static inline void new_primary_index(const std::string& name) {
    name_map[name] = new sm_index_descriptor(0, name, "", nullptr, nullptr);
  }

  static inline void new_secondary_index(const std::string& name,
                                         const std::string& primary_idx_name) {
    name_map[name] = new sm_index_descriptor(0, name, primary_idx_name, nullptr, nullptr);
  }
};
