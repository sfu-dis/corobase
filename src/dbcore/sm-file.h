#pragma once

#include <string>
#include "../txn.h"
#include "sm-common.h"

struct sm_file_descriptor {
  FID fid;
  std::string name;
  ndb_ordered_index *index;
  sm_file_descriptor() : fid(0), name(""), index(nullptr) {}
  sm_file_descriptor(FID f, std::string n, ndb_ordered_index *i) :
    fid(f), name(n), index(i) {}
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

  static inline ndb_ordered_index *get_index(std::string& n) {
    return name_map[n]->index;
  }
};
