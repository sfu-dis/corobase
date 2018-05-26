#pragma once

#include <string>
#include "sm-common.h"
#include "sm-oid.h"

namespace ermia {

class OrderedIndex;

class IndexDescriptor {
 public:
  static std::unordered_map<std::string, IndexDescriptor*> name_map;
  static std::unordered_map<FID, IndexDescriptor*> fid_map;

  static inline bool NameExists(std::string name) {
    return name_map.find(name) != name_map.end();
  }
  static inline bool FidExists(FID fid) {
    return fid_map.find(fid) != fid_map.end();
  }
  static inline IndexDescriptor* Get(std::string name) {
    return name_map[name];
  }
  static inline IndexDescriptor* Get(FID fid) { return fid_map[fid]; }
  static inline OrderedIndex* GetIndex(const std::string& name) {
    return name_map[name]->GetIndex();
  }
  static inline OrderedIndex* GetIndex(FID fid) {
    return fid_map[fid]->GetIndex();
  }
  static inline void New(std::string name, const char* primary = nullptr) {
    if (primary) {
      std::string p(primary);
      name_map[name] = new IndexDescriptor(name, p);
    } else {
      name_map[name] = new IndexDescriptor(name);
    }
  }
  static inline uint32_t NumIndexes() { return name_map.size(); }

 private:
  std::string name_;
  std::string primary_name_;  // Null for primary index
  OrderedIndex* index_;

  FID tuple_fid_;
  oid_array* tuple_array_;

  // An auxiliary array: on primary this is the key array, on
  // backups this is the persistent address array.
  FID aux_fid_;
  oid_array* aux_array_;

 public:
  IndexDescriptor(std::string& name);
  IndexDescriptor(std::string& name, std::string& primary_name);

  void Initialize();
  void Recover(FID tuple_fid, FID key_fid, OID himark = 0);
  inline bool IsPrimary() { return primary_name_.size() == 0; }
  inline std::string& GetName() { return name_; }
  inline OrderedIndex* GetIndex() { return index_; }
  inline FID GetTupleFid() { return tuple_fid_; }
  inline FID GetKeyFid() {
    ASSERT(!config::is_backup_srv() || (config::command_log && config::replay_threads));
    return aux_fid_;
  }
  inline oid_array* GetKeyArray() {
    ASSERT(!config::is_backup_srv() || (config::command_log && config::replay_threads));
    return aux_array_;
  }
  inline FID GetPersistentAddressFid() {
    ASSERT(config::is_backup_srv());
    return aux_fid_;
  }
  inline oid_array* GetPersistentAddressArray() {
    ASSERT(config::is_backup_srv());
    return aux_array_;
  }
  inline oid_array* GetTupleArray() { return tuple_array_; }
};
}  // namespace ermia
