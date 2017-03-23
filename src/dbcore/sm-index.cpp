#include "sm-index.h"  
#include "../benchmarks/ordered_index.h"

std::unordered_map<std::string, IndexDescriptor*> IndexDescriptor::name_map;
std::unordered_map<FID, IndexDescriptor*> IndexDescriptor::fid_map;

IndexDescriptor::IndexDescriptor(std::string& name) : name_(name), primary_name_(""),
  tuple_fid_(0), key_fid_(0), tuple_array_(nullptr), key_array_(nullptr) {
  name_map[name_] = this;
  index_ = new OrderedIndex(this);
}

IndexDescriptor::IndexDescriptor(std::string& name, std::string& primary_name)
: name_(name), primary_name_(primary_name),
  tuple_fid_(0), key_fid_(0), tuple_array_(nullptr), key_array_(nullptr) {
  name_map[name_] = this;
  index_ = new OrderedIndex(this);
}

void IndexDescriptor::Initialize() {
  if(IsPrimary()) {
    tuple_fid_ = oidmgr->create_file(true);
    fid_map[tuple_fid_] = this;
  } else {
    tuple_fid_ = name_map[primary_name_]->GetTupleFid();
  }
  tuple_array_ = oidmgr->get_array(tuple_fid_);

  // Dedicated array for keys
  key_fid_ = oidmgr->create_file(true);
  key_array_ = oidmgr->get_array(key_fid_);

  // Refresh the array pointers in the tree (for conveinence only)
  index_->SetArrays();
}

void IndexDescriptor::Recover(FID tuple_fid, FID key_fid, OID himark) {
  ALWAYS_ASSERT(tuple_fid_ == 0);
  tuple_fid_ = tuple_fid;
  key_fid_ = key_fid;

  // Both primary and secondary indexes point to the same descriptor
  if(!FidExists(tuple_fid_)) {
    // Primary index
    oidmgr->recreate_file(tuple_fid_);
    fid_map[tuple_fid_] = this;
  }
  oidmgr->recreate_file(key_fid_);
  fid_map[key_fid_] = this;

  ALWAYS_ASSERT(oidmgr->file_exists(tuple_fid));
  tuple_array_ = oidmgr->get_array(tuple_fid_);
  ALWAYS_ASSERT(oidmgr->file_exists(key_fid));
  key_array_ = oidmgr->get_array(key_fid_);
  ALWAYS_ASSERT(index_);

  if(himark > 0) {
    tuple_array_->ensure_size(tuple_array_->alloc_size(himark));
    key_array_->ensure_size(key_array_->alloc_size(himark));
    oidmgr->recreate_allocator(tuple_fid_, himark);
  }

  // Refresh the array pointers in the tree (for conveinence only)
  index_->SetArrays();
}
