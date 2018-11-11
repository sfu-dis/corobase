#include "sm-index.h"
#include "../ermia.h"

namespace ermia {

std::unordered_map<std::string, IndexDescriptor*> IndexDescriptor::name_map;
std::unordered_map<FID, IndexDescriptor*> IndexDescriptor::fid_map;

IndexDescriptor::IndexDescriptor(OrderedIndex *index, std::string& name)
    : name_(name),
      primary_name_(""),
      index_(index),
      tuple_fid_(0),
      tuple_array_(nullptr),
      aux_fid_(0),
      aux_array_(nullptr) {
  name_map[name_] = this;
}

IndexDescriptor::IndexDescriptor(OrderedIndex *index, std::string& name, std::string& primary_name)
    : name_(name),
      primary_name_(primary_name),
      index_(index),
      tuple_fid_(0),
      tuple_array_(nullptr),
      aux_fid_(0),
      aux_array_(nullptr) {
  name_map[name_] = this;
}

void IndexDescriptor::Initialize() {
  if (IsPrimary()) {
    tuple_fid_ = oidmgr->create_file(true);
    fid_map[tuple_fid_] = this;
  } else {
    tuple_fid_ = name_map[primary_name_]->GetTupleFid();
  }
  tuple_array_ = oidmgr->get_array(tuple_fid_);

  // Dedicated array for keys
  aux_fid_ = oidmgr->create_file(true);
  aux_array_ = oidmgr->get_array(aux_fid_);

  // Refresh the array pointers in the tree (for conveinence only)
  index_->SetArrays();
}

void IndexDescriptor::Recover(FID tuple_fid, FID aux_fid, OID himark) {
  ALWAYS_ASSERT(tuple_fid_ == 0);
  tuple_fid_ = tuple_fid;
  aux_fid_ = aux_fid;

  // Both primary and secondary indexes point to the same descriptor
  if (!FidExists(tuple_fid_)) {
    // Primary index
    oidmgr->recreate_file(tuple_fid_);
    fid_map[tuple_fid_] = this;
  }
  oidmgr->recreate_file(aux_fid_);
  fid_map[aux_fid_] = this;

  ALWAYS_ASSERT(oidmgr->file_exists(tuple_fid));
  tuple_array_ = oidmgr->get_array(tuple_fid_);
  ALWAYS_ASSERT(oidmgr->file_exists(aux_fid));
  aux_array_ = oidmgr->get_array(aux_fid_);
  ALWAYS_ASSERT(index_);

  if (himark > 0) {
    tuple_array_->ensure_size(tuple_array_->alloc_size(himark));
    aux_array_->ensure_size(aux_array_->alloc_size(himark));
    oidmgr->recreate_allocator(tuple_fid_, himark);
  }

  // Refresh the array pointers in the tree (for conveinence only)
  index_->SetArrays();
}
}  // namespace ermia
