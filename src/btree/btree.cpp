#include <cassert>
#include <iostream>
#include <glog/logging.h>
#include "btree.h"

#include "../dbcore/sm-oid.h"

namespace ermia {
namespace btree {

template<uint32_t NodeSize, class PayloadType>
bool LeafNode<NodeSize, PayloadType>::BinarySearch(char *key, uint32_t key_size, int32_t &idx) {
  // Find the position in the leaf entry array which begins at data_
  int32_t left = 0, right = num_keys_ - 1;
  while (left <= right) {
    uint32_t mid = (left + right) / 2;
    NodeEntry &entry = GetEntry(mid);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp == 0) {
      // Key exists
      idx = mid;
      return true;
    } else if (cmp > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  LOG_IF(FATAL, left < 0);
  idx = left;
  return false;
}

template<uint32_t NodeSize, class PayloadType>
bool LeafNode<NodeSize, PayloadType>::LinearSearch(char *key, uint32_t key_size, int32_t &idx) {
  idx = 0;
  for (idx = 0; idx < num_keys_; ++idx) {
    NodeEntry &entry = GetEntry(idx);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp == 0) {
      return true;
    } else if (cmp > 0) {
      // Found the place for potential insert
      break;
    }
  }
  return false;
}

template<uint32_t NodeSize>
bool InternalNode<NodeSize>::BinarySearch(char *key, uint32_t key_size, int32_t &idx) {
  // Find the position in the leaf entry array which begins at data_
  int32_t left = 0, right = num_keys_ - 1;
  while (left <= right) {
    uint32_t mid = (left + right) / 2;
    NodeEntry &entry = GetEntry(mid);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp == 0) {
      // Key already exists
      idx = mid;
      return true;
    } else if (cmp > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  LOG_IF(FATAL, left < 0);
  idx = left;
  return false;
}

template<uint32_t NodeSize>
bool InternalNode<NodeSize>::LinearSearch(char *key, uint32_t key_size, int32_t &idx) {
  idx = 0;
  for (idx = 0; idx < num_keys_; ++idx) {
    NodeEntry &entry = GetEntry(idx);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp == 0) {
      return true;
    } else if (cmp > 0) {
      // Found the place for potential insert
      break;
    }
  }
  return false;
}

template<uint32_t NodeSize, class PayloadType>
bool LeafNode<NodeSize, PayloadType>::Add(char *key,
                                          uint32_t key_size,
                                          PayloadType &payload,
                                          bool &did_split,
                                          Stack &stack) {
  did_split = false;

  int32_t insert_idx = -1;
  bool exists = LinearSearch(key, key_size, insert_idx);
  if (exists) {
    return false;
  }

  // Check space
  if (key_size + sizeof(payload) + sizeof(NodeEntry) + data_size_ > kMaxDataSize) {
    // Need split
    LeafNode<NodeSize, PayloadType> *left = nullptr, *right = nullptr;
    Split(left, right, stack);
    bool inserted = false;
    bool split = false;
    if (insert_idx > left->NumKeys()) {  // Belongs to the new right sibling
      inserted = right->Add(key, key_size, payload, split, stack);
    } else {
      inserted = left->Add(key, key_size, payload, split, stack);
    }
    LOG_IF(FATAL, !inserted);
    LOG_IF(FATAL, split) << "New nodes shouldn't split";
    did_split = true;
  } else {
    InsertAt(insert_idx, key, key_size, payload);
  }
  return true;
}

template<uint32_t NodeSize, class PayloadType>
void LeafNode<NodeSize, PayloadType>::InsertAt(uint32_t idx,
                                               char *key,
                                               uint32_t key_size,
                                               PayloadType &payload) {
  LOG_IF(FATAL, sizeof(*this) + key_size + sizeof(payload) + sizeof(NodeEntry) + data_size_ > NodeSize);

  if (idx < num_keys_) {
    // Found the place to insert: move everything after the insert location
    memmove(&((NodeEntry *)data_)[idx + 1],
            &((NodeEntry *)data_)[idx],
            sizeof(NodeEntry) * (num_keys_ - idx));
  }

  // Now the idx-th slot is ready
  NodeEntry *entry = (NodeEntry *)data_ + idx;
  char *data_start = data_ + kMaxDataSize - KeyValueSize() - sizeof(payload) - key_size;
  new (entry) NodeEntry(key_size, sizeof(payload), data_start, key, (char*)&payload);
  assert(memcmp(entry->GetKeyData(), key, key_size) == 0);
  assert(memcmp(entry->GetValueData(), &payload, sizeof(payload)) == 0);

  ++num_keys_;
  data_size_ += ((sizeof(NodeEntry) + key_size + sizeof(payload)));
}

template<uint32_t NodeSize, class PayloadType>
void LeafNode<NodeSize, PayloadType>::Split(LeafNode<NodeSize, PayloadType> *&left,
                                            LeafNode<NodeSize, PayloadType> *&right,
                                            Stack &stack) {
  LOG_IF(FATAL, num_keys_ < 2);
  left = LeafNode::New();
  right = LeafNode::New();

  if (left_sibling_) {
    left_sibling_->SetRightSibling(left);
  }
  if (right_sibling_) {
    right_sibling_->SetLeftSibling(right);
  }

  left->SetLeftSibling(left_sibling_);
  left->SetRightSibling(right);

  right->SetLeftSibling(left);
  right->SetRightSibling(right_sibling_);

  // Copy keys and values to the new left and right nodes
  LOG_IF(FATAL, num_keys_ < 2);
  uint32_t keys_to_move = num_keys_ / 2;
  for (uint32_t i = 0; i < num_keys_; ++i) {
    NodeEntry &entry = GetEntry(i);
    bool added = false;
    bool did_split = false;
    if (i < keys_to_move) {
      added = left->Add(entry.GetKeyData(), entry.GetKeySize(),
                        *(PayloadType *)entry.GetValueData(),
                        did_split, stack);
    } else {
      added = right->Add(entry.GetKeyData(), entry.GetKeySize(),
                         *(PayloadType *)entry.GetValueData(),
                         did_split, stack);
    }
    LOG_IF(FATAL, did_split) << "Shouldn't split";
    LOG_IF(FATAL, !added) << "Couldn't add key-value";
  }

  // Insert the separator key to parent
  InternalNode<NodeSize> *parent = (InternalNode<NodeSize> *)stack.Pop();
  if (!parent) {
    parent = InternalNode<NodeSize>::New();
    stack.Push(parent);  // Growing tree height
  }

  // Keys >= this separator are on the right sibling
  NodeEntry &entry = GetEntry(keys_to_move);
  bool did_split = false;
  parent->Add(entry.GetKeyData(), entry.GetKeySize(), left, right, did_split, stack);
  if (did_split) {
    free(parent);
  }
}

template<uint32_t NodeSize>
void InternalNode<NodeSize>::Add(char *key, uint32_t key_size,
                                 Node *left_child, Node *right_child,
                                 bool &did_split, Stack &stack) {
  int32_t insert_idx = -1;
  bool found = LinearSearch(key, key_size, insert_idx);
  LOG_IF(FATAL, found) << "Key already exists";

  did_split = false;
  // Check space
  if (key_size + sizeof(right_child) + sizeof(NodeEntry) + data_size_ > kMaxDataSize) {
    // Need split
    InternalNode<NodeSize> *left = nullptr, *right = nullptr;
    Split(left, right, stack);
    bool new_node_split = false;
    if (insert_idx > left->NumKeys()) {  // Belongs to the new right sibling
      right->Add(key, key_size, left_child, right_child, new_node_split, stack);
    } else {
      left->Add(key, key_size, left_child, right_child, new_node_split, stack);
    }
    LOG_IF(FATAL, new_node_split) << "New nodes should not split";
    did_split = true;
  } else {
    InsertAt(insert_idx, key, key_size, left_child, right_child);
  }
}

template<uint32_t NodeSize>
void InternalNode<NodeSize>::Split(InternalNode<NodeSize> *&left,
                                   InternalNode<NodeSize> *&right,
                                   Stack &stack) {
  LOG_IF(FATAL, num_keys_ < 2);
  left = InternalNode<NodeSize>::New();
  right = InternalNode<NodeSize>::New();

  // The last half of keys/values go to the right sibling
  uint32_t keys_to_move = num_keys_ / 2;
  assert(keys_to_move > 0);
  assert(num_keys_ >= 2);
  for (uint32_t i = 0; i < num_keys_; ++i) {
    NodeEntry &entry = GetEntry(i);
    Node *left_child = nullptr;
    Node *right_child = *(Node **)entry.GetValueData();
    bool split = false;

    if (i < keys_to_move) {
      if (i == 0) {
        left_child = min_ptr_;
      } else {
        left_child = *(Node **)GetEntry(i - 1).GetValueData();
      }
      left->Add(entry.GetKeyData(), entry.GetKeySize(), left_child, right_child, split, stack);
    } else if (i > keys_to_move) {  // Skip the separator to be inserted to parent
      left_child = *(Node **)GetEntry(i - 1).GetValueData();
      right->Add(entry.GetKeyData(), entry.GetKeySize(), left_child, right_child, split, stack);
    }
    LOG_IF(FATAL, split);
  }

  // Insert the separator key to parent
  InternalNode<NodeSize> *parent = (InternalNode<NodeSize> *)stack.Pop();
  if (!parent) {
    parent = InternalNode<NodeSize>::New();
    stack.Push(parent);
  }

  // Keys >= this separator are on the right sibling
  NodeEntry &entry = GetEntry(keys_to_move);
  bool split = false;
  parent->Add(entry.GetKeyData(), entry.GetKeySize(), left, right, split, stack);
  if (split) {
    free(parent);
  }
}

template<uint32_t NodeSize>
void InternalNode<NodeSize>::InsertAt(uint32_t idx,
                                      char *key, uint32_t key_size,
                                      Node *left_child, Node *right_child) {
  // Found the place to insert: move everything after the insert location
  memmove(&((NodeEntry *)data_)[idx + 1],
          &((NodeEntry *)data_)[idx],
          sizeof(NodeEntry) * (num_keys_ - idx));

  // Now the idx-th slot is ready
  NodeEntry &entry = GetEntry(idx);

  // right_child goes to the value space 
  char *data_start = data_ + kMaxDataSize - KeyValueSize() - key_size - sizeof(Node*);
  new (&entry) NodeEntry(key_size, sizeof(Node*), data_start, key, (char*)&right_child);
  assert(memcmp(entry.GetKeyData(), key, key_size) == 0);
  assert(memcmp(entry.GetValueData(), (char *)&right_child, sizeof(Node*)) == 0);

  ++num_keys_;
  data_size_ += (key_size + sizeof(Node*) + sizeof(NodeEntry));

  // Set up left sibling's right child pointer
  if (idx == 0) {
    min_ptr_ = left_child;
  } else {
    NodeEntry &left_entry = GetEntry(idx - 1);
    memcpy(left_entry.GetValueData(), (char *)&left_child, sizeof(Node*));
  }
}

template<uint32_t NodeSize>
Node *InternalNode<NodeSize>::GetChild(char *key, uint32_t key_size) {
  int32_t idx = -1;
  bool found = LinearSearch(key, key_size, idx);
  if (found) {
    ++idx;
  }

  Node *node = nullptr;
  if (idx == 0) {
    node = min_ptr_;
  } else {
    NodeEntry &entry = GetEntry(idx - 1);
    node = *(Node **)entry.GetValueData();
  }
  return node;
}

template<uint32_t NodeSize, class PayloadType>
LeafNode<NodeSize, PayloadType> *BTree<NodeSize, PayloadType>::ReachLeaf(
    char *key, uint32_t key_size, Stack &stack) {
  Node *node = root_;
  while (!node->IsLeaf()) {
    stack.Push(node);
    node = ((InternalNode<NodeSize> *)node)->GetChild(key, key_size);
  }
  return (LeafNode<NodeSize, PayloadType> *)node;
}

template<uint32_t NodeSize, class PayloadType>
bool BTree<NodeSize, PayloadType>::Insert(char *key, uint32_t key_size, PayloadType &payload) {
  Stack stack;
  LeafNode<NodeSize, PayloadType> *node = ReachLeaf(key, key_size, stack);
  bool did_split = false;
  bool inserted = node->Add(key, key_size, payload, did_split, stack);
  if (did_split) {
    free(node);
  }
  if (stack.num_frames == 1 && stack.Top() != root_) {
    // Only possible if tree has growed during a split, which must have already
    // popped all old internal nodes and will push the new root node. See
    // InternalNode's Split for details.
    root_ = stack.Top();
    assert(root_);
  }
  return inserted;
}

template<uint32_t NodeSize, class PayloadType>
NodeEntry *LeafNode<NodeSize, PayloadType>::GetEntry(char *key, uint32_t key_size) {
  int32_t idx = -1;
  if (LinearSearch(key, key_size, idx)) {
    NodeEntry &entry = GetEntry(idx);
    assert(entry.CompareKey(key, key_size) == 0);
    return &entry;
  }
  return nullptr;
}

template<uint32_t NodeSize, class PayloadType>
bool BTree<NodeSize, PayloadType>::Search(char *key, uint32_t key_size, PayloadType *payload) {
  Stack stack;
  LeafNode<NodeSize, PayloadType> *node = ReachLeaf(key, key_size, stack);
  NodeEntry *entry = node->GetEntry(key, key_size);
  if (entry) {
    memcpy(payload, entry->GetValueData(), sizeof(PayloadType));
  }
  return entry != nullptr;
}

template<uint32_t NodeSize, class PayloadType>
void LeafNode<NodeSize, PayloadType>::Dump() {
  std::cout << "Dumping " << this << ": ";
  for (uint32_t i = 0; i < num_keys_; ++i) {
    auto &entry = GetEntry(i);
    uint64_t key = *(uint64_t*)entry.GetKeyData();
    uint64_t value = *(uint64_t*)entry.GetValueData();
    std::cout << std::dec << key << " -> " << std::hex << value << " | " << std::dec;
  }
  std::cout << std::endl;
}

template<uint32_t NodeSize>
void InternalNode<NodeSize>::Dump() {
  std::cout << "Dumping " << this << ": ";
  std::cout << std::hex << (uint64_t)min_ptr_ << " <- ";
  for (uint32_t i = 0; i < num_keys_; ++i) {
    auto &entry = GetEntry(i);
    uint64_t key = *(uint64_t*)entry.GetKeyData();
    uint64_t right = *(uint64_t*)entry.GetValueData();
    std::cout << std::dec << key << " -> " << std::hex << right << " | " << std::dec;
  }
  std::cout << std::endl;
}

template<uint32_t NodeSize, class PayloadType>
void BTree<NodeSize, PayloadType>::Dump() {
}

// Template instantiation
template class LeafNode<128, uint64_t>;
template class InternalNode<128>;
template class BTree<128, uint64_t>;

template class LeafNode<128, OID>;
template class BTree<128, OID>;

template class LeafNode<256, OID>;
template class InternalNode<256>;
template class BTree<256, OID>;

template class LeafNode<512, OID>;
template class InternalNode<512>;
template class BTree<512, OID>;

template class LeafNode<1024, OID>;
template class InternalNode<1024>;
template class BTree<1024, OID>;

template class LeafNode<2048, OID>;
template class InternalNode<2048>;
template class BTree<2048, OID>;

template class LeafNode<4096, OID>;
template class InternalNode<4096>;
template class BTree<4096, OID>;

template class LeafNode<4096, int>;
template class LeafNode<4096, uint64_t>;
template class BTree<4096, uint64_t>;
}  // namespace btree
}  // namespace ermia
