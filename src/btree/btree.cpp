#include <cassert>
#include <glog/logging.h>
#include "btree.h"

namespace ermia {
namespace btree {

template<uint32_t NodeSize, class PayloadType>
bool LeafNode<NodeSize, PayloadType>::Add(char *key,
                                          uint32_t key_size,
                                          PayloadType &payload,
                                          bool &did_split,
                                          Stack &stack) {
  did_split = false;

  // Find the position in the leaf entry array which begins at data_
  // FIXME(tzwang): do binary search here
  uint32_t insert_idx = 0;
  for (insert_idx = 0; insert_idx < num_keys_; ++insert_idx) {
    NodeEntry &entry = GetEntry(insert_idx);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp == 0) {
      // Key already exists
      return false;
    } else if (cmp > 0) {
      // Found the place
      break;
    }
  }

  // Check space
  if (key_size + sizeof(payload) + sizeof(NodeEntry) + data_size_ > DataCapacity()) {
    // Need split
    LeafNode<NodeSize, PayloadType> *left = nullptr, *right = nullptr;
    Split(left, right, stack);
    bool inserted = false;
    bool split = false;
    if (insert_idx > num_keys_ / 2) {  // Belongs to the new right sibling
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
  // Found the place to insert: move everything after the insert location
  memmove(&((NodeEntry *)data_)[idx + 1],
          &((NodeEntry *)data_)[idx],
          sizeof(NodeEntry) * (num_keys_ - idx));

  // Now the idx-th slot is ready
  NodeEntry *entry = (NodeEntry *)data_ + idx;
  char *data_start = data_ + NodeSize - sizeof(*this) - data_size_ - key_size - sizeof(payload);
  new (entry) NodeEntry(key_size, sizeof(payload), data_start, key, (char*)&payload);
  assert(memcmp(entry->GetKeyData(), key, key_size) == 0);
  assert(memcmp(entry->GetValueData(), &payload, sizeof(payload)) == 0);

  ++num_keys_;
  data_size_ += (key_size + sizeof(payload));
}

template<uint32_t NodeSize, class PayloadType>
void LeafNode<NodeSize, PayloadType>::Split(LeafNode<NodeSize, PayloadType> *&left,
                                            LeafNode<NodeSize, PayloadType> *&right,
                                            Stack &stack) {
  LOG_IF(FATAL, num_keys_ < 2);
  left = LeafNode::New();
  right = LeafNode::New();

  right->SetRightSibling(right_sibling_);
  left->SetRightSibling(right);
  right_sibling_ = right;

  // Copy keys and values to the new left and right nodes
  uint32_t keys_to_move = num_keys_ / 2;
  for (uint32_t i = 0; i < num_keys_; ++i) {
    NodeEntry &entry = GetEntry(i);
    bool added = false;
    bool did_split = false;
    if (i < num_keys_ - keys_to_move) {
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

  // Keys >= this separator are on the right sibling
  NodeEntry &entry = GetEntry(num_keys_ - keys_to_move);

  // Insert the separator key to parent
  InternalNode<NodeSize> *parent = (InternalNode<NodeSize> *)stack.Pop();
  if (!parent) {
    parent = InternalNode<NodeSize>::New();
    stack.Push(parent);  // Growing tree height
  }
  parent->Add(entry.GetKeyData(), entry.GetKeySize(), left, right, stack);
}

template<uint32_t NodeSize>
void InternalNode<NodeSize>::Add(char *key, uint32_t key_size,
                                 Node *left_child, Node *right_child,
                                 Stack &stack) {
  // Find the position in the leaf entry array which begins at data_
  // FIXME(tzwang): do binary search here
  uint32_t insert_idx = 0;
  for (insert_idx = 0; insert_idx < num_keys_; ++insert_idx) {
    NodeEntry &entry = GetEntry(insert_idx);
    int cmp = entry.CompareKey(key, key_size);
    LOG_IF(FATAL, cmp == 0) << "Key already exists in parent node";
    if (cmp > 0) {
      // Found the place
      break;
    }
  }

  // Check space
  if (key_size + sizeof(right_child) + data_size_ + sizeof(InternalNode) > DataCapacity()) {
    abort();
    // Need split
    auto *right = Split(stack);
    if (insert_idx > num_keys_ / 2) {  // Belongs to the new right sibling
      right->Add(key, key_size, left_child, right_child, stack);
    }
  }

  InsertAt(insert_idx, key, key_size, left_child, right_child);
}

template<uint32_t NodeSize>
InternalNode<NodeSize> *InternalNode<NodeSize>::Split(Stack &stack) {
  LOG_IF(FATAL, num_keys_ < 2);
  InternalNode<NodeSize> *right_sibling = InternalNode::New();
  // The last half of keys/values go to the right sibling
  uint32_t keys_to_move = num_keys_ / 2;
  for (uint32_t i = num_keys_ - keys_to_move; i < num_keys_; ++i) {
    NodeEntry &entry = GetEntry(i);
    right_sibling->Add(entry.GetKeyData(), entry.GetKeySize(),
                       (Node *)GetEntry(i - 1).GetValueData(),
                       (Node *)entry.GetValueData(), stack);
  }

  // Keys >= this separator are on the right sibling
  NodeEntry &entry = GetEntry(num_keys_ - keys_to_move);

  // Insert the separator key to parent
  InternalNode<NodeSize> *parent = (InternalNode<NodeSize> *)stack.Top();
  if (!parent) {
    parent = InternalNode<NodeSize>::New();
  }
  parent->Add(entry.GetKeyData(), entry.GetKeySize(), this, right_sibling, stack);

  // Reduce the number of keys, leave the data there to be overwritten later
  num_keys_ -= keys_to_move;

  return right_sibling;
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
  char *data_start = data_ + NodeSize - sizeof(*this) - data_size_ - key_size - sizeof(Node*);
  new (&entry) NodeEntry(key_size, sizeof(Node*), data_start, key, (char*)&right_child);
  assert(memcmp(entry.GetKeyData(), key, key_size) == 0);
  assert(memcmp(entry.GetValueData(), (char *)&right_child, sizeof(Node*)) == 0);

  ++num_keys_;
  data_size_ += (key_size + sizeof(Node*));

  // Set up left sibling's right child pointer
  if (idx == 0) {
    min_ptr_ = left_child;
  } else {
    NodeEntry &left_sibling = GetEntry(idx - 1);
    memcpy(left_sibling.GetValueData(), (char *)&left_child, sizeof(InternalNode*));
  }
}

template<uint32_t NodeSize>
Node *InternalNode<NodeSize>::GetChild(char *key, uint32_t key_size) {
  uint32_t idx = 0;
  for (idx = 0; idx < num_keys_; ++idx) {
    NodeEntry &entry = GetEntry(idx);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp > 0) {
      break;
    }
  }

  Node *node;
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
  }
}

template<uint32_t NodeSize, class PayloadType>
NodeEntry *LeafNode<NodeSize, PayloadType>::GetEntry(char *key, uint32_t key_size) {
  for (uint32_t idx = 0; idx < num_keys_; ++idx) {
    NodeEntry &entry = GetEntry(idx);
    int cmp = entry.CompareKey(key, key_size);
    if (cmp == 0) {
      return &entry;
    }
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
void BTree<NodeSize, PayloadType>::Dump() {
}

// Template instantiation
template class LeafNode<4096, int>;
template class LeafNode<4096, uint64_t>;
template class InternalNode<4096>;
template class BTree<4096, uint64_t>;
}  // namespace btree
}  // namespace ermia
