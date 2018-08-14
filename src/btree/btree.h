#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

namespace ermia {
namespace btree {

class Node {
protected:
  uint32_t num_keys_;

public:
  Node() : num_keys_(0) {}
  inline uint32_t NumKeys() { return num_keys_; }
  virtual bool IsLeaf() = 0;
};

struct Stack {
  struct Frame {
    Node *node;
    Frame() : node(nullptr) {}
    Frame(Node *node) : node(node) {}
    ~Frame() { node = nullptr; }
  };

  static const uint32_t kMaxFrames = 32;
  Frame frames[kMaxFrames];
  uint32_t num_frames;

  Stack() : num_frames(0) {}
  ~Stack() { num_frames = 0; }
  inline void Push(Node *node) { new (&frames[num_frames++]) Frame(node); }
  inline Frame *Pop() { return &frames[--num_frames]; }
  Frame *Top() { return &frames[num_frames - 1]; }
};

class NodeEntry {
private:
  uint32_t key_size_;    // Key size
  uint32_t value_size_;  // Value size
  char *data_;           // Data (includes key and value) address

private:
  inline char *GetData() { return data_; }
  static int Compare(char *d1, uint32_t l1, char *d2, uint32_t l2) {
    int cmp = memcmp(d1, d2, std::min<uint32_t>(l1, l2));
    if (cmp == 0 && l1 != l2) {
      return l1 > l2 ? 1 : -1;
    }
    return cmp;
  }

public:
  NodeEntry() : key_size_(0), value_size_(0), data_(nullptr) {}
  NodeEntry(uint32_t key_size, uint32_t value_size, char *data, char *key, char *value)
    : key_size_(key_size), value_size_(value_size), data_(data) {
    memcpy(data_, key, key_size_);
    memcpy(data_ + key_size_, value, value_size_);
  }
  inline uint32_t GetKeySize() { return key_size_; }
  inline uint32_t GetValueSize() { return value_size_; }
  inline char *GetKeyData() { return data_; }
  inline char *GetValueData() { return data_ + key_size_; }

  inline int CompareKey(char *key, uint32_t size) {
    return NodeEntry::Compare(GetKeyData(), key_size_, key, size);
  }
};

template<uint32_t NodeSize, class PayloadType>
class LeafNode : public Node {
private:
  uint32_t data_size_;  // Includes keys and values
  char data_[0];  // Must be the last element

private:
  void InsertAt(uint32_t idx, char *key, uint32_t key_size, PayloadType &payload);
  // Split out a new right sibling, self as the left sibling
  LeafNode *Split(Stack &stack);
  inline NodeEntry &GetEntry(uint32_t idx) { return ((NodeEntry *)data_)[idx]; }
  NodeEntry *GetEntry(char *key, uint32_t key_size);

public:
  LeafNode() : Node(), data_size_(0) {}
  inline virtual bool IsLeaf() { return true; }

  static LeafNode *New() {
    LeafNode *node = (LeafNode *)malloc(NodeSize);
    new (node) LeafNode();
    return node;
  }

  // Data area size, including keys and values
  inline uint32_t Capacity() { return NodeSize - sizeof(*this); }
  inline char *GetKey(uint32_t idx) { return GetEntry(idx).GetKeyData(); }
  inline char *GetValue(uint32_t idx) { return GetEntry(idx).GetValueData(); }
  bool Add(char *key, uint32_t key_size, PayloadType &payload, Stack &stack);
};

template<uint32_t NodeSize>
class InternalNode : public Node {
private:
  Node *min_ptr_;
  uint32_t data_size_;  // Includes keys only, pointers are stored in InternalEntries
  char data_[0];  // Must be the last element

private:
  void InsertAt(uint32_t idx, char *key, uint32_t key_size, Node *left_child, Node *right_child);
  InternalNode *Split(Stack &stack);

public:
  InternalNode() : min_ptr_(nullptr), data_size_(0) {}
  inline virtual bool IsLeaf() { return false; }
  inline uint32_t Capacity() { return NodeSize - sizeof(*this); }
  inline NodeEntry &GetEntry(uint32_t idx) { return ((NodeEntry *)data_)[idx]; }
  Node *GetChild(char *key, uint32_t key_size);
  static inline InternalNode *New() {
    InternalNode<NodeSize> *node = (InternalNode *)malloc(NodeSize);
    new (node) InternalNode<NodeSize>;
    return node;
  }
  void Add(char *key, uint32_t key_size, Node *left_child, Node *right_child, Stack &stack);
  Node *MinPtr() { return min_ptr_; }
};

template<uint32_t NodeSize, class PayloadType>
class BTree {
private:
  Node *root_;

private:
  LeafNode<NodeSize, PayloadType> *ReachLeaf(char *key, uint32_t key_size, Stack &stack);

public:
  BTree() : root_(LeafNode<NodeSize, PayloadType>::New()) {}
  bool Insert(char *key, uint32_t key_size, PayloadType &payload);
  bool Search(char *key, uint32_t key_size, PayloadType *payload);
};
}  // namespace btree
}  // namespace ermia
