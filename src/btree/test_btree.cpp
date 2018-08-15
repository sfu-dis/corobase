#include <algorithm>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "btree.h"

TEST(LeafNode, InsertSplit) {
  // Prepare a list of integers to be inserted in random order
  std::vector<int> inputs;
  // 169 for filling a 4k page: this must be adjusted if page layout changes
  // (i.e., fields added to or removed from LeafPage, or page size changes).
  typedef ermia::btree::LeafNode<4096, int> LeafNodeType;
  const uint32_t kInserts = 169;
  for (uint32_t i = 0; i < kInserts + 1; ++i) {
    inputs.emplace_back(i);
  }
  std::random_shuffle(inputs.begin(), inputs.end());

  // Allocate a node
  LeafNodeType *node = (LeafNodeType*)malloc(4096);
  new (node) LeafNodeType;

  // Insert all keys
  ermia::btree::Stack stack;
  for (uint32_t i = 0; i < kInserts; ++i) {
    int k = inputs[i];
    int v = inputs[i] + 1;
    bool did_split;
    bool inserted = node->Add((char*)&k, sizeof(int), v, did_split, stack);
    ASSERT_TRUE(inserted);
    ASSERT_FALSE(did_split);
    ASSERT_EQ(i + 1, node->NumKeys());
  }

  ASSERT_EQ(node->NumKeys(), kInserts);
    
  // Dump all key-payload pairs
  for (uint32_t i = 0; i < node->NumKeys(); ++i) {
    int k = *(int*)node->GetKey(i);
    int v = *(int*)node->GetValue(i);
    ASSERT_EQ(k, v - 1);
  }

  // Now insert more keys will trigger a split
  int k = inputs[kInserts];
  int v = inputs[kInserts] + 1;
  bool did_split = false;
  bool inserted = node->Add((char*)&k, sizeof(int), v, did_split, stack);
  ASSERT_TRUE(did_split);
  ASSERT_TRUE(inserted);

  // Dump all key-payload pairs
  for (uint32_t i = 0; i < node->NumKeys(); ++i) {
    int k = *(int*)node->GetKey(i);
    int v = *(int*)node->GetValue(i);
    ASSERT_EQ(k, v - 1);
  }
  ASSERT_TRUE(inserted);

  // Must <= 1: after split one might be 1 larger
  int diff = node->GetRightSibling()->NumKeys() - kInserts;
  ASSERT_TRUE(diff <= 1);
  free(node);
}

TEST(InternalNode, Insert) {
  // Prepare a list of integers to be inserted in random order
  std::vector<uint64_t> inputs;
  const uint32_t kKeys = 100;
  for (uint64_t i = 0; i < kKeys; ++i) {
    inputs.emplace_back(i);
  }
  std::random_shuffle(inputs.begin(), inputs.end());

  // Allocate a node
  typedef ermia::btree::InternalNode<4096> InternalNodeType;
  InternalNodeType *node = (InternalNodeType*)malloc(4096);
  new (node) InternalNodeType;

  // Insert all keys
  ermia::btree::Stack stack;
  for (uint64_t i = 0; i < inputs.size(); ++i) {
    uint64_t k = inputs[i];
    InternalNodeType *left = (InternalNodeType *)(k + 1);
    InternalNodeType *right = (InternalNodeType *)(k + 2);
    node->Add((char*)&k, sizeof(uint64_t), left, right, stack);
    ASSERT_EQ(i + 1, node->NumKeys());
  }

  ASSERT_EQ(node->NumKeys(), kKeys);

  // Dump all key-payload pairs
  ASSERT_EQ((uint64_t)node->MinPtr(), 1);
  for (uint32_t i = 0; i < node->NumKeys(); ++i) {
    auto &entry = node->GetEntry(i);
    int k = *(int*)entry.GetKeyData();
    ASSERT_EQ(k, i);
    ASSERT_EQ(*(int *)entry.GetValueData(), i + 2);
  }

  free(node);
}

TEST(BTree, NoSplit) {
  ermia::btree::BTree<4096, uint64_t> btree;

  // Prepare a list of integers to be inserted in random order
  std::vector<uint64_t> inputs;
  const uint32_t kKeys = 100;
  for (uint64_t i = 0; i < kKeys; ++i) {
    inputs.emplace_back(i);
  }
  std::random_shuffle(inputs.begin(), inputs.end());

  // Insert all key-value pairs
  for (uint64_t i = 0; i < inputs.size(); ++i) {
    uint64_t k = inputs[i];
    uint64_t v = inputs[i] + 1;
    bool inserted = btree.Insert((char*)&k, sizeof(uint64_t), v);
    ASSERT_TRUE(inserted);
  }

  // See if we can find them
  for (uint64_t i = 0; i < inputs.size(); ++i) {
    uint64_t k = inputs[i];
    uint64_t v = 0;
    bool found = btree.Search((char*)&k, sizeof(uint64_t), &v);
    ASSERT_TRUE(found);
    ASSERT_EQ(v, k + 1);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
