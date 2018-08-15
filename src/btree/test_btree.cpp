#include <algorithm>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "btree.h"

TEST(LeafNode, Insert) {
  // Prepare a list of integers to be inserted in random order
  std::vector<int> inputs;
  const uint32_t kKeys = 100;
  for (uint32_t i = 0; i < kKeys; ++i) {
    inputs.emplace_back(i);
  }
  std::random_shuffle(inputs.begin(), inputs.end());

  // Allocate a node
  typedef ermia::btree::LeafNode<4096, int> LeafNodeType;
  LeafNodeType *node = (LeafNodeType*)malloc(4096);
  new (node) LeafNodeType;

  // Insert all keys
  ermia::btree::Stack stack;
  for (uint32_t i = 0; i < inputs.size(); ++i) {
    int k = inputs[i];
    int v = inputs[i] + 1;
    bool inserted = node->Add((char*)&k, sizeof(int), v, stack);
    ASSERT_TRUE(inserted);
    ASSERT_EQ(i + 1, node->NumKeys());
  }

  ASSERT_EQ(node->NumKeys(), kKeys);
    
  // Dump all key-payload pairs
  for (uint32_t i = 0; i < node->NumKeys(); ++i) {
    int k = *(int*)node->GetKey(i);
    int v = *(int*)node->GetValue(i);
    ASSERT_EQ(k, i);
    ASSERT_EQ(v, i + 1);
  }

  free(node);
}

TEST(LeafNode, Split) {
  // Prepare a list of integers to be inserted in random order
  std::vector<int> inputs;
  const uint32_t kKeys = 1000;
  for (uint32_t i = 0; i < kKeys; ++i) {
    inputs.emplace_back(i);
  }
  //std::random_shuffle(inputs.begin(), inputs.end());

  // Allocate a node
  typedef ermia::btree::LeafNode<4096, int> LeafNodeType;
  LeafNodeType *node = (LeafNodeType*)malloc(4096);
  new (node) LeafNodeType;

  // Insert all keys
  ermia::btree::Stack stack;
  for (uint32_t i = 0; i < inputs.size(); ++i) {
    int k = inputs[i];
    int v = inputs[i] + 1;
    LOG(INFO) << "INSERT " << k;
    bool inserted = node->Add((char*)&k, sizeof(int), v, stack);
    ASSERT_TRUE(inserted);
    ASSERT_EQ(i + 1, node->NumKeys());
  }

  ASSERT_EQ(node->NumKeys(), kKeys);

  // Dump all key-payload pairs
  for (uint32_t i = 0; i < node->NumKeys(); ++i) {
    int k = *(int*)node->GetKey(i);
    int v = *(int*)node->GetValue(i);
    ASSERT_EQ(k, i);
    ASSERT_EQ(v, i + 1);
  }

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
