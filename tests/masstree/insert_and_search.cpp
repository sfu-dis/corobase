#include <gtest/gtest.h>
#include <numa.h>
#include <sched.h>
#include <sys/mman.h>
#include <vector>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-coroutine.h>
#include <masstree/masstree_btree.h>
#include <varstr.h>

#include "utils/context_mock.h"
#include "utils/rand.h"
#include "utils/record.h"

using ermia::MM::allocated_node_memory;
using ermia::MM::node_memory;

template <typename T>
using task = ermia::dia::task<T>;

static constexpr size_t kPerNodeMemorySize = 128 * ermia::config::MB;

class SingleThreadMasstree : public ::testing::Test {
   private:
    void allocMemoryOnNumaNode() {
        // allocate memory for node of current thread
        int node = numa_node_of_cpu(sched_getcpu());

        // hack, only allocate enough space to fit the current
        // thread node value (which is the index).
        ermia::config::numa_nodes = node + 1;
        allocated_node_memory = new uint64_t[ermia::config::numa_nodes];
        node_memory = new char *[ermia::config::numa_nodes];

        // pre-allocate memory for current thread node
        allocated_node_memory[node] = 0;
        node_memory[node] = new char[kPerNodeMemorySize];

        ALWAYS_ASSERT(node_memory[node]);
        LOG(INFO) << "Memory allocated for node " << node;
    }

    void freeMemoryOnNumaNode() {
        int node = numa_node_of_cpu(sched_getcpu());

        delete[] node_memory[node];
        delete[] allocated_node_memory;
        delete[] node_memory;
    }

   protected:
    virtual void SetUp() override {
        allocMemoryOnNumaNode();

        tree_ = new ermia::SingleThreadedMasstree();
    }

    virtual void TearDown() override {
        delete tree_;

        freeMemoryOnNumaNode();
    }

    template <typename record_key_t>
    bool insertRecord(const Record<record_key_t> &record) {
        std::string key_str = record.key_to_str();
        return tree_->insert(ermia::varstr(key_str.data(), key_str.size()),
                             record.value, mock_xid_get_context(), nullptr,
                             nullptr);
    }

    template <typename record_key_t>
    PROMISE(bool)
    searchRecord(Record<record_key_t> *out_record) {
        std::string key_str = out_record->key_to_str();
        RETURN AWAIT tree_->search(ermia::varstr(key_str.data(), key_str.size()),
                                   out_record->value, mock_get_cur_epoch(),
                                   nullptr);
    }

    ermia::SingleThreadedMasstree *tree_;
};

TEST_F(SingleThreadMasstree, InsertOnly) {
    Record<uint32_t> record{1, 1};

    EXPECT_TRUE(insertRecord(record));
}

TEST_F(SingleThreadMasstree, InsertAndSearch_Found) {
    Record<uint32_t> insert_record{1, 2};
    EXPECT_TRUE(insertRecord(insert_record));

    Record<uint32_t> search_result{1, 0};
    EXPECT_TRUE(sync_wait_coro(searchRecord(&search_result)));
    EXPECT_EQ(search_result.value, insert_record.value);
}

TEST_F(SingleThreadMasstree, InsertAndSearch_NotFound) {
    Record<uint32_t> insert_record{1, 2};
    EXPECT_TRUE(insertRecord(insert_record));

    Record<uint32_t> search_result{2, 0};
    EXPECT_FALSE(sync_wait_coro(searchRecord(&search_result)));
}

TEST_F(SingleThreadMasstree, VarKey_InsertAndSearch) {
    const int kRecordNum = 10;
    std::vector<Record<std::string>> data;
    data.reserve(kRecordNum);
    for (uint32_t i = 0; i < kRecordNum; i++) {
        data.emplace_back(Record<std::string>{randString(), randUInt32()});
    }

    for (const auto &record : data) {
        EXPECT_TRUE(insertRecord(record));
    }

    for (const auto &inserted_record : data) {
        Record<std::string> search_result{inserted_record.key, 0};
        EXPECT_TRUE(sync_wait_coro(searchRecord(&search_result)));
        EXPECT_EQ(inserted_record.value, search_result.value);
    }
}

#ifdef USE_STATIC_COROUTINE

// TEST_F(SingleThreadMasstree, CoroutineGet) {
//    std::vector<Record<uint32_t>> data;
//
//    const int kRecordNum = 100;
//    for (uint32_t i = 0; i < kRecordNum; i++) {
//    }
//}

#endif  // USE_STATIC_COROUTINE

