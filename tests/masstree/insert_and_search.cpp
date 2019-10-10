#include <gtest/gtest.h>
#include <numa.h>
#include <sched.h>
#include <sys/mman.h>
#include <array>
#include <vector>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-coroutine.h>
#include <masstree/masstree_btree.h>
#include <varstr.h>

#include "utils/context_mock.h"
#include "utils/record.h"

using ermia::MM::allocated_node_memory;
using ermia::MM::node_memory;

template <typename T>
using task = ermia::dia::task<T>;

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
        ermia::config::node_memory_gb = 1;
        allocated_node_memory[node] = 0;
        node_memory[node] =
            new char[ermia::config::node_memory_gb * ermia::config::GB];
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

    bool insertRecord(const Record &record) {
        return tree_->insert(
            ermia::varstr(record.key.data(), record.key.size()), record.value,
            mock_xid_get_context(), nullptr, nullptr);
    }

    PROMISE(bool)
    searchByKey(const std::string &key, ermia::OID *out_value) {
        RETURN AWAIT tree_->search(ermia::varstr(key.data(), key.size()),
                                   *out_value, mock_get_cur_epoch(), nullptr);
    }

    ermia::SingleThreadedMasstree *tree_;
};

TEST_F(SingleThreadMasstree, InsertOnly) {
    Record record = Record{"absd", 1423};
    EXPECT_TRUE(insertRecord(record));
}

TEST_F(SingleThreadMasstree, InsertAndSearch_Found) {
    Record record = Record{"absd", 1423};
    EXPECT_TRUE(insertRecord(record));

    ermia::OID value_out;
    EXPECT_TRUE(sync_wait_coro(searchByKey(record.key, &value_out)));
    EXPECT_EQ(record.value, value_out);
}

TEST_F(SingleThreadMasstree, InsertAndSearch_NotFound) {
    Record record = Record{"absd", 1423};
    EXPECT_TRUE(insertRecord(record));

    ermia::OID value_out;
    EXPECT_FALSE(sync_wait_coro(searchByKey("ccc", &value_out)));
}

TEST_F(SingleThreadMasstree, InsertAndSearch_ManyRecords) {
    const std::vector<Record> records_to_insert = genRandRecords(400, 128);
    for (const Record &record : records_to_insert) {
        EXPECT_TRUE(insertRecord(record));
    }

    for (const Record &record : records_to_insert) {
        ermia::OID value_out;
        EXPECT_TRUE(sync_wait_coro(searchByKey(record.key, &value_out)));
        EXPECT_EQ(record.value, value_out);
    }

    ermia::OID value_out;
    std::string non_key = genKeyNotInRecords(records_to_insert);
    EXPECT_FALSE(sync_wait_coro(searchByKey(non_key, &value_out)));
}

#ifdef USE_STATIC_COROUTINE

TEST_F(SingleThreadMasstree, InsertAndSearch_Coro) {
    std::vector<Record> records_to_insert = genRandRecords(50, 10);
    for (const Record &record : records_to_insert) {
        EXPECT_TRUE(insertRecord(record));
    }

    std::vector<ermia::OID> coro_return_values;
    coro_return_values.resize(records_to_insert.size());
    
    constexpr int task_queue_size= 5;
    std::array<task<bool>, task_queue_size> task_queue;
    uint32_t next_task_index = 0;
    for (task<bool> & coro_task : task_queue) {
        const Record & record = records_to_insert[next_task_index];
        ermia::OID & result = coro_return_values[next_task_index];
        next_task_index++;
        coro_task = searchByKey(record.key, &result);
    }

    uint32_t completed_task_cnt = 0;
    while (completed_task_cnt < records_to_insert.size()) {
        for(task<bool> & coro_task : task_queue) {
            if(!coro_task.valid()) {
                continue;
            }

            if(!coro_task.done()) {
                coro_task.resume();
            } else {
                EXPECT_TRUE(coro_task.get_return_value());
                completed_task_cnt++;

                if(next_task_index < records_to_insert.size()) {
                    const Record & record = records_to_insert[next_task_index];
                    ermia::OID & result = coro_return_values[next_task_index];
                    next_task_index++;
                    coro_task = searchByKey(record.key, &result);
                } else {
                    coro_task = task<bool>(nullptr);
                }
            }
        }
    }

    for(uint32_t i = 0; i < records_to_insert.size(); i++) {
        EXPECT_EQ(records_to_insert[i].value, coro_return_values[i]);
    }
}

#endif  // USE_STATIC_COROUTINE

