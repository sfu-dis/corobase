#include <array>
#include <vector>

#include <gtest/gtest.h>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-thread.h>
#include <dbcore/sm-coroutine.h>
#include <masstree/masstree_btree.h>
#include <xid.h>
#include <varstr.h>

#include "utils/record.h"

using ermia::MM::allocated_node_memory;
using ermia::MM::node_memory;

template <typename T>
using task = ermia::dia::task<T>;

ermia::TXN::xid_context mock_create_xid_context() {
    ermia::TXN::xid_context ret;
    ret.owner = ermia::XID::make(0, 0);
    ret.xct = nullptr;
    return ret;
}

void mock_destroy_xid_context(ermia::TXN::xid_context *xid_context) {
    void(0);
}


class ConcurrentMasstree: public ::testing::Test {
   protected:
    virtual void SetUp() override {
        ermia::config::node_memory_gb = 2;
        ermia::config::num_backups = 0;
        ermia::config::numa_spread = false;
        ermia::config::threads = 5;

        ermia::config::init();
        ermia::MM::prepare_node_memory();

        tree_ = new ermia::ConcurrentMasstree();
    }

    virtual void TearDown() override {
        delete tree_;

        ermia::MM::free_node_memory();
        ermia::thread::Finalize();
    }

    ermia::TXN::xid_context thread_begin() {
        ermia::TXN::xid_context xid_ctx = mock_create_xid_context();
        ermia::epoch_num e = ermia::MM::epoch_enter();         
        xid_ctx.begin_epoch = e;

        return xid_ctx;
    }

    void thread_end(ermia::TXN::xid_context * xid_ctx) {
        ermia::MM::epoch_exit(0, xid_ctx->begin_epoch);
        mock_destroy_xid_context(xid_ctx);
    }

    PROMISE(bool) insertRecord(const Record &record,
                               ermia::TXN::xid_context * xid_ctx) {
        bool res = AWAIT tree_->insert(
            ermia::varstr(record.key.data(), record.key.size()),
            record.value,
            xid_ctx, nullptr, nullptr);

        RETURN res;
    }

    PROMISE(bool)
    searchByKey(const std::string &key, ermia::OID *out_value, ermia::epoch_num e) {
        bool res = AWAIT tree_->search(ermia::varstr(key.data(), key.size()),
                                       *out_value, e, nullptr);

        RETURN res;
    }

    void concurrentInsertSequential(const std::vector<Record> & records) {
        initRunningThreads();

        const size_t per_thread_records_num = std::ceil(
                records.size() / static_cast<float>(runnable_threads_.size()));

        std::vector<size_t> records_begin_idxs;
        records_begin_idxs.reserve(runnable_threads_.size());

        ermia::thread::Thread::Task insert_task = [&] (char *tid) {
            ermia::TXN::xid_context xid_context = thread_begin();
            ermia::dia::coro_task_private::memory_pool memory_pool;

            size_t thread_id = static_cast<size_t>(reinterpret_cast<intptr_t>(tid));
            size_t begin_index = records_begin_idxs[thread_id];
            for (uint32_t i = begin_index;
                 i < std::min(begin_index + per_thread_records_num, records.size());
                 i++) {
                const Record & record = records[i];
                EXPECT_TRUE(sync_wait_coro(insertRecord(record, &xid_context)));
            }

            thread_end(&xid_context);
        };

        size_t thread_id = 0;
        size_t thread_begin_idx = 0;
        for(ermia::thread::Thread * th : runnable_threads_) {
            records_begin_idxs.emplace_back(thread_begin_idx);

            // hack: pass a thread id through value a pointer(intptr_t)
            th->StartTask(insert_task, reinterpret_cast<char *>(thread_id));

            thread_begin_idx += per_thread_records_num;
            thread_id++;
        }

        finiRunningThreads();
    }

    void concurrentSearchSequential(const std::vector<Record> & records_found,
                                    const std::vector<Record> & records_not_found) {

        initRunningThreads();

        ermia::thread::Thread::Task search_task = [&] (char *tid) {
            ermia::TXN::xid_context xid_context = thread_begin();
            ermia::dia::coro_task_private::memory_pool memory_pool;

            size_t thread_id = static_cast<size_t>(reinterpret_cast<intptr_t>(tid));
            
            ermia::OID out_value;
            for(const Record & record : records_found) {
                EXPECT_TRUE(sync_wait_coro(
                            searchByKey(record.key, &out_value, xid_context.begin_epoch)));
                EXPECT_EQ(out_value, record.value);
            }

            for(const Record & record : records_not_found) {
                EXPECT_FALSE(sync_wait_coro(
                            searchByKey(record.key, &out_value, xid_context.begin_epoch)));
            }

            thread_end(&xid_context);
        };

        size_t thread_id = 0;
        for(ermia::thread::Thread * th : runnable_threads_) {
            th->StartTask(search_task, reinterpret_cast<char *>(thread_id));

            thread_id++;
        }

        finiRunningThreads();
    }

    void initRunningThreads() {
        for(uint32_t i = 0; i < ermia::config::threads; i++) {
            ermia::thread::Thread* th = ermia::thread::GetThread(true);
            EXPECT_TRUE(th);
            runnable_threads_.emplace_back(th);
        } 

    }

    void finiRunningThreads() {
        for(ermia::thread::Thread * th : runnable_threads_) {
            th->Join();
            ermia::thread::PutThread(th);
        }

        runnable_threads_.clear();
    }

    ermia::ConcurrentMasstree *tree_;
    std::vector<ermia::thread::Thread *> runnable_threads_;
};

TEST_F(ConcurrentMasstree, InsertSequential) {
    constexpr size_t per_thread_insert_num = 10;
    const size_t all_records_num = per_thread_insert_num * ermia::config::threads;
    const std::vector<Record> all_records = genRandRecords(all_records_num);

    concurrentInsertSequential(all_records);
}

TEST_F(ConcurrentMasstree, InsertSequentialAndSearchSequential) {
    constexpr size_t per_thread_insert_num = 50;
    const size_t insert_records_num = per_thread_insert_num * ermia::config::threads;
    const std::vector<Record> records_to_insert = genRandRecords(insert_records_num);

    concurrentInsertSequential(records_to_insert);


    constexpr size_t search_not_found_num = 100;
    const std::vector<Record> records_not_found = genDisjointRecords(
            records_to_insert, search_not_found_num);

    concurrentSearchSequential(records_to_insert, records_not_found);
}

#ifdef USE_STATIC_COROUTINE

TEST_F(ConcurrentMasstree, InsertSequentialAndSearchInterleaved) {
    constexpr size_t per_thread_insert_num = 50;
    const size_t insert_records_num = per_thread_insert_num * ermia::config::threads;
    const std::vector<Record> records_to_insert = genRandRecords(insert_records_num);

    concurrentInsertSequential(records_to_insert);


    initRunningThreads();

    ermia::thread::Thread::Task search_task = [&] (char *tid) {
        constexpr int task_queue_size= 5;

        ermia::TXN::xid_context xid_context = thread_begin();
        ermia::dia::coro_task_private::memory_pool memory_pool;

        std::array<task<bool>, task_queue_size> task_queue;
        std::array<
            std::vector<std::experimental::coroutine_handle<void>>,
            task_queue_size> coro_stacks;

        std::vector<ermia::OID> coro_return_values;
        coro_return_values.resize(records_to_insert.size());

        uint32_t next_task_index = 0;
        for (uint32_t i = 0; i < task_queue_size; i++) {
            task<bool> & coro_task = task_queue[i];
            const Record & record = records_to_insert[next_task_index];
            ermia::OID & result = coro_return_values[next_task_index];
            next_task_index++;
            coro_task = searchByKey(record.key, &result, xid_context.begin_epoch);
            coro_task.set_call_stack(&coro_stacks[i]);
        }

        uint32_t completed_task_cnt = 0;
        while (completed_task_cnt < records_to_insert.size()) {
            for(uint32_t i = 0; i < task_queue_size; i++) {
                task<bool> & coro_task = task_queue[i];

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
                        coro_task = searchByKey(record.key, &result,
                                                xid_context.begin_epoch);
                        coro_task.set_call_stack(&coro_stacks[i]);
                    } else {
                        coro_task = task<bool>(nullptr);
                    }
                }
            }
        }

        for(uint32_t i = 0; i < records_to_insert.size(); i++) {
            EXPECT_EQ(records_to_insert[i].value, coro_return_values[i]);
        }

        for(task<bool> & coro_task : task_queue) {
            EXPECT_TRUE(!coro_task.valid() || coro_task.done());
        }

        thread_end(&xid_context);
    };

    size_t thread_id = 0;
    for(ermia::thread::Thread * th : runnable_threads_) {
        th->StartTask(search_task, reinterpret_cast<char *>(thread_id));

        thread_id++;
    }

    finiRunningThreads();
}


#endif
