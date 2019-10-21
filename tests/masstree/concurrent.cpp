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

    PROMISE(bool) insertRecord(const Record &record) {
        ermia::TXN::xid_context xid_ctx = mock_create_xid_context();
        ermia::epoch_num e = ermia::MM::epoch_enter();         
        xid_ctx.begin_epoch = e;

        bool res = AWAIT tree_->insert(
            ermia::varstr(record.key.data(), record.key.size()),
            record.value,
            &xid_ctx, nullptr, nullptr);

        ermia::MM::epoch_exit(0, e);
        mock_destroy_xid_context(&xid_ctx);

        RETURN res;
    }

    PROMISE(bool)
    searchByKey(const std::string &key, ermia::OID *out_value) {
        ermia::epoch_num e = ermia::MM::epoch_enter();         

        bool res = AWAIT tree_->search(ermia::varstr(key.data(), key.size()),
                                       *out_value, e, nullptr);

        ermia::MM::epoch_exit(0, e);
        RETURN res;
    }

    void concurrentInsertSequential(const std::vector<Record> & records) {
        initRunningThreads();

        const size_t per_thread_records_num = records.size() / runnable_threads_.size();

        ermia::thread::Thread::Task insert_task = [&] (char *beg) {
            size_t begin_index = static_cast<size_t>(reinterpret_cast<uint64_t>(beg));
            for (uint32_t i = begin_index;
                 i < std::min(begin_index + per_thread_records_num, records.size());
                 i++) {
                const Record & record = records[i];
                EXPECT_TRUE(sync_wait_coro(insertRecord(record)));
            }
        };

        size_t thread_begin_idx = 0;
        for(ermia::thread::Thread * th : runnable_threads_) {
            // hack: pass a int by casting the pointer value
            th->StartTask(insert_task, reinterpret_cast<char *>(thread_begin_idx));
            thread_begin_idx += per_thread_records_num;
        }

        finiRunningThreads();
    }

    void concurrentSearchSequential(const std::vector<Record> & records_found,
                                    const std::vector<Record> & records_not_found) {

        initRunningThreads();

        ermia::thread::Thread::Task search_task = [&] (char *beg) {
        };

        // TODO

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
  private:
    std::vector<ermia::thread::Thread *> runnable_threads_;
};

TEST_F(ConcurrentMasstree, InsertSequential) {
    constexpr size_t per_thread_insert_num = 10;
    const size_t all_records_num = per_thread_insert_num * ermia::config::threads;
    const std::vector<Record> all_records = genRandRecords(all_records_num);

    concurrentInsertSequential(all_records);
}

TEST_F(ConcurrentMasstree, InsertSequentialAndSearchSequential) {
    constexpr size_t per_thread_insert_num = 10;
    const size_t insert_records_num = per_thread_insert_num * ermia::config::threads;
    const std::vector<Record> records_to_insert = genRandRecords(insert_records_num);

    concurrentInsertSequential(records_to_insert);


    constexpr size_t per_thread_search_num = 15;
    constexpr size_t per_thread_search_not_found_num =
        per_thread_search_num - per_thread_insert_num;
    const size_t not_found_records_num =
        per_thread_search_not_found_num * ermia::config::threads;

    const std::vector<Record> records_not_found = genDisjointRecords(
            records_to_insert, not_found_records_num);

    concurrentSearchSequential(records_to_insert, records_not_found);
}

#ifdef USE_STATIC_COROUTINE


#endif

