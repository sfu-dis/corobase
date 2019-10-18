
#include <gtest/gtest.h>
#include <array>
#include <vector>

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
        ermia::config::threads = 2;

        ermia::config::init();
        ermia::MM::prepare_node_memory();

        tree_ = new ermia::ConcurrentMasstree();
    }

    virtual void TearDown() override {
        delete tree_;
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

    void initRunningThreads() {
        for(uint32_t i = 0; i < ermia::config::threads; i++) {
            ermia::thread::Thread* th = ermia::thread::GetThread(true);
            runnable_threads_.emplace_back(th);
        } 

    }

    void finiRunningThreads() {
        for(ermia::thread::Thread * th : runnable_threads_) {
            th->Join();
            th->Destroy();
        }
    }

    std::vector<ermia::thread::Thread *> runnable_threads_;
    ermia::ConcurrentMasstree *tree_;
};

TEST_F(ConcurrentMasstree, InsertSequential) {
    const size_t per_thread_records = 10;
    const size_t all_records_num = per_thread_records * runnable_threads_.size();
    const std::vector<Record> all_records = genRandRecords(all_records_num, 128);

    ermia::thread::Thread::Task insert_task = [&] (char *beg) {
        size_t begin_index = static_cast<size_t>(reinterpret_cast<uint64_t>(beg));
        for (uint32_t i = begin_index; 
             i < std::min(begin_index + per_thread_records, all_records.size());
             i++) {
            const Record & record = all_records[i];
            EXPECT_TRUE(sync_wait_coro(insertRecord(record)));
        }
    };

    initRunningThreads();

    size_t thread_begin_idx = 0;
    for(ermia::thread::Thread * th : runnable_threads_) {
        // hack: pass a int by casting the pointer value
        th->StartTask(insert_task, reinterpret_cast<char *>(thread_begin_idx));
        thread_begin_idx += per_thread_records;
    }

    finiRunningThreads();
}

#ifdef USE_STATIC_COROUTINE


#endif

