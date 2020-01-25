#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-coroutine.h>
#include <dbcore/sm-thread.h>
#include <masstree/masstree_btree.h>
#include <varstr.h>

#include <array>
#include <cassert>
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "utils/record.h"

template <typename T>
using task = ermia::dia::task<T>;
using key_buffer_t = uint64_t;

class Context {
   public:
    Context() : masstree_(nullptr) {
        init();
    }
    ~Context() {
        fini();
    }

    void run() {
        running_threads_ = getThreads(k_threads);
        std::cout << "Running perf use " << running_threads_.size()
                  << " threads" << std::endl;
        setUpMasstree();

        std::cout << "Randomly generating " << k_record_num << " records..."
                  << std::endl;
        std::vector<Record> records = genSequentialRecords(k_record_num, k_key_len);

        static_assert(sizeof(key_buffer_t) <= k_key_len, "Key buffer does not have enough space");

        loadRecords(records);
        verifyInserted(records);

        spin_barrier setup_barrier(running_threads_.size());
        spin_barrier start_barrier(running_threads_.size() > 0 ? 1 : 0);
        std::vector<uint32_t*> counters(running_threads_.size(), nullptr);

        searchRecords(setup_barrier, start_barrier, counters);

        ermia::volatile_write(is_running_, true);
        setup_barrier.wait_for();
        start_barrier.count_down();
        uint32_t last_counter_sum = 0;
        for (uint32_t i = 0; i < k_running_secs; i++) {
            sleep(1);
            std::cout << "Run after " << i+1 << " seconds..." << std::endl;

            uint32_t counter_sum = 0;
            for (uint32_t * p_c : counters) {
              counter_sum += ermia::volatile_read(*p_c);
            }
            std::cout << "Last second throughput: " << (counter_sum - last_counter_sum) << std::endl;
            last_counter_sum = counter_sum;
        }
        ermia::volatile_write(is_running_, false);

        uint32_t counter_sum = 0;
        for (uint32_t *p_c : counters) {
          counter_sum += *p_c;
        }
        std::cout << "Perf completed" << std::endl;
        std::cout << "Avg throughput(per sec): " << counter_sum / k_running_secs
                  << std::endl;

        returnThreads(running_threads_);
    }

    virtual void searchRecords(spin_barrier &setup_barrier, spin_barrier &start_barrier,
                std::vector<uint32_t*> &counters) = 0;

   protected:
    static constexpr int k_record_num = 30000000;
    static constexpr int k_key_len = 8;
    static constexpr int k_threads = 10;
    static constexpr int k_batch_size = 10;
    static constexpr int k_running_secs = 10;

    bool is_running_;

    ermia::ConcurrentMasstree *masstree_;
    std::vector<ermia::thread::Thread *> running_threads_;

   private:
    void setUpMasstree() {
        assert(!masstree_);
        assert(!running_threads_.empty());

        ermia::thread::Thread *th = running_threads_[0];

        ermia::thread::Thread::Task masstree_alloc_task = [&](char *) {
            masstree_ = new (ermia::MM::allocate(sizeof(ermia::ConcurrentMasstree)))
                        ermia::ConcurrentMasstree();
        };
        th->StartTask(masstree_alloc_task, nullptr);
        th->Join();
    }

    void loadRecords(const std::vector<Record> &records) {
        const uint32_t records_per_threads =
            ceil(records.size() / static_cast<float>(running_threads_.size()));
        std::cout << "Start loading " << records.size() << " records..."
                  << std::endl;
        uint32_t loading_begin_idx = 0;
        for (uint32_t i = 0; i < running_threads_.size(); i++) {
            ermia::thread::Thread::Task load_task = [&, i, loading_begin_idx](
                                                        char *) {
                uint32_t records_to_load = std::min(
                    records_per_threads,
                    static_cast<uint32_t>(records.size() - loading_begin_idx));
                printf(
                    "thread ID(%d): start loading %d records from index "
                    "%d...\n",
                    i, records_to_load, loading_begin_idx);

                ermia::dia::coro_task_private::memory_pool memory_pool;
                for (uint32_t j = loading_begin_idx;
                     j < loading_begin_idx + records_to_load; j++) {
                    const Record &record = records[j];
                    ermia::TXN::xid_context xid_ctx;
                    xid_ctx.begin_epoch = 0;
                    assert(
                        sync_wait_coro(masstree_->insert(
                            ermia::varstr(record.key.data(), record.key.size()),
                            record.value, &xid_ctx, nullptr, nullptr)) &&
                        "Fail to insert record into masstree!");
                }

                printf("thread ID(%d): finish loading %d records\n", i,
                       records_to_load);
            };
            running_threads_[i]->StartTask(load_task, nullptr);
            loading_begin_idx += records_per_threads;
        }
        for(auto & th : running_threads_) {
            th->Join();
        }
        std::cout << "Finish loading " << records.size() << " records"
                  << std::endl;
    }

    void verifyInserted(const std::vector<Record> &records) {
        ermia::dia::coro_task_private::memory_pool memory_pool;
        for (auto &record : records) {
            ermia::OID value;
            sync_wait_coro(masstree_->search(
                ermia::varstr(record.key.data(), record.key.size()), value, 0,
                nullptr));
            assert(value == record.value);
        }
    }

    static void init() {
        ermia::config::node_memory_gb = 2;
        ermia::config::num_backups = 0;
        ermia::config::physical_workers_only = true;
        ermia::config::threads = 20;

        ermia::config::init();
        ermia::MM::prepare_node_memory();
    }

    static void fini() {
        ermia::MM::free_node_memory();
        ermia::thread::Finalize();
    }

    static std::vector<ermia::thread::Thread *> getThreads(unsigned int num) {
        std::vector<ermia::thread::Thread *> idle_threads;
        for (uint32_t i = 0; i < std::min(num, ermia::config::threads); i++) {
            idle_threads.emplace_back(ermia::thread::GetThread(true));
            assert(idle_threads.back() && "Threads not available!");
        }
        return idle_threads;
    }

    static void returnThreads(
        std::vector<ermia::thread::Thread *> &running_threads_) {
        for (ermia::thread::Thread *th : running_threads_) {
            th->Join();
            ermia::thread::PutThread(th);
        }
        running_threads_.clear();
    }
};

#ifdef USE_STATIC_COROUTINE

class ContextNestedCoro : public Context {
   public:
    void searchRecords(spin_barrier &setup_barrier, spin_barrier &start_barrier,
                std::vector<uint32_t*> &counters) {
        for (uint32_t i = 0; i < running_threads_.size(); i++) {
            ermia::thread::Thread::Task search_task = [&, i](char *) {
                auto seed =
                    std::chrono::system_clock::now().time_since_epoch().count();
                std::default_random_engine generator(seed);
                std::uniform_int_distribution<uint64_t> distribution;
                std::array<task<bool>, k_batch_size> task_queue;
                std::array<ermia::OID, k_batch_size> task_rets;
                std::array<key_buffer_t, k_batch_size> task_key_bufs;
                std::array<ermia::varstr, k_batch_size> task_params;
                std::array<ermia::dia::coro_task_private::coro_stack,
                           k_batch_size>
                    coro_stacks;
                ermia::dia::coro_task_private::memory_pool memory_pool;

                uint32_t *counter = (uint32_t*)(ermia::MM::allocate(sizeof(uint32_t)));
                *counter = 0;
                counters[i] = counter;

                setup_barrier.count_down();

                start_barrier.wait_for();
                while (ermia::volatile_read(is_running_)) {
                    for (uint32_t j = 0; j < k_batch_size; j++) {
                        task<bool> &t = task_queue[j];
                        if (t.valid()) {
                            if (t.done()) {
                                bool res = t.get_return_value();
                                t = task<bool>(nullptr);
                                (*counter)++;
                            } else {
                                t.resume();
                            }
                        }

                        if (!t.valid()) {
                            task_key_bufs[j] = distribution(generator);
                            task_params[j] = ermia::varstr((const char*)(&task_key_bufs[j]), k_key_len);
                            t = masstree_->search(task_params[j], task_rets[j], 0, nullptr);
                            t.set_call_stack(&(coro_stacks[j]));
                        }
                    }
                }

                for (auto &t : task_queue) {
                    t.destroy();
                }
            };
            running_threads_[i]->StartTask(search_task, nullptr);
        }
    }
};

#else

class ContextSequential : public Context {
   public:
    void searchRecords(spin_barrier &setup_barrier, spin_barrier &start_barrier,
                std::vector<uint32_t*> &counters) {
        for (uint32_t i = 0; i < running_threads_.size(); i++) {
            ermia::thread::Thread::Task search_task = [&, i](char *) {
                auto seed =
                    std::chrono::system_clock::now().time_since_epoch().count();
                std::default_random_engine generator(seed);
                std::uniform_int_distribution<uint64_t> distribution;
                key_buffer_t key_buf;

                uint32_t *counter = (uint32_t*)(ermia::MM::allocate(sizeof(uint32_t)));
                *counter = 0;
                counters[i] = counter;

                setup_barrier.count_down();

                start_barrier.wait_for();
                while (ermia::volatile_read(is_running_)) {
                    ermia::OID value_out;
                    key_buf = distribution(generator);
                    bool res = sync_wait_coro(
                        masstree_->search(ermia::varstr((const char*)(&key_buf), k_key_len),
                                          value_out, 0, nullptr));
                    (*counter)++;
                }
            };
            running_threads_[i]->StartTask(search_task, nullptr);
        }
    }
};

class ContextAmac : public Context {
   public:
    void searchRecords(spin_barrier &setup_barrier, spin_barrier &start_barrier,
                std::vector<uint32_t*> &counters) {
        for (uint32_t i = 0; i < running_threads_.size(); i++) {
            ermia::thread::Thread::Task search_task = [&, i](char *) {
                auto seed =
                    std::chrono::system_clock::now().time_since_epoch().count();
                std::default_random_engine generator(seed);
                std::uniform_int_distribution<uint64_t> distribution;
                std::vector<ermia::ConcurrentMasstree::AMACState> amac_states;
                amac_states.reserve(k_batch_size);
                std::array<key_buffer_t, k_batch_size> key_bufs;
                std::array<ermia::varstr, k_batch_size> amac_params;

                uint32_t *counter = (uint32_t*)(ermia::MM::allocate(sizeof(uint32_t)));
                *counter = 0;
                counters[i] = counter;

                setup_barrier.count_down();

                start_barrier.wait_for();
                while (ermia::volatile_read(is_running_)) {
                    for(uint32_t j = 0; j < k_batch_size; j++) {
                        key_bufs[j] = distribution(generator);
                        amac_params[j] = ermia::varstr((const char*)(&key_bufs[j]), k_key_len);
                        amac_states.emplace_back(&(amac_params[j]));
                    }

                    if(amac_states.empty()) {
                        break;
                    }

                    masstree_->search_amac(amac_states, 0);
                    amac_states.clear();

                    *counter += k_batch_size;
                }
            };
            running_threads_[i]->StartTask(search_task, nullptr);
        }
    }
};

#endif

int main() {
#ifdef USE_STATIC_COROUTINE
    ContextNestedCoro context;
#else
    ContextAmac context;
#endif
    context.run();
    return 0;
}
