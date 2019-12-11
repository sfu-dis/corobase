#include <benchmark/benchmark.h>

#include <numa.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>

#include <array>
#include <vector>
#include <sstream>

#include <masstree/masstree_btree.h>

#include "utils/record.h"

using ermia::MM::allocated_node_memory;
using ermia::MM::node_memory;

template <typename T>
using task = ermia::dia::task<T>;

using generator_handle = std::experimental::coroutine_handle<
    ermia::dia::generator<bool>::promise_type>;

class PerfSingleThreadSearch : public benchmark::Fixture {
  public:
    void SetUp(const ::benchmark::State &state) {
        // create masstree
        tree_ = new ermia::ConcurrentMasstree();

        // generate records
        const uint32_t key_length = state.range(1);
        records = genSequentialRecords(state.range(0), key_length);

        // insert records
        ermia::TXN::xid_context context_mock;
        context_mock.begin_epoch = 0;
        context_mock.owner = ermia::XID::make(0, 0);
        context_mock.xct = nullptr;

        for(const Record & record : records) {
            sync_wait_coro(
                tree_->insert(
                    ermia::varstr(record.key.data(), record.key.size()),
                    record.value,
                    &context_mock, nullptr, nullptr));
        }

        if (ermia::config::enable_perf) {
            startPerf();
        }
    }

    void TearDown(const ::benchmark::State &state) {
        if (ermia::config::enable_perf) {
            stopPerf();
        }

        delete tree_;
        records.clear();
    }

    static void InterleavedArguments(benchmark::internal::Benchmark *b) {
        std::vector<uint32_t> record_num = {10000, 1000000, 10000000};
        std::vector<uint32_t> key_size = {8};
        std::vector<uint32_t> group_size = {5, 15, 25};
        for(uint32_t r : record_num) {
            for(uint32_t k : key_size) {
                for(uint32_t g : group_size) {
                    b->Args({r, k, g});
                }
            }
        }
    }

    static void SequentialArguments(benchmark::internal::Benchmark *b) {
        std::vector<uint32_t> record_num = {10000, 1000000, 10000000};
        std::vector<uint32_t> key_size = {8};
        for(uint32_t r : record_num) {
            for(uint32_t k : key_size) {
                b->Args({r, k});
            }
        }
    }

    void startPerf() {
       std::cerr << "start perf..." << std::endl;

       std::stringstream parent_pid;
       parent_pid << getpid();

       pid_t pid = fork();
       // Launch profiler
       if (pid == 0) {
           if(ermia::config::perf_record_event != "") {
             exit(execl("/usr/bin/perf","perf","record", "-F", "99", "-e", ermia::config::perf_record_event.c_str(),
                        "-p", parent_pid.str().c_str(), nullptr));
           } else {
             exit(execl("/usr/bin/perf","perf","stat", "-B", "-e",  "cache-references,cache-misses,cycles,instructions,branches,faults", 
                        "-p", parent_pid.str().c_str(), nullptr));
           }
       } else {
           perf_pid_ = pid;
       }
    }

    void stopPerf() {
        std::cerr << "stop perf..." << std::endl;

        kill(perf_pid_, SIGINT);
        waitpid(perf_pid_, nullptr, 0);
    }

    pid_t perf_pid_;
    std::vector<Record> records;
    ermia::ConcurrentMasstree *tree_;
    ermia::dia::coro_task_private::memory_pool *pool_;
};


#ifdef USE_STATIC_COROUTINE

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, AdvancedCoro) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<task<bool>> task_queue(queue_size);
        std::vector<ermia::OID> out_values(queue_size);
        std::vector<std::vector<std::experimental::coroutine_handle<void>>> call_stacks(
                queue_size, std::vector<std::experimental::coroutine_handle<void>>(10));

        uint32_t completed_task_cnt = 0;
        uint32_t next_record_idx = 0;
        while (completed_task_cnt < records.size()) {
            for(uint32_t i= 0; i < queue_size; i++) {
                task<bool> & coro_task = task_queue[i];
                if(coro_task.valid()){
                    if(!coro_task.done()) {
                        coro_task.resume();
                    } else {
                        bool res = coro_task.get_return_value();
                        ASSERT(res);
                        completed_task_cnt++;
                        coro_task = task<bool>(nullptr);
                    }
                }

                if(!coro_task.valid() && next_record_idx < records.size()) {
                    const Record & record = records[next_record_idx++];
                    coro_task = tree_->search(
                            ermia::varstr(record.key.data(), record.key.size()),
                            out_values[i], cur_epoch, nullptr);
                    // call_stacks[i].reserve(20);
                    coro_task.set_call_stack(&call_stacks[i]);
                    // XXX: remove this line improve performance about 25% ??
                    // initial suspended
                    // coro_task.resume();
                }
            }
        }
    }    
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, AdvancedCoro)
    ->Apply(PerfSingleThreadSearch::InterleavedArguments);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, AdvancedCoroForceGrouped) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<task<bool>> task_queue(queue_size);
        std::vector<ermia::OID> out_values(queue_size);
        std::vector<std::vector<std::experimental::coroutine_handle<void>>> call_stacks(queue_size);

        uint32_t completed_task_cnt = 0;
        uint32_t next_record_idx = 0;
        while (completed_task_cnt < records.size()) {
            for (uint32_t i= 0; i < queue_size; i++) {
                if (next_record_idx >= records.size()) {
                    break;
                }

                task<bool> & coro_task = task_queue[i];
                ASSERT(!coro_task.valid());
                const Record & record = records[next_record_idx++];
                coro_task = tree_->search(
                        ermia::varstr(record.key.data(), record.key.size()),
                        out_values[i], cur_epoch, nullptr);
                call_stacks[i].reserve(20);
                coro_task.set_call_stack(&call_stacks[i]);
            }

            bool batch_completed = false;
            while (!batch_completed) {
                batch_completed = true;
                for (uint32_t i= 0; i < queue_size; i++) {
                    task<bool> & coro_task = task_queue[i];
                    if (!coro_task.valid()){
                        continue;
                    }

                    if (!coro_task.done()) {
                        coro_task.resume();
                        batch_completed = false;
                    } else {
                        bool res = coro_task.get_return_value();
                        ASSERT(res);
                        completed_task_cnt++;
                        coro_task = task<bool>(nullptr);
                    }
                }
            }
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, AdvancedCoroForceGrouped)
    ->Apply(PerfSingleThreadSearch::InterleavedArguments);

#else

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, Sequential) (benchmark::State &st) {
    for (auto _ : st) {
        constexpr ermia::epoch_num cur_epoch = 0;
        ermia::OID out_value;
        for(const Record & record : records) {
            bool res = tree_->search(ermia::varstr(record.key.data(), record.key.size()),
                                     out_value, cur_epoch, nullptr);
            ASSERT(res && out_value == record.value);
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, Sequential)
    ->Apply(PerfSingleThreadSearch::SequentialArguments);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, SimpleCoro) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);

        std::vector<generator_handle> generator_queue(queue_size, nullptr);
        std::vector<ermia::OID> out_values(queue_size, 0);

        constexpr ermia::epoch_num cur_epoch = 0;
        ermia::SingleThreadedMasstree::threadinfo ti(cur_epoch);

        uint32_t completed_search_num = 0;
        uint32_t next_record_idx = 0;

        while(completed_search_num < records.size()) {
            for(uint32_t i = 0; i < queue_size; i++) {
                generator_handle & handle = generator_queue[i];

                if (handle) {
                    if(!handle.done()) {
                        handle.resume();
                    } else {
                        completed_search_num++;
                        handle.destroy();
                        handle = nullptr;
                    }
                }
                
                if(!handle && next_record_idx < records.size()) {
                    const Record & record = records[next_record_idx++];
                    handle = tree_->search_coro(
                        ermia::varstr(record.key.data(), record.key.size()),
                        out_values[i], ti, nullptr).get_handle();
                }
            }
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, SimpleCoro)
    ->Apply(PerfSingleThreadSearch::InterleavedArguments);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, Amac) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t parellel_amac_num = st.range(2);

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<ermia::ConcurrentMasstree::AMACState> amac_states;
        std::vector<ermia::varstr> amac_params;
        amac_states.reserve(parellel_amac_num);
        amac_params.reserve(parellel_amac_num);

        uint32_t next_record_idx = 0;
        while(next_record_idx < records.size()) {
            for(uint32_t i = 0; i < parellel_amac_num; i++) {
                if (next_record_idx >= records.size()) {
                    break;
                }

                const Record & record = records[next_record_idx++];
                amac_params.emplace_back(record.key.data(), record.key.size());
                amac_states.emplace_back(&amac_params.back());
            }

            if(amac_states.empty()) {
                break;
            }

            tree_->search_amac(amac_states, cur_epoch);

            amac_states.clear();
            amac_params.clear();
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, Amac)
    ->Apply(PerfSingleThreadSearch::InterleavedArguments);

#endif
