#include <benchmark/benchmark.h>

#include <numa.h>
#include <sched.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/syscall.h>
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

    static void BatchArguments(benchmark::internal::Benchmark *bench) {
        std::vector<uint32_t> record_num = {10000000};
        std::vector<uint32_t> key_size = {8};
        std::vector<uint32_t> group_size = {5, 15, 25};
        std::vector<uint32_t> batch_to_run = {1000000};
        for(uint32_t r : record_num) {
            for(uint32_t k : key_size) {
                for(uint32_t g : group_size) {
                    for(uint32_t b : batch_to_run) {
                        bench->Args({r, k, g, b});
                    }
                }
            }
        }
    }

    void startPerf() {
       std::cerr << "start perf..." << std::endl;

       std::stringstream parent_pid;
       parent_pid << getpid();

       int cpu = -1;
       syscall(SYS_getcpu, &cpu, nullptr, nullptr);

       std::stringstream cpu_id;
       cpu_id << cpu;

       pid_t pid = fork();
       // Launch profiler
       if (pid == 0) {
           if(ermia::config::perf_record_event != "") {
             exit(execl("/usr/bin/perf", "perf", "record", "-F", "99", "-e", ermia::config::perf_record_event.c_str(),
                        "-p", parent_pid.str().c_str(), "--cpu", cpu_id.str().c_str(), nullptr));
           } else {
             exit(execl("/usr/bin/perf", "perf", "stat", "-B", "-e", "cache-references,cache-misses,cycles,instructions,branches,faults", 
                        "-p", parent_pid.str().c_str(), "--cpu", cpu_id.str().c_str(), nullptr));
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
        const uint32_t batch_to_run = st.range(3);
        const uint32_t task_to_run = batch_to_run * queue_size;

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<task<bool>> task_queue(queue_size);
        std::vector<ermia::OID> out_values(queue_size);
        std::vector<ermia::dia::coro_task_private::coro_stack> call_stacks(
                queue_size);

        // Run tasks with the same number of records
        uint32_t completed_task_cnt = 0;
        while (completed_task_cnt < task_to_run) {
            for(uint32_t i= 0; i < queue_size; i++) {
                task<bool> & coro_task = task_queue[i];
                if(coro_task.valid()){
                    if(!coro_task.done()) {
                        coro_task.resume();
                    } else {
                        completed_task_cnt++;
                        bool res = coro_task.get_return_value();
                        ASSERT(res);
                        coro_task = task<bool>(nullptr);
                    }
                }

                if(!coro_task.valid()) {
                    const Record & record = records[std::rand() % records.size()];
                    coro_task = tree_->search(
                        ermia::varstr(record.key.data(), record.key.size()),
                        out_values[i], cur_epoch, nullptr);

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
    ->Apply(PerfSingleThreadSearch::BatchArguments);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, AdvancedCoroBatched) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);
        const uint32_t batch_to_run = st.range(3);

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<task<bool>> task_queue(queue_size);
        std::vector<ermia::OID> out_values(queue_size);
        std::vector<ermia::dia::coro_task_private::coro_stack> call_stacks(queue_size);

        for(uint32_t b = 0; b < batch_to_run; b++) {
            for (uint32_t i = 0; i < queue_size; i++) {
                task<bool> & coro_task = task_queue[i];
                ASSERT(!coro_task.valid());

                const Record & record = records[std::rand() % records.size()];
                coro_task = tree_->search(
                    ermia::varstr(record.key.data(), record.key.size()),
                    out_values[i], cur_epoch, nullptr);
                coro_task.set_call_stack(&call_stacks[i]);
            }

            int finished = 0;
            while (finished < queue_size) {
                for (uint32_t i = 0; i < queue_size; i++) {
                    task<bool> & coro_task = task_queue[i];
                    if (!coro_task.valid()){
                        continue;
                    }

                    if (!coro_task.done()) {
                        coro_task.resume();
                    } else {
                        finished++;
                        bool res = coro_task.get_return_value();
                        ASSERT(res);
                        coro_task = task<bool>(nullptr);
                    }
                }
            }
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, AdvancedCoroBatched)
    ->Apply(PerfSingleThreadSearch::BatchArguments);

#else

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, Sequential) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);
        const uint32_t batch_to_run = st.range(3);

        constexpr ermia::epoch_num cur_epoch = 0;
        ermia::OID out_value;
        for(uint32_t b = 0; b < batch_to_run; b++) {
            [&] () -> void {
                for(uint32_t i = 0; i < queue_size; i++) {
                    const Record & record = records[std::rand() % records.size()];
                    bool res = tree_->search(ermia::varstr(record.key.data(), record.key.size()),
                                             out_value, cur_epoch, nullptr);
                    ASSERT(res && out_value == record.value);
                }
            }();
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, Sequential)
    ->Apply(PerfSingleThreadSearch::BatchArguments);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, SimpleCoro) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);
        const uint32_t batch_to_run = st.range(3);

        constexpr ermia::epoch_num cur_epoch = 0;
        ermia::SingleThreadedMasstree::threadinfo ti(cur_epoch);

        std::vector<generator_handle> generator_queue(queue_size, nullptr);
        std::vector<ermia::varstr> keys(queue_size);
        std::vector<ermia::OID> out_values(queue_size, 0);
        for(uint32_t b = 0; b < batch_to_run; b++) {
            for(uint32_t i = 0; i < queue_size; i++) {
                const Record & record = records[std::rand() % records.size()];
                keys[i] = ermia::varstr(record.key.data(), record.key.size());
                generator_queue[i] = tree_->search_coro(keys[i], out_values[i], ti, nullptr).get_handle();
            }

            int finished = 0;
            while(finished < queue_size) {
                for (auto & handle : generator_queue) {
                    if (handle) {
                        if(!handle.done()) {
                            handle.resume();
                        } else {
                            finished++;
                            handle.destroy();
                            handle = nullptr;
                        }
                    }
                }
            }
        }
    }
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, SimpleCoro)
    ->Apply(PerfSingleThreadSearch::BatchArguments);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, Amac) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);
        const uint32_t batch_to_run = st.range(3);

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<ermia::ConcurrentMasstree::AMACState> amac_states;
        std::vector<ermia::varstr> amac_params;
        amac_states.reserve(queue_size);
        amac_params.reserve(queue_size);

        for(uint32_t b = 0; b < batch_to_run; b++) {
            for(uint32_t i = 0; i < queue_size; i++) {
                const Record & record = records[std::rand() % records.size()];
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
    ->Apply(PerfSingleThreadSearch::BatchArguments);

#endif
