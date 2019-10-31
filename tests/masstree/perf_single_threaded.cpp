#include <benchmark/benchmark.h>

#include <numa.h>
#include <sched.h>
#include <sys/mman.h>
#include <array>
#include <vector>

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
        records = genRandRecords(state.range(0), key_length);

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
    }

    void TearDown(const ::benchmark::State &state) {
        delete tree_;
        records.clear();
    }

    static void InterleavedArguments(benchmark::internal::Benchmark *b) {
        std::vector<uint32_t> record_num = {10000, 1000000, 10000000};
        std::vector<uint32_t> key_size = {8};
        std::vector<uint32_t> group_size = {5, 15, 50};
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

    std::vector<Record> records;
    ermia::ConcurrentMasstree *tree_;
};


#ifdef USE_STATIC_COROUTINE

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, AdvancedCoro) (benchmark::State &st) {
    for (auto _ : st) {
        const uint32_t queue_size = st.range(2);

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<task<bool>> task_queue(queue_size);
        std::vector<ermia::OID> out_values(queue_size);

        uint32_t completed_task_cnt = 0;
        uint32_t next_record_idx = 0;
        while (completed_task_cnt < records.size()) {
            for(uint32_t i= 0; i < queue_size; i++) {
                task<bool> & coro_task = task_queue[i];
                if(coro_task.valid()){
                    if(!coro_task.done()) {
                        coro_task.resume();
                    } else {
                        completed_task_cnt++;
                        coro_task = task<bool>(nullptr);
                    }
                }

                if(!coro_task.valid() && next_record_idx < records.size()) {
                    const Record & record = records[next_record_idx++];
                    coro_task = tree_->search(
                            ermia::varstr(record.key.data(), record.key.size()),
                            out_values[i], cur_epoch, nullptr);
                    // initial suspended
                    coro_task.resume();
                }
            }
        }
    }    
}

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, AdvancedCoro)
    ->Apply(PerfSingleThreadSearch::InterleavedArguments);

#else

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, Sequential) (benchmark::State &st) {
    for (auto _ : st) {
        constexpr ermia::epoch_num cur_epoch = 0;
        ermia::OID out_value;
        for(const Record & record : records) {
            tree_->search(ermia::varstr(record.key.data(), record.key.size()),
                          out_value, cur_epoch, nullptr);
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
