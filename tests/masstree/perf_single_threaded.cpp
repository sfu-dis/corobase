#include <benchmark/benchmark.h>

#include <numa.h>
#include <sched.h>
#include <sys/mman.h>
#include <array>
#include <vector>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-coroutine.h>
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
        // allocate memory for node of current thread
        int node = numa_node_of_cpu(sched_getcpu());

        // hack, only allocate enough space to fit the current
        // thread node value (which is the index).
        ermia::config::numa_nodes = node + 1;
        allocated_node_memory = new uint64_t[ermia::config::numa_nodes];
        node_memory = new char *[ermia::config::numa_nodes];

        // pre-allocate memory on node for current thread node
        ermia::config::node_memory_gb = 2;
        allocated_node_memory[node] = 0;

        numa_set_preferred(node);
        node_memory[node] = (char *)mmap(
          nullptr, ermia::config::node_memory_gb * ermia::config::GB,
          PROT_READ | PROT_WRITE,
          MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE, -1, 0);

        // create masstree
        tree_ = new ermia::SingleThreadedMasstree();

        // generate records
        constexpr uint32_t key_length = 128;
        records = genRandRecords(state.range(0), 128);

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

        int node = numa_node_of_cpu(sched_getcpu());

        munmap(node_memory[node], ermia::config::node_memory_gb * ermia::config::GB);
        delete[] allocated_node_memory;
        delete[] node_memory;
    }

    std::vector<Record> records;
    ermia::SingleThreadedMasstree *tree_;
};


#ifdef USE_STATIC_COROUTINE

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, AdvancedCoro) (benchmark::State &st) {
    for (auto _ : st) {
        constexpr uint32_t queue_size = 3; // TODO move to benchmark::State 

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<task<bool>> task_queue(queue_size);
        std::vector<ermia::OID> out_values(queue_size);

        uint32_t completed_task_cnt = 0;
        uint32_t next_record_idx = 0;
        while (completed_task_cnt < records.size()) {
            for(uint32_t i= 0; i < queue_size; i++) {
                task<bool> & coro_task = task_queue[i];
                if(coro_task.valid() ){
                    if(coro_task.done()) {
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

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, AdvancedCoro)->Range(8<<4, 8<<10);

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

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, Sequential)->Range(8<<4, 8<<10);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, SimpleCoro) (benchmark::State &st) {
    for (auto _ : st) {
        constexpr uint32_t queue_size = 3; // TODO move to benchmark::State 

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

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, SimpleCoro)->Range(8<<4, 8<<10);

BENCHMARK_DEFINE_F(PerfSingleThreadSearch, Amac) (benchmark::State &st) {
    for (auto _ : st) {
        constexpr uint32_t parellel_amac_num = 3;

        constexpr ermia::epoch_num cur_epoch = 0;

        std::vector<ermia::SingleThreadedMasstree::AMACState> amac_states;
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

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, Amac)->Range(8<<4, 8<<10);

#endif
