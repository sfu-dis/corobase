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
        ermia::config::node_memory_gb = 1;
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
                AWAIT tree_->insert(
                    ermia::varstr(record.key.data(), record.key.size()),
                    record.value,
                    &context_mock, nullptr, nullptr));
        }
    }

    void TearDown(const ::benchmark::State &state) {
        delete tree_;
        records.clear();

        int node = numa_node_of_cpu(sched_getcpu());

        munmap(node_memory[node], ermia::config::node_memory_gb);
        delete[] allocated_node_memory;
        delete[] node_memory;
    }

    std::vector<Record> records;
    ermia::SingleThreadedMasstree *tree_;
};


#ifdef USE_STATIC_COROUTINE

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

BENCHMARK_REGISTER_F(PerfSingleThreadSearch, Sequential)->Range(8, 8<<10);

#endif
