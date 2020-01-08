#include <gflags/gflags.h>
#include <benchmark/benchmark.h>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-coroutine.h>

DEFINE_bool(enable_perf, false, "enable linux perf");
DEFINE_string(perf_record_event, "", "linux perf event, empty means show all");

void ermia_init() {
    ermia::config::node_memory_gb = 10;
    ermia::config::num_backups = 0;

    // FIXME: hack to cover all the numa nodes
    ermia::config::numa_spread = true;
    ermia::config::physical_workers_only = false;
    ermia::config::threads = 4;

    ermia::config::init();
    ermia::MM::prepare_node_memory();
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_enable_perf) {
        std::vector<char *> nargv;
        for (uint32_t i = 0; i < argc; i++) {
            nargv.emplace_back(argv[i]);
        }

        // ensure each perf runs only 1 iteration
        char* ss = "--benchmark_min_time=0"; 
        nargv.emplace_back(ss);

        ermia::config::enable_perf = FLAGS_enable_perf;
        ermia::config::perf_record_event = FLAGS_perf_record_event;

        int nargc = nargv.size();
        ::benchmark::Initialize(&nargc, nargv.data());
    } else {
        ::benchmark::Initialize(&argc, argv);
    }

    ermia_init();
    // FIXME:
    ermia::dia::coro_task_private::memory_pool memory_pool;
    ::benchmark::RunSpecifiedBenchmarks();
}

