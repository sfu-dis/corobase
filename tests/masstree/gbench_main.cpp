#include <benchmark/benchmark.h>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-thread.h>
#include <dbcore/sm-coroutine.h>

void ermia_init() {
    ermia::config::node_memory_gb = 10;
    ermia::config::num_backups = 0;

    // FIXME: hack to cover all the numa nodes
    ermia::config::numa_spread = true;
    ermia::config::physical_workers_only = true;
    ermia::config::threads = 4;

    ermia::thread::Initialize();
    ermia::config::init();
    ermia::MM::prepare_node_memory();
}

int main(int argc, char** argv) {
    const char PERF_EVENT[] = "--perf_record_event";
    const char ENABLE_PERF[] = "--enable_perf";
    bool FLAGS_enable_perf = false;
    std::string FLAGS_perf_record_event = "";
    for(uint32_t i = 1; i < argc; i++) {
        std::string curArg = std::string(argv[i]);
        if(curArg == ENABLE_PERF) {
            FLAGS_enable_perf = true;
            continue;
        }
        if(curArg.size() > strlen(PERF_EVENT) &&
           curArg.substr(0, strlen(PERF_EVENT)) == PERF_EVENT &&
           curArg[strlen(PERF_EVENT)] == '=') {
            FLAGS_perf_record_event = curArg.substr(strlen(PERF_EVENT) + 1);
        }
    }
    std::cout << "perf event: " <<  FLAGS_perf_record_event << std::endl;

    if (FLAGS_enable_perf) {
        std::vector<char *> nargv;
        for (uint32_t i = 0; i < argc; i++) {
            nargv.emplace_back(argv[i]);
        }

        // ensure each perf runs only 1 iteration
        std::string ss = "--benchmark_min_time=0";
        nargv.emplace_back(const_cast<char *>(ss.c_str()));

        ermia::config::enable_perf = FLAGS_enable_perf;
        ermia::config::perf_record_event = FLAGS_perf_record_event;

        int nargc = nargv.size();
        ::benchmark::Initialize(&nargc, nargv.data());
    } else {
        ::benchmark::Initialize(&argc, argv);
    }

    ermia_init();
    ::benchmark::RunSpecifiedBenchmarks();
}

