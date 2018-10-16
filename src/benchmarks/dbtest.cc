#include <iostream>
#include <fstream>
#include <sstream>
#include <utility>
#include <string>

#include <gflags/gflags.h>

#include "bench.h"
#include "../dbcore/sm-dia.h"
#include "../dbcore/rcu.h"
#include "../dbcore/sm-log-recover-impl.h"
#include "../dbcore/sm-rep.h"

#if defined(SSI) && defined(SSN)
#error "SSI + SSN?"
#endif

// Options that are shared by the primary and backup servers
DEFINE_bool(htt, true, "Whether the HW has hyper-threading enabled."
  "Ignored if auto-detection of physical cores succeeded.");
DEFINE_bool(verbose, true, "Verbose mode.");
DEFINE_string(benchmark, "tpcc", "Benchmark name: tpcc, tpce, or ycsb");
DEFINE_bool(dia, false, "Whether to use decoupled index access (DIA)");
DEFINE_string(benchmark_options, "", "Benchmark-specific opetions.");
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_uint64(node_memory_gb, 12, "GBs of memory to allocate per node.");
DEFINE_string(tmpfs_dir, "/dev/shm",
              "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_segment_mb, 8192, "Log segment size in MB.");
DEFINE_uint64(log_buffer_mb, 16, "Log buffer size in MB.");
DEFINE_bool(log_ship_by_rdma, false, "Whether to use RDMA for log shipping.");
DEFINE_bool(phantom_prot, false, "Whether to enable phantom protection.");
DEFINE_uint64(read_view_stat_interval_ms, 0,
  "Time interval between two outputs of read view LSN in milliseconds."
  "0 means do not output");
DEFINE_string(read_view_stat_file, "/dev/shm/ermia_read_view_stat",
  "Where to store all the read view LSN outputs. Recommend tmpfs.");
DEFINE_bool(print_cpu_util, false, "Whether to print CPU utilization.");
#if defined(SSN) || defined(SSI)
DEFINE_bool(safesnap, false,
            "Whether to use the safe snapshot (for SSI and SSN only).");
#endif
#ifdef SSN
DEFINE_string(ssn_read_opt_threshold, "0xFFFFFFFFFFFFFFFF",
              "Threshold for SSN's read optimization."
              "0 - don't track reads at all;"
              "0xFFFFFFFFFFFFFFFF - track all reads.");
#endif
#ifdef SSI
DEFINE_bool(ssi_read_only_opt, false,
            "Whether to enable SSI's read-only optimization."
            "Note: this is **not** safe snapshot.");
#endif

// Options specific to the primary
DEFINE_uint64(seconds, 10, "Duration to run benchmark in seconds.");
DEFINE_bool(parallel_loading, true, "Load data in parallel.");
DEFINE_bool(retry_aborted_transactions, false,
            "Whether to retry aborted transactions.");
DEFINE_bool(backoff_aborted_transactions, false,
            "Whether backoff when retrying.");
DEFINE_uint64(scale_factor, 1, "Scale factor.");
DEFINE_string(
    recovery_warm_up, "none",
    "Method to load tuples during recovery:"
    "none - don't load anything; lazy - load tuples using a background thread; "
    "eager - load everything to memory during recovery.");
DEFINE_bool(enable_chkpt, false, "Whether to enable checkpointing.");
DEFINE_uint64(chkpt_interval, 10, "Checkpoint interval in seconds.");
DEFINE_bool(null_log_device, false, "Whether to skip writing log records.");
DEFINE_bool(
    truncate_at_bench_start, false,
    "Whether truncate the log/chkpt file written before starting benchmark (save tmpfs space).");
DEFINE_bool(log_key_for_update, false,
            "Whether to store the key in update log records.");
// Group (pipelined) commit related settings. The daemon will flush the log
// buffer
// when the following happens, whichever is earlier:
// 1. queue is full; 2. the log buffer is half full; 3. after [timeout] seconds.
DEFINE_bool(group_commit, false, "Whether to enable group commit.");
DEFINE_uint64(group_commit_queue_length, 25000, "Group commit queue length");
DEFINE_uint64(group_commit_timeout, 5,
              "Group commit flush interval (in seconds).");
DEFINE_uint64(group_commit_size_kb, 4,
              "Group commit flush size interval in KB.");
DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_uint64(num_backups, 0, "Number of backup servers. For primary only.");
DEFINE_bool(wait_for_backups, true,
            "Whether to wait for backups to become online before starting "
            "transactions.");
DEFINE_string(persist_policy, "sync",
              "Up to what time must the shipped log be persistent."
              "pipelined - not persisted until the next shipping"
              "sync - request immediate ack on persistence from backups"
              "async - don't care at all, i.e., asynchronous log shipping");
DEFINE_bool(command_log, false, "Whether to use command logging.");
DEFINE_uint64(command_log_buffer_mb, 16, "Size of command log buffer.");
DEFINE_bool(log_ship_offset_replay, false, "Whether to parallel offset based replay.");

// Options specific to backups
DEFINE_bool(nvram_log_buffer, true, "Whether to use NVRAM-based log buffer.");
DEFINE_string(
    nvram_delay_type, "none",
    "How should NVRAM be emulated?"
    "none - no dealy, same as DRAM + non-volatile cache;"
    "clflush - use clflush to 'persist';"
    "clwb-emu - spin the equivalent number of cycles clflush would consume but"
    "without using clflush (so content not evicted), emulates clwb.");
DEFINE_string(
    log_ship_warm_up, "none",
    "Method to load tuples for log shipping:"
    "none - don't load anything; lazy - load tuples using a background thread; "
    "eager - load everything to memory after received log.");
DEFINE_string(primary_host, "",
              "Hostname of the primary server. For backups only.");
DEFINE_string(primary_port, "10000",
              "Port of the primary server for log shipping. For backups only.");
DEFINE_bool(quick_bench_start, false,
            "Whether to start benchmark right after loading, without waiting "
            "for user input. "
            "For backup servers only. Subject to -wait_for_primary.");
DEFINE_bool(
    wait_for_primary, true,
    "Whether to start benchmark only after the primary starts benchmark.");
DEFINE_string(replay_policy, "pipelined",
              "How log records should be replayed on backups."
              "pipelined - replay out of the critical path in a separate "
              "thread, but in sync with primary;"
              "bg - replay out of the critical path in background with a "
              "separate thread, async with the primary;"
              "sync - synchronous replay in the critical path; doesn't ack "
              "primary until finished replay;"
              "none - don't replay at all.");
DEFINE_bool(
    full_replay, false,
    "Create a version object directly and install it on the main arrays."
    "(for comparison and experimental purpose only).");
DEFINE_uint64(replay_threads, 0, "How many replay threads to use.");
DEFINE_bool(persist_nvram_on_replay, true,
            "Whether to issue clwb/clflush (if specified) during replay.");

static std::vector<std::string> split_ws(const std::string &s) {
  std::vector<std::string> r;
  std::istringstream iss(s);
  copy(std::istream_iterator<std::string>(iss), std::istream_iterator<std::string>(),
       std::back_inserter<std::vector<std::string>>(r));
  return r;
}

int main(int argc, char **argv) {
#ifndef NDEBUG
  std::cerr << "WARNING: benchmark built in DEBUG mode!!!" << std::endl;
#endif
  std::cerr << "PID: " << getpid() << std::endl;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  ermia::config::state = ermia::config::kStateLoading;
  ermia::config::print_cpu_util = FLAGS_print_cpu_util;
  ermia::config::htt_is_on = FLAGS_htt;
  ermia::config::verbose = FLAGS_verbose;
  ermia::config::node_memory_gb = FLAGS_node_memory_gb;
  ermia::config::threads = FLAGS_threads;
  ermia::config::tmpfs_dir = FLAGS_tmpfs_dir;
  ermia::config::log_dir = FLAGS_log_data_dir;
  ermia::config::log_segment_mb = FLAGS_log_segment_mb;
  ermia::config::log_buffer_mb = FLAGS_log_buffer_mb;
  ermia::config::phantom_prot = FLAGS_phantom_prot;
  ermia::config::recover_functor = new ermia::parallel_oid_replay(FLAGS_threads);
  ermia::config::log_ship_by_rdma = FLAGS_log_ship_by_rdma;

#if defined(SSI) || defined(SSN)
  ermia::config::enable_safesnap = FLAGS_safesnap;
#endif
#ifdef SSI
  ermia::config::enable_ssi_read_only_opt = FLAGS_ssi_read_only_opt;
#endif
#ifdef SSN
  ermia::config::ssn_read_opt_threshold =
      strtoul(FLAGS_ssn_read_opt_threshold.c_str(), nullptr, 16);
#endif

  ermia::config::primary_srv = FLAGS_primary_host;
  ermia::config::primary_port = FLAGS_primary_port;

  ermia::config::log_redo_partitions = ermia::rep::kMaxLogBufferPartitions;
  ermia::config::read_view_stat_interval_ms = FLAGS_read_view_stat_interval_ms;
  ermia::config::read_view_stat_file = FLAGS_read_view_stat_file;

  ermia::config::command_log = FLAGS_command_log;
  ermia::config::command_log_buffer_mb = FLAGS_command_log_buffer_mb;

  // Backup specific arguments
  if (ermia::config::is_backup_srv()) {
    ermia::config::nvram_log_buffer = FLAGS_nvram_log_buffer;
    if (FLAGS_nvram_delay_type == "clwb-emu") {
      ermia::config::CalibrateNvramDelay();
      ermia::config::nvram_delay_type = ermia::config::kDelayClwbEmu;
    } else if (FLAGS_nvram_delay_type == "clflush") {
      ermia::config::nvram_delay_type = ermia::config::kDelayClflush;
      ermia::config::cycles_per_byte = 0;
    } else {
      ALWAYS_ASSERT(FLAGS_nvram_delay_type == "none");
      ermia::config::nvram_delay_type = ermia::config::kDelayNone;
      ermia::config::cycles_per_byte = 0;
    }

    ermia::config::benchmark_seconds = ~uint32_t{0};  // Backups run forever
    ermia::config::quick_bench_start = FLAGS_quick_bench_start;
    ermia::config::wait_for_primary = FLAGS_wait_for_primary;
    ermia::config::log_ship_by_rdma = FLAGS_log_ship_by_rdma;
    ermia::config::persist_nvram_on_replay = FLAGS_persist_nvram_on_replay;
    if (FLAGS_log_ship_warm_up == "none") {
      ermia::config::log_ship_warm_up_policy = ermia::config::WARM_UP_NONE;
    } else if (FLAGS_log_ship_warm_up == "lazy") {
      ermia::config::log_ship_warm_up_policy = ermia::config::WARM_UP_LAZY;
    } else if (FLAGS_log_ship_warm_up == "eager") {
      ermia::config::log_ship_warm_up_policy = ermia::config::WARM_UP_EAGER;
    } else {
      LOG(FATAL) << "Invalid log shipping warm up policy: "
                 << FLAGS_log_ship_warm_up;
    }
    if (FLAGS_replay_policy == "bg") {
      ermia::config::replay_policy = ermia::config::kReplayBackground;
    } else if (FLAGS_replay_policy == "sync") {
      ermia::config::replay_policy = ermia::config::kReplaySync;
    } else if (FLAGS_replay_policy == "pipelined") {
      ermia::config::replay_policy = ermia::config::kReplayPipelined;
    } else if (FLAGS_replay_policy == "none") {
      ermia::config::replay_policy = ermia::config::kReplayNone;
    } else {
      LOG(FATAL) << "Invalid log shipping replay policy: "
                 << FLAGS_replay_policy;
    }
    ermia::config::full_replay = FLAGS_full_replay;

    ermia::config::replay_threads = FLAGS_replay_threads;
    LOG_IF(FATAL, ermia::config::threads < ermia::config::replay_threads);
    ermia::config::worker_threads = ermia::config::threads - ermia::config::replay_threads;

    ermia::RCU::rcu_register();
    ALWAYS_ASSERT(ermia::config::log_dir.size());
    ALWAYS_ASSERT(not ermia::logmgr);
    ALWAYS_ASSERT(not ermia::oidmgr);
    ermia::RCU::rcu_enter();
    ermia::sm_log::allocate_log_buffer();
    if (ermia::config::log_ship_by_rdma) {
      ermia::rep::start_as_backup_rdma();
    } else {
      ermia::rep::start_as_backup_tcp();
    }
  } else {
    ermia::config::benchmark_seconds = FLAGS_seconds;
    ermia::config::benchmark_scale_factor = FLAGS_scale_factor;
    ermia::config::retry_aborted_transactions = FLAGS_retry_aborted_transactions;
    ermia::config::backoff_aborted_transactions = FLAGS_backoff_aborted_transactions;
    ermia::config::null_log_device = FLAGS_null_log_device;
    ermia::config::truncate_at_bench_start = FLAGS_truncate_at_bench_start;

    ermia::config::replay_threads = 0;
    ermia::config::worker_threads = FLAGS_threads;

    ermia::config::group_commit = FLAGS_group_commit;
    ermia::config::group_commit_queue_length = FLAGS_group_commit_queue_length;
    ermia::config::group_commit_timeout = FLAGS_group_commit_timeout;
    ermia::config::group_commit_size_kb = FLAGS_group_commit_size_kb;
    ermia::config::group_commit_bytes = FLAGS_group_commit_size_kb * 1024;
    ermia::config::enable_chkpt = FLAGS_enable_chkpt;
    ermia::config::chkpt_interval = FLAGS_chkpt_interval;
    ermia::config::parallel_loading = FLAGS_parallel_loading;
    ermia::config::enable_gc = FLAGS_enable_gc;

    if (FLAGS_recovery_warm_up == "none") {
      ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_NONE;
    } else if (FLAGS_recovery_warm_up == "lazy") {
      ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_LAZY;
    } else if (FLAGS_recovery_warm_up == "eager") {
      ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_EAGER;
    } else {
      LOG(FATAL) << "Invalid recovery warm up policy: "
                 << FLAGS_recovery_warm_up;
    }

    ermia::config::log_ship_offset_replay = FLAGS_log_ship_offset_replay;
    ermia::config::log_key_for_update = FLAGS_log_key_for_update;
    ermia::config::num_backups = FLAGS_num_backups;
    ermia::config::wait_for_backups = FLAGS_wait_for_backups;
    if (FLAGS_persist_policy == "sync") {
      ermia::config::persist_policy = ermia::config::kPersistSync;
    } else if (FLAGS_persist_policy == "async") {
      ermia::config::persist_policy = ermia::config::kPersistAsync;
      LOG_IF(FATAL, ermia::config::nvram_log_buffer)
        << "Not supported: NVRAM + async ship";
    } else if (FLAGS_persist_policy == "pipelined") {
      ermia::config::persist_policy = ermia::config::kPersistPipelined;
    } else {
      LOG(FATAL) << "Invalid persist policy: "
                 << FLAGS_persist_policy;
    }
  }

  ermia::config::init();

  std::cerr << "CC: ";
#ifdef SSI
  std::cerr << "SSI";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap << std::endl;
  std::cerr << "  read-only optimization : " << ermia::config::enable_ssi_read_only_opt
       << std::endl;
#elif defined(SSN)
#ifdef RC
  std::cerr << "RC+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
       << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#else
  std::cerr << "SI+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
       << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#endif
#else
  std::cerr << "SI";
#endif
  std::cerr << std::endl;
  std::cerr << "  phantom-protection: " << ermia::config::phantom_prot << std::endl;

  std::cerr << "Settings and properties" << std::endl;
  std::cerr << "  dia               : " << FLAGS_dia << std::endl;
  std::cerr << "  node-memory       : " << ermia::config::node_memory_gb << "GB" << std::endl;
  std::cerr << "  num-threads       : " << ermia::config::worker_threads << std::endl;
  std::cerr << "  numa-nodes        : " << ermia::config::numa_nodes << std::endl;
  std::cerr << "  benchmark         : " << FLAGS_benchmark << std::endl;
#ifdef USE_VARINT_ENCODING
  std::cerr << "  var-encode        : yes" << std::endl;
#else
  std::cerr << "  var-encode        : no" << std::endl;
#endif
  std::cerr << "  print-cpu-util    : " << ermia::config::print_cpu_util << std::endl;
  std::cerr << "  log-dir           : " << ermia::config::log_dir << std::endl;
  std::cerr << "  tmpfs-dir         : " << ermia::config::tmpfs_dir << std::endl;
  std::cerr << "  log-buffer-mb     : " << ermia::config::log_buffer_mb << std::endl;
  std::cerr << "  log-ship-by-rdma  : " << ermia::config::log_ship_by_rdma << std::endl;
  std::cerr << "  logbuf-partitions : " << ermia::config::log_redo_partitions << std::endl;
  std::cerr << "  worker-threads    : " << ermia::config::worker_threads << std::endl;
  std::cerr << "  total-threads     : " << ermia::config::threads << std::endl;
  std::cerr << "  persist-policy    : " << FLAGS_persist_policy << std::endl;
  std::cerr << "  command-log       : " << ermia::config::command_log << std::endl;
  std::cerr << "  command-logbuf    : " << ermia::config::command_log_buffer_mb << "MB" << std::endl;

  std::cerr << "  masstree_internal_node_size: " << ermia::ConcurrentMasstree::InternalNodeSize()
       << std::endl;
  std::cerr << "  masstree_leaf_node_size    : " << ermia::ConcurrentMasstree::LeafNodeSize()
       << std::endl;
  std::cerr << "  read_view_stat_interval : " << ermia::config::read_view_stat_interval_ms
       << "ms" << std::endl;
  std::cerr << "  read_view_stat_file     : " << ermia::config::read_view_stat_file << std::endl;
  std::cerr << "  log_ship_offset_replay  : " << ermia::config::log_ship_offset_replay << std::endl;

  if (ermia::config::is_backup_srv()) {
    std::cerr << "  nvram-log-buffer  : " << ermia::config::nvram_log_buffer << std::endl;
    std::cerr << "  nvram-delay-type  : " << FLAGS_nvram_delay_type << std::endl;
    std::cerr << "  cycles-per-byte   : " << ermia::config::cycles_per_byte << std::endl;
    std::cerr << "  log-ship-warm-up  : " << FLAGS_log_ship_warm_up << std::endl;
    std::cerr << "  replay-policy     : " << FLAGS_replay_policy << std::endl;
    std::cerr << "  full-replay       : " << ermia::config::full_replay << std::endl;
    std::cerr << "  quick-bench-start : " << ermia::config::quick_bench_start << std::endl;
    std::cerr << "  wait-for-primary  : " << ermia::config::wait_for_primary << std::endl;
    std::cerr << "  replay-threads    : " << ermia::config::replay_threads << std::endl;
    std::cerr << "  persist-nvram-on-replay : " << ermia::config::persist_nvram_on_replay
         << std::endl;
  } else {
    std::cerr << "  parallel-loading: " << FLAGS_parallel_loading << std::endl;
    std::cerr << "  retry-txns        : " << FLAGS_retry_aborted_transactions
         << std::endl;
    std::cerr << "  backoff-txns      : " << FLAGS_backoff_aborted_transactions
         << std::endl;
    std::cerr << "  scale-factor      : " << FLAGS_scale_factor << std::endl;
    std::cerr << "  group-commit      : " << ermia::config::group_commit << std::endl;
    std::cerr << "  commit-queue      : " << ermia::config::group_commit_queue_length
         << std::endl;
    std::cerr << "  group-commit-size : " << ermia::config::group_commit_size_kb << "KB"
         << std::endl;
    std::cerr << "  recovery-warm-up  : " << FLAGS_recovery_warm_up << std::endl;
    std::cerr << "  log-key-for-update: " << ermia::config::log_key_for_update << std::endl;
    std::cerr << "  enable-chkpt      : " << ermia::config::enable_chkpt << std::endl;
    if (ermia::config::enable_chkpt) {
      std::cerr << "  chkpt-interval    : " << ermia::config::chkpt_interval << std::endl;
    }
    std::cerr << "  enable-gc         : " << ermia::config::enable_gc << std::endl;
    std::cerr << "  null-log-device   : " << ermia::config::null_log_device << std::endl;
    std::cerr << "  truncate-at-bench-start : " << ermia::config::truncate_at_bench_start << std::endl;
    std::cerr << "  num-backups       : " << ermia::config::num_backups << std::endl;
    std::cerr << "  wait-for-backups  : " << ermia::config::wait_for_backups << std::endl;
  }

  ermia::MM::prepare_node_memory();
  std::vector<std::string> bench_toks = split_ws(FLAGS_benchmark_options);
  argc = 1 + bench_toks.size();
  char *new_argv[argc];
  new_argv[0] = (char *)FLAGS_benchmark.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    new_argv[i] = (char *)bench_toks[i - 1].c_str();

  // Must have everything in config ready by this point
  ermia::config::sanity_check();
  ermia::Engine *db = NULL;
  db = new ermia::Engine();
  if (FLAGS_dia) {
    ermia::dia::Initialize();
  }
  void (*test_fn)(ermia::Engine*, int argc, char **argv) = NULL;
  if (FLAGS_benchmark == "ycsb") {
    test_fn = FLAGS_dia ? ycsb_dia_do_test : ycsb_do_test;
  } else if (FLAGS_benchmark == "tpcc") {
    test_fn = FLAGS_dia ? tpcc_dora_do_test : tpcc_do_test;
  } else if (FLAGS_benchmark == "tpce") {
    test_fn = tpce_do_test;
  } else {
    LOG(FATAL) << "Invalid benchmark: " << FLAGS_benchmark;
  }

  // FIXME(tzwang): the current thread doesn't belong to the thread pool, and
  // it could be on any node. But not all nodes will be used by benchmark
  // (i.e., config::numa_nodes) and so not all nodes will have memory pool. So
  // here run on the first NUMA node to ensure we got a place to allocate memory
  numa_run_on_node(0);
  test_fn(db, argc, new_argv);
  delete db;
  return 0;
}
