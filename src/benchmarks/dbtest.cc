#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <set>

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>

#include "../dbcore/sm-alloc.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-log-recover-impl.h"
#include "../dbcore/sm-rep.h"
#include "../dbcore/sm-thread.h"
#include "bench.h"
#include "ndb_wrapper.h"

#if defined(SSI) && defined(SSN)
#error "SSI + SSN?"
#endif

using namespace std;
using namespace util;

// Options that are shared by the primary and backup servers
DEFINE_bool(htt, true, "Whether the HW has hyper-threading enabled.");
DEFINE_bool(verbose, true, "Verbose mode.");
DEFINE_string(benchmark, "tpcc", "Benchmark name: tpcc, tpce, or ycsb");
DEFINE_string(benchmark_options, "", "Benchmark-specific opetions.");
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_uint64(node_memory_gb, 12, "GBs of memory to allocate per node.");
DEFINE_string(tmpfs_dir, "/dev/shm",
              "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_segment_mb, 8192, "Log segment size in MB.");
DEFINE_uint64(log_buffer_mb, 512, "Log buffer size in MB.");
DEFINE_bool(log_ship_by_rdma, false, "Whether to use RDMA for log shipping.");
DEFINE_bool(phantom_prot, false, "Whether to enable phantom protection.");
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
    fake_log_write, false,
    "Whether truncate the log file after writing to it (save tmpfs space).");
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
DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_uint64(num_backups, 0, "Number of backup servers. For primary only.");
DEFINE_bool(wait_for_backups, true,
            "Whether to wait for backups to become online before starting "
            "transactions.");
DEFINE_bool(pipelined_persist, false,
            "Whether *not* to wait for the 'persited' signal from backups "
            "immediately after shipping.");

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

static vector<string> split_ws(const string &s) {
  vector<string> r;
  istringstream iss(s);
  copy(istream_iterator<string>(iss), istream_iterator<string>(),
       back_inserter<vector<string>>(r));
  return r;
}

int main(int argc, char **argv) {
#ifndef NDEBUG
  cerr << "WARNING: benchmark built in DEBUG mode!!!" << endl;
#endif
  cerr << "PID: " << getpid() << endl;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  config::state = config::kStateLoading;
  config::htt_is_on = FLAGS_htt;
  config::verbose = FLAGS_verbose;
  config::node_memory_gb = FLAGS_node_memory_gb;
  config::threads = FLAGS_threads;
  config::tmpfs_dir = FLAGS_tmpfs_dir;
  config::log_dir = FLAGS_log_data_dir;
  config::log_segment_mb = FLAGS_log_segment_mb;
  config::log_buffer_mb = FLAGS_log_buffer_mb;
  config::phantom_prot = FLAGS_phantom_prot;
  config::recover_functor = new parallel_oid_replay;
  config::log_ship_by_rdma = FLAGS_log_ship_by_rdma;

#if defined(SSI) || defined(SSN)
  config::enable_safesnap = FLAGS_safesnap;
#endif
#ifdef SSI
  config::enable_ssi_read_only_opt = FLAGS_ssi_read_only_opt;
#endif
#ifdef SSN
  config::ssn_read_opt_threshold =
      strtoul(FLAGS_ssn_read_opt_threshold.c_str(), nullptr, 16);
#endif

  config::primary_srv = FLAGS_primary_host;
  config::primary_port = FLAGS_primary_port;

  // Backup specific arguments
  if (config::is_backup_srv()) {
    config::nvram_log_buffer = FLAGS_nvram_log_buffer;
    if (FLAGS_nvram_delay_type == "clwb-emu") {
      config::CalibrateNvramDelay();
      config::nvram_delay_type = config::kDelayClwbEmu;
    } else if (FLAGS_nvram_delay_type == "clflush") {
      config::nvram_delay_type = config::kDelayClflush;
      config::cycles_per_byte = 0;
    } else {
      ALWAYS_ASSERT(FLAGS_nvram_delay_type == "none");
      config::nvram_delay_type = config::kDelayNone;
      config::cycles_per_byte = 0;
    }

    config::benchmark_seconds = ~uint32_t{0};  // Backups run forever
    config::quick_bench_start = FLAGS_quick_bench_start;
    config::wait_for_primary = FLAGS_wait_for_primary;
    config::log_ship_by_rdma = FLAGS_log_ship_by_rdma;
    config::persist_nvram_on_replay = FLAGS_persist_nvram_on_replay;
    if (FLAGS_log_ship_warm_up == "none") {
      config::log_ship_warm_up_policy = config::WARM_UP_NONE;
    } else if (FLAGS_log_ship_warm_up == "lazy") {
      config::log_ship_warm_up_policy = config::WARM_UP_LAZY;
    } else if (FLAGS_log_ship_warm_up == "eager") {
      config::log_ship_warm_up_policy = config::WARM_UP_EAGER;
    } else {
      LOG(FATAL) << "Invalid log shipping warm up policy: "
                 << FLAGS_log_ship_warm_up;
    }
    if (FLAGS_replay_policy == "bg") {
      config::replay_policy = config::kReplayBackground;
    } else if (FLAGS_replay_policy == "sync") {
      config::replay_policy = config::kReplaySync;
    } else if (FLAGS_replay_policy == "pipelined") {
      config::replay_policy = config::kReplayPipelined;
    } else if (FLAGS_replay_policy == "none") {
      config::replay_policy = config::kReplayNone;
    } else {
      LOG(FATAL) << "Invalid log shipping replay policy: "
                 << FLAGS_replay_policy;
    }
    config::full_replay = FLAGS_full_replay;

    config::replay_threads = FLAGS_replay_threads;
    LOG_IF(FATAL, config::threads < config::replay_threads);
    config::worker_threads = config::threads - config::replay_threads;

    RCU::rcu_register();
    ALWAYS_ASSERT(config::log_dir.size());
    ALWAYS_ASSERT(not logmgr);
    ALWAYS_ASSERT(not oidmgr);
    RCU::rcu_enter();
    sm_log::allocate_log_buffer();
    if (config::log_ship_by_rdma) {
      rep::start_as_backup_rdma();
    } else {
      rep::start_as_backup_tcp();
    }
  } else {
    config::benchmark_seconds = FLAGS_seconds;
    config::benchmark_scale_factor = FLAGS_scale_factor;
    config::retry_aborted_transactions = FLAGS_retry_aborted_transactions;
    config::backoff_aborted_transactions = FLAGS_backoff_aborted_transactions;
    config::null_log_device = FLAGS_null_log_device;
    config::fake_log_write = FLAGS_fake_log_write;

    // At least four partitions to avoid trouble when sending half buffer with 2
    // partitions
    config::logbuf_partitions = FLAGS_threads > 2 ? FLAGS_threads : 4;
    if (config::logbuf_partitions > rep::kMaxLogBufferPartitions) {
      LOG(FATAL) << "Too many log buffer partitions: max "
                 << rep::kMaxLogBufferPartitions;
    }

    config::replay_threads = 0;
    config::worker_threads = FLAGS_threads;

    config::group_commit = FLAGS_group_commit;
    config::group_commit_queue_length = FLAGS_group_commit_queue_length;
    config::group_commit_timeout = FLAGS_group_commit_timeout;
    config::enable_chkpt = FLAGS_enable_chkpt;
    config::chkpt_interval = FLAGS_chkpt_interval;
    config::parallel_loading = FLAGS_parallel_loading;
    config::enable_gc = FLAGS_enable_gc;

    if (FLAGS_recovery_warm_up == "none") {
      config::recovery_warm_up_policy = config::WARM_UP_NONE;
    } else if (FLAGS_recovery_warm_up == "lazy") {
      config::recovery_warm_up_policy = config::WARM_UP_LAZY;
    } else if (FLAGS_recovery_warm_up == "eager") {
      config::recovery_warm_up_policy = config::WARM_UP_EAGER;
    } else {
      LOG(FATAL) << "Invalid recovery warm up policy: "
                 << FLAGS_recovery_warm_up;
    }

    config::log_key_for_update = FLAGS_log_key_for_update;
    config::num_backups = FLAGS_num_backups;
    config::wait_for_backups = FLAGS_wait_for_backups;
    config::pipelined_persist = FLAGS_pipelined_persist;
  }

  config::init();

  cerr << "CC: ";
#ifdef SSI
  cerr << "SSI";
  cerr << "  safe snapshot          : " << config::enable_safesnap << endl;
  cerr << "  read-only optimization : " << config::enable_ssi_read_only_opt
       << endl;
#elif defined(SSN)
#ifdef RC
  cerr << "RC+SSN";
  cerr << "  safe snapshot          : " << config::enable_safesnap << endl;
  cerr << "  read opt threshold     : 0x" << hex
       << config::ssn_read_opt_threshold << dec << endl;
#else
  cerr << "SI+SSN";
  cerr << "  safe snapshot          : " << config::enable_safesnap << endl;
  cerr << "  read opt threshold     : 0x" << hex
       << config::ssn_read_opt_threshold << dec << endl;
#endif
#else
  cerr << "SI";
#endif
  cerr << endl;
  cerr << "  phantom-protection: " << config::phantom_prot << endl;

  cerr << "Settings and properties" << endl;
  cerr << "  node-memory       : " << config::node_memory_gb << "GB" << endl;
  cerr << "  num-threads       : " << config::worker_threads << endl;
  cerr << "  numa-nodes        : " << config::numa_nodes << endl;
  cerr << "  benchmark         : " << FLAGS_benchmark << endl;
#ifdef USE_VARINT_ENCODING
  cerr << "  var-encode        : yes" << endl;
#else
  cerr << "  var-encode        : no" << endl;
#endif
  cerr << "  log-dir           : " << config::log_dir << endl;
  cerr << "  tmpfs-dir         : " << config::tmpfs_dir << endl;
  cerr << "  log-ship-by-rdma  : " << config::log_ship_by_rdma << endl;
  cerr << "  logbuf-partitions : " << config::logbuf_partitions << endl;
  cerr << "  worker-threads    : " << config::worker_threads << endl;
  cerr << "  total-threads     : " << config::threads << endl;

  cerr << "  btree_internal_node_size: " << concurrent_btree::InternalNodeSize()
       << endl;
  cerr << "  btree_leaf_node_size    : " << concurrent_btree::LeafNodeSize()
       << endl;

  if (config::is_backup_srv()) {
    cerr << "  nvram-log-buffer  : " << config::nvram_log_buffer << endl;
    cerr << "  nvram-delay-type  : " << FLAGS_nvram_delay_type << endl;
    cerr << "  cycles-per-byte   : " << config::cycles_per_byte << endl;
    cerr << "  log-ship-warm-up  : " << FLAGS_log_ship_warm_up << endl;
    cerr << "  replay-policy     : " << FLAGS_replay_policy << endl;
    cerr << "  full-replay       : " << config::full_replay << endl;
    cerr << "  quick-bench-start : " << config::quick_bench_start << endl;
    cerr << "  wait-for-primary  : " << config::wait_for_primary << endl;
    cerr << "  replay-threads    : " << config::replay_threads << endl;
    cerr << "  persist-nvram-on-replay : " << config::persist_nvram_on_replay
         << endl;
  } else {
    cerr << "  parallel-loading: " << FLAGS_parallel_loading << endl;
    cerr << "  retry-txns        : " << FLAGS_retry_aborted_transactions
         << endl;
    cerr << "  backoff-txns      : " << FLAGS_backoff_aborted_transactions
         << endl;
    cerr << "  scale-factor      : " << FLAGS_scale_factor << endl;
    cerr << "  group-commit      : " << config::group_commit << endl;
    cerr << "  commit-queue      : " << config::group_commit_queue_length
         << endl;
    cerr << "  recovery-warm-up  : " << FLAGS_recovery_warm_up << endl;
    cerr << "  log-key-for-update: " << config::log_key_for_update << endl;
    cerr << "  enable-chkpt      : " << config::enable_chkpt << endl;
    if (config::enable_chkpt) {
      cerr << "  chkpt-interval    : " << config::chkpt_interval << endl;
    }
    cerr << "  enable-gc         : " << config::enable_gc << endl;
    cerr << "  null-log-device   : " << config::null_log_device << endl;
    cerr << "  fake-log-write    : " << config::fake_log_write << endl;
    cerr << "  num-backups       : " << config::num_backups << endl;
    cerr << "  wait-for-backups  : " << config::wait_for_backups << endl;
    cerr << "  pipelined-persist : " << config::pipelined_persist << endl;
  }

  MM::prepare_node_memory();
  vector<string> bench_toks = split_ws(FLAGS_benchmark_options);
  argc = 1 + bench_toks.size();
  char *new_argv[argc];
  new_argv[0] = (char *)FLAGS_benchmark.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    new_argv[i] = (char *)bench_toks[i - 1].c_str();

  // Must have everything in config ready by this point
  config::sanity_check();
  ndb_wrapper *db = NULL;
  db = new ndb_wrapper();
  void (*test_fn)(ndb_wrapper *, int argc, char **argv) = NULL;
  if (FLAGS_benchmark == "ycsb") {
    test_fn = ycsb_do_test;
  } else if (FLAGS_benchmark == "tpcc") {
    test_fn = tpcc_do_test;
  } else if (FLAGS_benchmark == "tpce") {
    test_fn = tpce_do_test;
  } else {
    LOG(FATAL) << "Invalid benchmark: " << FLAGS_benchmark;
  }

  test_fn(db, argc, new_argv);
  delete db;
  return 0;
}
