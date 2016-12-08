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

DEFINE_bool(verbose, true, "Verbose mode.");
DEFINE_bool(parallel_loading, true, "Load data in parallel.");
DEFINE_bool(retry_aborted_transactions, false, "Whether to retry aborted transactions.");
DEFINE_bool(backoff_aborted_transactions, false, "Whether backoff when retrying.");
DEFINE_string(benchmark, "tpcc", "Benchmark name: tpcc, tpce, or ycsb");
DEFINE_uint64(scale_factor, 1, "Scale factor.");
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_uint64(seconds, 10, "Duration to run benchmark in seconds.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_segment_mb, 8192, "Log segment size in MB.");
DEFINE_uint64(log_buffer_mb, 512, "Log buffer size in MB.");
DEFINE_string(recovery_warm_up, "none", "Method to load tuples during recovery:"
  "none - don't load anything; lazy - load tuples using a background thread; "
  "eager - load everything to memory during recovery.");
DEFINE_string(log_ship_warm_up, "none", "Method to load tuples for log shipping:"
  "none - don't load anything; lazy - load tuples using a background thread; "
  "eager - load everything to memory after received log.");
DEFINE_bool(log_ship_by_rdma, false, "Whether to use RDMA for log shipping.");
DEFINE_bool(log_ship_sync_redo, false, "Redo synchronously during log shipping.");
DEFINE_bool(log_ship_full_redo, false, "Whether to repeat index update during redo."
  "For experimenting with the benefit/cost of having indirection arrays only.");
DEFINE_bool(log_key_for_update, false, "Whether to store the key in update log records.");
DEFINE_bool(enable_chkpt, false, "Whether to enable checkpointing.");
DEFINE_uint64(chkpt_interval, 10, "Checkpoint interval in seconds.");
DEFINE_bool(null_log_device, false, "Whether to skip writing log records.");
DEFINE_bool(nvram_log_buffer, false, "Whether to use NVRAM-based log buffer.");

// Group (pipelined) commit related settings. The daemon will flush the log buffer
// when the following happens, whichever is earlier:
// 1. queue is full; 2. the log buffer is half full; 3. after [timeout] seconds.
DEFINE_bool(group_commit, false, "Whether to enable group commit.");
DEFINE_uint64(group_commit_queue_length, 5000, "Group commit queue length");
DEFINE_uint64(group_commit_timeout, 5, "Group commit flush interval (in seconds).");

DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_uint64(node_memory_gb, 12, "GBs of memory to allocate per node.");
DEFINE_string(tmpfs_dir, "/dev/shm", "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(parallel_recovery_by, "oid", "Parallelize recovery by OID (oid) or table (file).");
DEFINE_string(primary_host, "", "Hostname of the primary server. For backups only.");
DEFINE_string(primary_port, "10000", "Port of the primary server for log shipping. For backups only.");
DEFINE_uint64(num_backups, 0, "Number of backup servers. For primary only.");
DEFINE_bool(wait_for_backups, true,
  "Whether to wait for backups to become online before starting transactions.");
DEFINE_bool(phantom_prot, false, "Whether to enable phantom protection.");
#if defined(SSN) || defined(SSI)
DEFINE_bool(safesnap, false, "Whether to use the safe snapshot (for SSI and SSN only).");
#endif
#ifdef SSN
DEFINE_string(ssn_read_opt_threshold, "0xFFFFFFFFFFFFFFFF", "Threshold for SSN's read optimization."
  "0 - don't track reads at all;"
  "0xFFFFFFFFFFFFFFFF - track all reads.");
#endif
#ifdef SSI
DEFINE_bool(ssi_read_only_opt, false, "Whether to enable SSI's read-only optimization."
  "Note: this is **not** safe snapshot.");
#endif

DEFINE_string(benchmark_options, "", "Benchmark-specific opetions.");

static vector<string>
split_ws(const string &s)
{
  vector<string> r;
  istringstream iss(s);
  copy(istream_iterator<string>(iss),
       istream_iterator<string>(),
       back_inserter<vector<string>>(r));
  return r;
}

int
main(int argc, char **argv)
{
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  config::benchmark_seconds = FLAGS_seconds;
  config::benchmark_scale_factor = FLAGS_scale_factor;
  config::verbose = FLAGS_verbose;
  config::node_memory_gb = FLAGS_node_memory_gb;
  config::worker_threads = FLAGS_threads;
  config::retry_aborted_transactions = FLAGS_retry_aborted_transactions;
  config::backoff_aborted_transactions = FLAGS_backoff_aborted_transactions;

  config::tmpfs_dir = FLAGS_tmpfs_dir;
  config::log_dir = FLAGS_log_data_dir;
  config::log_segment_mb = FLAGS_log_segment_mb;
  config::log_buffer_mb = FLAGS_log_buffer_mb;
  config::null_log_device = FLAGS_null_log_device;
  config::group_commit = FLAGS_group_commit;
  config::group_commit_queue_length = FLAGS_group_commit_queue_length;
  config::group_commit_timeout = FLAGS_group_commit_timeout;

  config::enable_chkpt = FLAGS_enable_chkpt;
  config::chkpt_interval = FLAGS_chkpt_interval;

  config::parallel_loading = FLAGS_parallel_loading;
  config::enable_gc = FLAGS_enable_gc;
  config::nvram_log_buffer = FLAGS_nvram_log_buffer;
  if(config::nvram_log_buffer) {
    LOG(FATAL) << "Not supported: nvram_log_buffer";
  }
  if(FLAGS_parallel_recovery_by == "oid") {
    config::recover_functor = new parallel_oid_replay;
  } else if(FLAGS_parallel_recovery_by == "file") {
    config::recover_functor = new parallel_file_replay;
  } else {
    LOG(FATAL) << "Invalid parallel replay mode: " << FLAGS_parallel_recovery_by;
  }

  if(FLAGS_recovery_warm_up == "none" ) {
    config::recovery_warm_up_policy = config::WARM_UP_NONE;
  } else if(FLAGS_recovery_warm_up == "lazy") {
    config::recovery_warm_up_policy = config::WARM_UP_LAZY;
  } else if(FLAGS_recovery_warm_up == "eager") {
    config::recovery_warm_up_policy = config::WARM_UP_EAGER;
  } else {
    LOG(FATAL) << "Invalid recovery warm up policy: " << FLAGS_recovery_warm_up;
  }

  config::primary_srv = FLAGS_primary_host;
  config::primary_port = FLAGS_primary_port;
  config::log_key_for_update = FLAGS_log_key_for_update;
  config::log_ship_by_rdma = FLAGS_log_ship_by_rdma;
  config::log_ship_sync_redo = FLAGS_log_ship_sync_redo;
  config::log_ship_full_redo = FLAGS_log_ship_full_redo;
  config::num_backups = FLAGS_num_backups;
  config::wait_for_backups = FLAGS_wait_for_backups;
  if(FLAGS_log_ship_warm_up == "none" ) {
    config::log_ship_warm_up_policy = config::WARM_UP_NONE;
  } else if(FLAGS_log_ship_warm_up == "lazy") {
    config::log_ship_warm_up_policy = config::WARM_UP_LAZY;
  } else if(FLAGS_log_ship_warm_up == "eager") {
    config::log_ship_warm_up_policy = config::WARM_UP_EAGER;
  } else {
    LOG(FATAL) << "Invalid log shipping warm up policy: " << FLAGS_log_ship_warm_up;
  }

#if defined(SSI) || defined(SSN)
  config::enable_safesnap = FLAGS_safesnap;
#endif
#ifdef SSI
  config::enable_ssi_read_only_opt = FLAGS_ssi_read_only_opt;
#endif
#ifdef SSN
  config::ssn_read_opt_threshold = strtoul(FLAGS_ssn_read_opt_threshold.c_str(), nullptr, 16);
#endif

  config::init();

#ifndef NDEBUG
  cerr << "WARNING: benchmark built in DEBUG mode!!!" << endl;
#endif

  cerr << "CC : ";
#ifdef SSI
  cerr << "SSI";
#elif defined(SSN)
#ifdef RC
  cerr << "RC+SSN";
#elif defined RC_SPIN
  cerr << "RC_SPIN+SSN";
#else
  cerr << "SI+SSN";
#endif
#else
  cerr << "SI";
#endif
  cerr << endl;
  cerr << "PID: " << getpid() << endl;
  cerr << "Settings:" << endl;
  cerr << "  node-memory       : " << config::node_memory_gb << "GB" << endl;
  cerr << "  parallel-loading: " << FLAGS_parallel_loading << endl;
  cerr << "  retry-txns        : " << FLAGS_retry_aborted_transactions << endl;
  cerr << "  backoff-txns      : " << FLAGS_backoff_aborted_transactions << endl;
  cerr << "  benchmark         : " << FLAGS_benchmark << endl;
  cerr << "  scale-factor      : " << FLAGS_scale_factor << endl;
  cerr << "  num-threads       : " << config::worker_threads << endl;
  cerr << "  numa-nodes        : " << config::numa_nodes << endl;
#ifdef USE_VARINT_ENCODING
  cerr << "  var-encode        : yes" << endl;
#else
  cerr << "  var-encode        : no" << endl;
#endif
  cerr << "  phantom-protection: " << config::phantom_prot << endl;
  cerr << "  log-dir           : " << config::log_dir << endl;
  cerr << "  group-commit      : " << config::group_commit << endl;
  cerr << "  commit-queue      : " << config::group_commit_queue_length << endl;
  cerr << "  tmpfs-dir         : " << config::tmpfs_dir << endl;
  cerr << "  recovery-warm-up  : " << FLAGS_recovery_warm_up << endl;
  cerr << "  parallel-recovery : " << FLAGS_parallel_recovery_by << endl;
  cerr << "  log-ship-warm-up  : " << FLAGS_log_ship_warm_up << endl;
  cerr << "  log-ship-by-rdma  : " << config::log_ship_by_rdma << endl;
  cerr << "  log-ship-sync-redo: " << config::log_ship_sync_redo << endl;
  cerr << "  log-ship-full-redo: " << config::log_ship_full_redo << endl;
  cerr << "  log-key-for-update: " << config::log_key_for_update << endl;
  cerr << "  enable-chkpt      : " << config::enable_chkpt << endl;
  if(config::enable_chkpt) {
    cerr << "  chkpt-interval  : " << config::chkpt_interval << endl;
  }
  cerr << "  enable-gc         : " << config::enable_gc     << endl;
  cerr << "  null-log-device   : " << config::null_log_device << endl;
  cerr << "  num-backups       : " << config::num_backups   << endl;
  cerr << "  wait-for-backups  : " << config::wait_for_backups << endl;

#if defined(SSN) || defined(SSI)
  cerr << "  SSN/SSI safesnap  : " << config::enable_safesnap << endl;
#endif
#ifdef SSI
  cerr << "  SSI read-only opt : " << config::enable_ssi_read_only_opt << endl;
#endif
#ifdef SSN
  cerr << "  SSN read opt threshold: 0x" << hex << config::ssn_read_opt_threshold << dec << endl;
#endif

  cerr << "System properties:" << endl;
  cerr << "  btree_internal_node_size: " << concurrent_btree::InternalNodeSize() << endl;
  cerr << "  btree_leaf_node_size    : " << concurrent_btree::LeafNodeSize() << endl;

#ifdef TUPLE_PREFETCH
  cerr << "  tuple_prefetch          : yes" << endl;
#else
  cerr << "  tuple_prefetch          : no" << endl;
#endif

  MM::prepare_node_memory();
  vector<string> bench_toks = split_ws(FLAGS_benchmark_options);
  argc = 1 + bench_toks.size();
  char *new_argv[argc];
  new_argv[0] = (char*)FLAGS_benchmark.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    new_argv[i] = (char *) bench_toks[i - 1].c_str();

  // Must have everything in config ready by this point (ndb-wrapper's ctor will use them)
  config::sanity_check();
  config::calibrate_spin_cycles();
  ndb_wrapper *db = NULL;
  db = new ndb_wrapper();
  void (*test_fn)(ndb_wrapper *, int argc, char **argv) = NULL;
  if(FLAGS_benchmark == "ycsb") {
    test_fn = ycsb_do_test; 
  } else if(FLAGS_benchmark == "tpcc") {
    test_fn = tpcc_do_test;
  } else if(FLAGS_benchmark == "tpce") {
    test_fn = tpce_do_test;
  } else {
   LOG(FATAL) << "Invalid benchmark: " << FLAGS_benchmark;
  }

  test_fn(db, argc, new_argv);
  delete db;
  return 0;
}
