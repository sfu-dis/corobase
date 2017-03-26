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
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_buffer_mb, 512, "Log buffer size in MB.");
DEFINE_string(log_ship_warm_up, "none", "Method to load tuples for log shipping:"
  "none - don't load anything; lazy - load tuples using a background thread; "
  "eager - load everything to memory after received log.");
DEFINE_bool(log_ship_by_rdma, false, "Whether to use RDMA for log shipping.");
DEFINE_bool(log_ship_sync_redo, false, "Redo synchronously during log shipping.");
DEFINE_bool(log_ship_full_redo, false, "Whether to repeat index update during redo."
  "For experimenting with the benefit/cost of having indirection arrays only.");
DEFINE_bool(nvram_log_buffer, false, "Whether to use NVRAM-based log buffer.");

DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_uint64(node_memory_gb, 12, "GBs of memory to allocate per node.");
DEFINE_string(tmpfs_dir, "/dev/shm", "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(primary_host, "", "Hostname of the primary server. For backups only.");
DEFINE_string(primary_port, "10000", "Port of the primary server for log shipping. For backups only.");
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

DEFINE_string(benchmark, "tpcc", "Benchmark name: tpcc, tpce, or ycsb");
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
  google::ParseCommandLineFlags(&argc, &argv, true);

  config::verbose = FLAGS_verbose;
  config::node_memory_gb = FLAGS_node_memory_gb;
  config::worker_threads = FLAGS_threads;
  config::phantom_prot = FLAGS_phantom_prot;

  config::tmpfs_dir = FLAGS_tmpfs_dir;
  config::log_dir = FLAGS_log_data_dir;
  config::log_buffer_mb = FLAGS_log_buffer_mb;
  config::recover_functor = new parallel_oid_replay;

  config::enable_gc = FLAGS_enable_gc;
  config::nvram_log_buffer = FLAGS_nvram_log_buffer;
  if(config::nvram_log_buffer) {
    LOG(FATAL) << "Not supported: nvram_log_buffer";
  }

  config::primary_srv = FLAGS_primary_host;
  config::primary_port = FLAGS_primary_port;
  config::log_ship_by_rdma = FLAGS_log_ship_by_rdma;
  config::log_ship_sync_redo = FLAGS_log_ship_sync_redo;
  config::log_ship_full_redo = FLAGS_log_ship_full_redo;
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

  if(config::log_ship_by_rdma) {
    RCU::rcu_register();
    ALWAYS_ASSERT(config::log_dir.size());
    ALWAYS_ASSERT(not logmgr);
    ALWAYS_ASSERT(not oidmgr);
    RCU::rcu_enter();
    sm_log::allocate_log_buffer();
    rep::start_as_backup_rdma();
  } else {
    LOG(FATAL) << "TODO";
  }

  if(config::logbuf_partitions > rep::kMaxLogBufferPartitions) {
    LOG(FATAL) << "Too many log buffer partitions: max " << rep::kMaxLogBufferPartitions;
  }

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
  cerr << "  benchmark         : " << FLAGS_benchmark << endl;
  cerr << "  scale-factor      : " << config::benchmark_scale_factor << endl;
  cerr << "  num-threads       : " << config::worker_threads << endl;
  cerr << "  numa-nodes        : " << config::numa_nodes << endl;
#ifdef USE_VARINT_ENCODING
  cerr << "  var-encode        : yes" << endl;
#else
  cerr << "  var-encode        : no" << endl;
#endif
  cerr << "  phantom-protection: " << config::phantom_prot << endl;
  cerr << "  log-dir           : " << config::log_dir << endl;
  cerr << "  tmpfs-dir         : " << config::tmpfs_dir << endl;
  cerr << "  log-ship-warm-up  : " << FLAGS_log_ship_warm_up << endl;
  cerr << "  log-ship-by-rdma  : " << config::log_ship_by_rdma << endl;
  cerr << "  log-ship-sync-redo: " << config::log_ship_sync_redo << endl;
  cerr << "  log-ship-full-redo: " << config::log_ship_full_redo << endl;
  cerr << "  log-buffer-mb     : " << config::log_buffer_mb << endl;
  cerr << "  log-segment-mb    : " << config::log_segment_mb << endl;
  cerr << "  logbuf-partitions : " << config::logbuf_partitions << endl;
  cerr << "  enable-gc         : " << config::enable_gc     << endl;

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
