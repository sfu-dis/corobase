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

#include "../dbcore/sm-config.h"
#include "../dbcore/sm-alloc.h"
#include "../dbcore/sm-rep.h"
#include "../stats_server.h"
#include "bench.h"
#include "ndb_wrapper.h"
//#include "kvdb_wrapper.h"
//#include "kvdb_wrapper_impl.h"
#if !NO_MYSQL
#include "mysql_wrapper.h"
#endif

#if defined(USE_PARALLEL_SSI) && defined(USE_PARALLEL_SSN)
#error "SSI + SSN?"
#endif

using namespace std;
using namespace util;

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

void
heap_prefault()
{
    uint64_t size = (((uint64_t)1<<30)* sysconf::prefault_gig);
    if (not size)
        return;
    uint8_t* p = (uint8_t*)malloc(size);
    ALWAYS_ASSERT(p);
    ALWAYS_ASSERT(not mlock(p, size));
    mallopt (M_TRIM_THRESHOLD, -1);
    mallopt (M_MMAP_MAX, 0);
    free(p);
}

int
main(int argc, char **argv)
{
  abstract_db *db = NULL;
  void (*test_fn)(abstract_db *, int argc, char **argv) = NULL;
  string bench_type = "ycsb";
  char *curdir = get_current_dir_name();
  string basedir = curdir;
  string bench_opts;
  free(curdir);
  int saw_run_spec = 0;
  string stats_server_sockfile;
  // Do whatever we can to gather configs independent of cmdargs
  sysconf::init();

  while (1) {
    static struct option long_options[] =
    {
      {"verbose"                    , no_argument       , &verbose                   , 1}   ,
      {"parallel-loading"           , no_argument       , &enable_parallel_loading   , 1}   ,
      {"pin-cpus"                   , no_argument       , &pin_cpus                  , 1}   ,
      {"slow-exit"                  , no_argument       , &slow_exit                 , 1}   ,
      {"retry-aborted-transactions" , no_argument       , &retry_aborted_transaction , 1}   ,
      {"backoff-aborted-transactions" , no_argument     , &backoff_aborted_transaction , 1}   ,
      {"bench"                      , required_argument , 0                          , 'b'} ,
      {"scale-factor"               , required_argument , 0                          , 's'} ,
      {"num-threads"                , required_argument , 0                          , 't'} ,
      {"basedir"                    , required_argument , 0                          , 'B'} ,
      {"txn-flags"                  , required_argument , 0                          , 'f'} ,
      {"runtime"                    , required_argument , 0                          , 'r'} ,
      {"ops-per-worker"             , required_argument , 0                          , 'n'} ,
      {"bench-opts"                 , required_argument , 0                          , 'o'} ,
      {"log-dir"                    , required_argument , 0                          , 'l'} ,
      {"log-segment-mb"             , required_argument , 0                          , 'e'} ,
      {"log-buffer-mb"              , required_argument , 0                          , 'u'} ,
      {"warm-up"                    , required_argument , 0                          , 'w'} ,
      {"enable-chkpt"               , no_argument       , &enable_chkpt              , 1} ,
      {"stats-server-sockfile"      , required_argument , 0                          , 'x'} ,
      {"no-reset-counters"          , no_argument       , &no_reset_counters         , 1}   ,
      {"null-log-device"            , no_argument       , &sysconf::null_log_device  , 1} ,
      {"prefault-gig"               , required_argument , 0                          , 'p'},
      {"enable-gc"                  , no_argument       , &sysconf::enable_gc        , 1},
      {"tmpfs-dir"                  , required_argument , 0                          , 'm'},
      {"as-backup"                  , no_argument       , &sysconf::is_backup_srv    , 1},
      {"primary"                    , required_argument , 0                          , 'g'},
      {"wait-for-backups"           , no_argument       , &sysconf::wait_for_backups , 1},
      {"num-backups"                , required_argument , 0                          , 'a'},
#if defined(USE_PARALLEL_SSI) || defined(USE_PARALLEL_SSN)
      {"safesnap"                   , no_argument       , &sysconf::enable_safesnap  , 1},
#ifdef USE_PARALLEL_SSI
      {"ssi-read-only-opt"          , no_argument       , &sysconf::enable_ssi_read_only_opt, 1},
#endif
#ifdef USE_PARALLEL_SSN
      {"ssn-read-opt-threshold"     , required_argument , 0                          , 'h'},
#endif
#endif
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "b:s:t:B:f:r:n:o:m:l:e:u:w:x:p:m:g:a:", long_options, &option_index);
    if (c == -1)
      break;

    string *warm_up_policy = NULL;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();

    case 'a':
      sysconf::num_backups = strtoul(optarg, NULL, 10);
      break;

    case 'p':
      sysconf::prefault_gig = strtoul(optarg, NULL, 10);
      break;

    case 'g':
      sysconf::primary_srv = std::string(optarg);
      break;

    case 'b':
      bench_type = optarg;
      break;

    case 's':
      scale_factor = strtod(optarg, NULL);
      break;

    case 't':
      sysconf::worker_threads = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(sysconf::worker_threads > 0);
      break;

#ifdef USE_PARALLEL_SSN
    case 'h':
      sysconf::ssn_read_opt_threshold = strtoul(optarg, NULL, 16);
      break;
#endif

    case 'B':
      basedir = optarg;
      break;

    case 'f':
      txn_flags = strtoul(optarg, NULL, 10);
      break;

    case 'r':
      ALWAYS_ASSERT(!saw_run_spec);
      saw_run_spec = 1;
      runtime = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(runtime > 0);
      run_mode = RUNMODE_TIME;
      break;

    case 'w':
      warm_up_policy = new string(optarg);
      if (*warm_up_policy == "eager")
        sm_log::warm_up = sm_log::WU_EAGER;
      else if (*warm_up_policy == "lazy")
        sm_log::warm_up = sm_log::WU_LAZY;
      else
        sm_log::warm_up = sm_log::WU_NONE;
      break;

    case 'n':
      ALWAYS_ASSERT(!saw_run_spec);
      saw_run_spec = 1;
      ops_per_worker = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(ops_per_worker > 0);
      run_mode = RUNMODE_OPS;
      break;

    case 'o':
      bench_opts = optarg;
      break;

    case 'l':
      sysconf::log_dir = std::string(optarg);
      break;

    case 'm':
      sysconf::tmpfs_dir = string(optarg);
      break;

    case 'e':
      sysconf::log_segment_mb = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(sysconf::log_segment_mb);
      break;

    case 'u':
      sysconf::log_buffer_mb = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(sysconf::log_buffer_mb);
      break;

    case 'x':
      stats_server_sockfile = optarg;
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      abort();
    }
  }

 if (bench_type == "ycsb")
    test_fn = ycsb_do_test;
  else if (bench_type == "tpcc")
    test_fn = tpcc_do_test;
  else if (bench_type == "tpce")
    test_fn = tpce_do_test;
  else
    ALWAYS_ASSERT(false);

  if (sysconf::log_dir.empty()) {
    cerr << "[ERROR] no log dir specified" << endl;
    return 1;
  }

#ifndef ENABLE_EVENT_COUNTERS
  if (!stats_server_sockfile.empty()) {
    cerr << "[WARNING] --stats-server-sockfile with no event counters enabled is useless" << endl;
  }
#endif

#ifdef DEBUG
  cerr << "WARNING: benchmark built in DEBUG mode!!!" << endl;
#endif

#ifdef CHECK_INVARIANTS
  cerr << "WARNING: invariant checking is enabled - should disable for benchmark" << endl;
#ifdef PARANOID_CHECKING
  cerr << "  *** Paranoid checking is enabled ***" << endl;
#endif
#endif

  if (verbose) {
#ifdef USE_PARALLEL_SSI
    printf("System: SSI\n");
#elif defined(USE_PARALLEL_SSN)
#ifdef USE_READ_COMMITTED
    printf("System: RC+SSN\n");
#else
    printf("System: SI+SSN\n");
#endif
#else
    printf("System: SI\n");
#endif
    const unsigned long ncpus = coreid::num_cpus_online();
    cerr << "Database Benchmark:"                           << endl;
    cerr << "  pid: " << getpid()                           << endl;
    cerr << "settings:"                                     << endl;
    cerr << "  prefault-gig: " << sysconf::prefault_gig     << endl;
    cerr << "  par-loading : " << enable_parallel_loading   << endl;
    cerr << "  pin-cpus    : " << pin_cpus                  << endl;
    cerr << "  slow-exit   : " << slow_exit                 << endl;
    cerr << "  retry-txns  : " << retry_aborted_transaction << endl;
    cerr << "  backoff-txns: " << backoff_aborted_transaction << endl;
    cerr << "  bench       : " << bench_type                << endl;
    cerr << "  scale       : " << scale_factor              << endl;
    cerr << "  num-cpus    : " << ncpus                     << endl;
    cerr << "  num-threads : " << sysconf::worker_threads   << endl;
    cerr << "  numa-nodes  : " << sysconf::numa_nodes       << endl;
    cerr << "  basedir     : " << basedir                   << endl;
    cerr << "  txn-flags   : " << hexify(txn_flags)         << endl;
    if (run_mode == RUNMODE_TIME)
      cerr << "  runtime     : " << runtime                 << endl;
    else
      cerr << "  ops/worker  : " << ops_per_worker          << endl;
#ifdef USE_VARINT_ENCODING
    cerr << "  var-encode  : yes"                           << endl;
#else
    cerr << "  var-encode  : no"                            << endl;
#endif

#ifdef USE_JEMALLOC
    cerr << "  allocator   : jemalloc"                      << endl;
#elif defined USE_TCMALLOC
    cerr << "  allocator   : tcmalloc"                      << endl;
#elif defined USE_FLOW
    cerr << "  allocator   : flow"                          << endl;
#else
    cerr << "  allocator   : libc"                          << endl;
#endif
    cerr << "  tmpfs-dir   : " << sysconf::tmpfs_dir        << endl;
    cerr << "  log-dir     : " << sysconf::log_dir          << endl;
    cerr << "  log-segment-mb: " << sysconf::log_segment_mb   << endl;
    cerr << "  log-buffer-mb: " << sysconf::log_buffer_mb    << endl;
    cerr << "  warm-up     : ";
    if (sm_log::warm_up == sm_log::WU_NONE)
      cerr << "0";
    else if (sm_log::warm_up == sm_log::WU_LAZY)
      cerr << "lazy";
    else {
      ALWAYS_ASSERT(sm_log::warm_up == sm_log::WU_EAGER);
      cerr << "eager";
    }
    cerr << endl;
    cerr << "  enable-chkpt    : " << enable_chkpt           << endl;
    cerr << "  enable-gc       : " << sysconf::enable_gc     << endl;
    cerr << "  null-log-device : " << sysconf::null_log_device << endl;
    cerr << "  as-backup       : " << sysconf::is_backup_srv << endl;
    cerr << "  num-backups     : " << sysconf::num_backups   << endl;
    cerr << "  wait-for-backups: " << sysconf::wait_for_backups << endl;
    cerr << "  stats-server-sockfile: " << stats_server_sockfile << endl;

    cerr << "system properties:" << endl;
    cerr << "  btree_internal_node_size: " << concurrent_btree::InternalNodeSize() << endl;
    cerr << "  btree_leaf_node_size    : " << concurrent_btree::LeafNodeSize() << endl;

#ifdef TUPLE_PREFETCH
    cerr << "  tuple_prefetch          : yes" << endl;
#else
    cerr << "  tuple_prefetch          : no" << endl;
#endif

#ifdef BTREE_NODE_PREFETCH
    cerr << "  btree_node_prefetch     : yes" << endl;
#else
    cerr << "  btree_node_prefetch     : no" << endl;
#endif
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    cerr << "  SSN/SSI safe snapshot   : " << sysconf::enable_safesnap << endl;
#endif
#ifdef USE_PARALLEL_SSI
    cerr << "  SSI read-only optimization: " << sysconf::enable_ssi_read_only_opt << endl;
#endif
#ifdef USE_PARALLEL_SSN
    cerr << "  SSN read optimization threshold: 0x" << hex << sysconf::ssn_read_opt_threshold << dec << endl;
#endif
  }

  if (!stats_server_sockfile.empty()) {
    stats_server *srvr = new stats_server(stats_server_sockfile);
    thread(&stats_server::serve_forever, srvr).detach();
  }

  if (sysconf::wait_for_backups and sysconf::num_backups == 0) {
    std::cout << "[Primary] no backups\n";
    abort();
  }

  heap_prefault();
  vector<string> bench_toks = split_ws(bench_opts);
  argc = 1 + bench_toks.size();
  char *new_argv[argc];
  new_argv[0] = (char *) bench_type.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    new_argv[i] = (char *) bench_toks[i - 1].c_str();

  if (sysconf::is_backup_srv)
    rep::start_as_backup(sysconf::primary_srv);

  // Must have everything in CONF ready by this point (ndb-wrapper's ctor will use them)
  sysconf::sanity_check();
  db = new ndb_wrapper();
  test_fn(db, argc, new_argv);
  delete db;
  return 0;
}
