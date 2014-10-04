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

#include "../allocator.h"
#include "../stats_server.h"
#include "bench.h"
#include "bdb_wrapper.h"
#include "ndb_wrapper.h"
#include "ndb_wrapper_impl.h"
//#include "kvdb_wrapper.h"
//#include "kvdb_wrapper_impl.h"
#if !NO_MYSQL
#include "mysql_wrapper.h"
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

/*
static size_t
parse_memory_spec(const string &s)
{
  std::string x(s);
  size_t mult = 1;
  if (x.back() == 'G') {
    mult = static_cast<size_t>(1) << 30;
    // IP: pop_back is supported from gcc4.7 onwards (and it is a c++11).
    //     Since it is pretty much the same with substr, I changed it to
    //     compile on my dev env.
    //x.pop_back();
    x = x.substr(0,x.size()-1);
  } else if (x.back() == 'M') {
    mult = static_cast<size_t>(1) << 20;
    //x.pop_back();
    x = x.substr(0,x.size()-1);    
  } else if (x.back() == 'K') {
    mult = static_cast<size_t>(1) << 10;
    //x.pop_back();
    x = x.substr(0,x.size()-1);
  }
  return strtoul(x.c_str(), nullptr, 10) * mult;
}
*/

int
main(int argc, char **argv)
{
  abstract_db *db = NULL;
  void (*test_fn)(abstract_db *, int argc, char **argv) = NULL;
  string bench_type = "ycsb";
  string db_type = "ndb-proto2";
  char *curdir = get_current_dir_name();
  string basedir = curdir;
  string bench_opts;
  free(curdir);
  int saw_run_spec = 0;
  int disable_gc = 0;
  int disable_snapshots = 0;
  string *log_dir = NULL;
  size_t log_segsize = 512 * 1024 * 1024; // log segment size
  size_t log_bufsize = 64 * 1024 * 1024;  // log buffer size
  string stats_server_sockfile;
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
      {"db-type"                    , required_argument , 0                          , 'd'} ,
      {"basedir"                    , required_argument , 0                          , 'B'} ,
      {"txn-flags"                  , required_argument , 0                          , 'f'} ,
      {"runtime"                    , required_argument , 0                          , 'r'} ,
      {"ops-per-worker"             , required_argument , 0                          , 'n'} ,
      {"bench-opts"                 , required_argument , 0                          , 'o'} ,
      {"numa-memory"                , required_argument , 0                          , 'm'} , // implies --pin-cpus
      {"log-dir"                    , required_argument , 0                          , 'l'} ,
      {"log-segsize"                , required_argument , 0                          , 'e'} ,
      {"log-bufsize"                , required_argument , 0                          , 'u'} ,
      {"disable-gc"                 , no_argument       , &disable_gc                , 1}   ,
      {"disable-snapshots"          , no_argument       , &disable_snapshots         , 1}   ,
      {"stats-server-sockfile"      , required_argument , 0                          , 'x'} ,
      {"no-reset-counters"          , no_argument       , &no_reset_counters         , 1}   ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "b:s:t:d:B:f:r:n:o:m:l:a:x:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'b':
      bench_type = optarg;
      break;

    case 's':
      scale_factor = strtod(optarg, NULL);
      ALWAYS_ASSERT(scale_factor > 0.0);
      break;

    case 't':
      nthreads = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(nthreads > 0);
      break;

    case 'd':
      db_type = optarg;
      break;

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

    case 'n':
      ALWAYS_ASSERT(!saw_run_spec);
      saw_run_spec = 1;
      ops_per_worker = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(ops_per_worker > 0);
      run_mode = RUNMODE_OPS;

    case 'o':
      bench_opts = optarg;
      break;

    case 'l':
      log_dir = new string(optarg);
      break;

    case 'e':
      log_segsize = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(log_segsize > 0);

    case 'u':
      log_bufsize = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(log_bufsize > 0);

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
// FIXME: tzwang: don't bother for ycsb now
/* if (bench_type == "ycsb")
    test_fn = ycsb_do_test;
  else */
  if (bench_type == "tpcc")
    test_fn = tpcc_do_test;
  else if (bench_type == "queue")
    test_fn = queue_do_test;
  else if (bench_type == "encstress")
    test_fn = encstress_do_test;
  else if (bench_type == "bid")
    test_fn = bid_do_test;
  else
    ALWAYS_ASSERT(false);

  if (!log_dir) {
    cerr << "[ERROR] no log dir specified" << endl;
    return 1;
  }

#ifndef ENABLE_EVENT_COUNTERS
  if (!stats_server_sockfile.empty()) {
    cerr << "[WARNING] --stats-server-sockfile with no event counters enabled is useless" << endl;
  }
#endif

#ifdef PROTO2_CAN_DISABLE_GC
  const set<string> has_gc({"ndb-proto1", "ndb-proto2"});
  if (disable_gc && !has_gc.count(db_type)) {
    cerr << "[ERROR] benchmark " << db_type
         << " does not have gc to disable" << endl;
    return 1;
  }
#else
  if (disable_gc) {
    cerr << "[ERROR] macro PROTO2_CAN_DISABLE_GC was not set, cannot disable gc" << endl;
    return 1;
  }
#endif

#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
  const set<string> has_snapshots({"ndb-proto2"});
  if (disable_snapshots && !has_snapshots.count(db_type)) {
    cerr << "[ERROR] benchmark " << db_type
         << " does not have snapshots to disable" << endl;
    return 1;
  }
#else
  if (disable_snapshots) {
    cerr << "[ERROR] macro PROTO2_CAN_DISABLE_SNAPSHOTS was not set, cannot disable snapshots" << endl;
    return 1;
  }
#endif

  if (db_type == "bdb") {
    const string cmd = "rm -rf " + basedir + "/db/*";
    // XXX(stephentu): laziness
    int ret UNUSED = system(cmd.c_str());
    db = new bdb_wrapper("db", bench_type + ".db");
  } else if (db_type == "ndb-proto1") {
    // XXX: hacky simulation of proto1
    db = new ndb_wrapper<transaction_proto2>(log_dir->c_str(), log_segsize, log_bufsize);
    transaction_proto2_static::set_hack_status(true);
    ALWAYS_ASSERT(transaction_proto2_static::get_hack_status());
#ifdef PROTO2_CAN_DISABLE_GC
    if (!disable_gc)
      transaction_proto2_static::InitGC();
#endif
  } else if (db_type == "ndb-proto2") {
    db = new ndb_wrapper<transaction_proto2>(log_dir->c_str(), log_segsize, log_bufsize);
    ALWAYS_ASSERT(!transaction_proto2_static::get_hack_status());
#ifdef PROTO2_CAN_DISABLE_GC
    if (!disable_gc)
      transaction_proto2_static::InitGC();
#endif
  } 
  // FIXME: tzwang: don't bother other benches for now...
  /*
  else if (db_type == "kvdb") {
    db = new kvdb_wrapper<true>;
  } else if (db_type == "kvdb-st") {
    db = new kvdb_wrapper<false>;
#if !NO_MYSQL
  } else if (db_type == "mysql") {
    string dbdir = basedir + "/mysql-db";
    db = new mysql_wrapper(dbdir, bench_type);
#endif
  } */
  else
    ALWAYS_ASSERT(false);

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
    const unsigned long ncpus = coreid::num_cpus_online();
    cerr << "Database Benchmark:"                           << endl;
    cerr << "  pid: " << getpid()                           << endl;
    cerr << "settings:"                                     << endl;
    cerr << "  par-loading : " << enable_parallel_loading   << endl;
    cerr << "  pin-cpus    : " << pin_cpus                  << endl;
    cerr << "  slow-exit   : " << slow_exit                 << endl;
    cerr << "  retry-txns  : " << retry_aborted_transaction << endl;
    cerr << "  backoff-txns: " << backoff_aborted_transaction << endl;
    cerr << "  bench       : " << bench_type                << endl;
    cerr << "  scale       : " << scale_factor              << endl;
    cerr << "  num-cpus    : " << ncpus                     << endl;
    cerr << "  num-threads : " << nthreads                  << endl;
    cerr << "  db-type     : " << db_type                   << endl;
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
    cerr << "  log-dir : " << *log_dir                      << endl;
    cerr << "  log-segsize : " << log_segsize               << endl;
    cerr << "  log-bufsize : " << log_bufsize               << endl;
    cerr << "  disable-gc : " << disable_gc                 << endl;
    cerr << "  disable-snapshots : " << disable_snapshots   << endl;
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

  }

  if (!stats_server_sockfile.empty()) {
    stats_server *srvr = new stats_server(stats_server_sockfile);
    thread(&stats_server::serve_forever, srvr).detach();
  }

  vector<string> bench_toks = split_ws(bench_opts);
  argc = 1 + bench_toks.size();
  char *new_argv[argc];
  new_argv[0] = (char *) bench_type.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    new_argv[i] = (char *) bench_toks[i - 1].c_str();
  test_fn(db, argc, new_argv);
  delete db;
  return 0;
}
