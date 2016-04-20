#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <sys/sysinfo.h>

#include "bench.h"

#include "../dbcore/rcu.h"
#include "../dbcore/sm-chkpt.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-rep.h"

#ifdef USE_JEMALLOC
//cannot include this header b/c conflicts with malloc.h
//#include <jemalloc/jemalloc.h>
extern "C" void malloc_stats_print(void (*write_cb)(void *, const char *), void *cbopaque, const char *opts);
extern "C" int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);
#endif
#ifdef USE_TCMALLOC
#include <gperftools/heap-profiler.h>
#endif

using namespace std;
using namespace util;

volatile bool running = true;
int verbose = 0;
uint64_t txn_flags = 0;
double scale_factor = 1.0;
uint64_t runtime = 30;
uint64_t ops_per_worker = 0;
int run_mode = RUNMODE_TIME;
int enable_parallel_loading = false;
int slow_exit = 0;
int retry_aborted_transaction = 0;
int backoff_aborted_transaction = 0;
int enable_chkpt = 0;

template <typename T>
static void
delete_pointers(const vector<T *> &pts)
{
  for (size_t i = 0; i < pts.size(); i++)
    delete pts[i];
}

template <typename T>
	static vector<T>
elemwise_sum(const vector<T> &a, const vector<T> &b)
{
	INVARIANT(a.size() == b.size());
	vector<T> ret(a.size());
	for (size_t i = 0; i < a.size(); i++)
		ret[i] = a[i] + b[i];
	return ret;
}

template <typename K, typename V>
	static void
map_agg(map<K, V> &agg, const map<K, V> &m)
{
	for (typename map<K, V>::const_iterator it = m.begin();
			it != m.end(); ++it)
		agg[it->first] += it->second;
}

// returns <free_bytes, total_bytes>
	static pair<uint64_t, uint64_t>
get_system_memory_info()
{
	struct sysinfo inf;
	sysinfo(&inf);
	return make_pair(inf.mem_unit * inf.freeram, inf.mem_unit * inf.totalram);
}

	static bool
clear_file(const char *name)
{
	ofstream ofs(name);
	ofs.close();
	return true;
}

static void
write_cb(void *p, const char *s) UNUSED;
	static void
write_cb(void *p, const char *s)
{
	const char *f = "jemalloc.stats";
	static bool s_clear_file UNUSED = clear_file(f);
	ofstream ofs(f, ofstream::app);
	ofs << s;
	ofs.flush();
	ofs.close();
}

void
bench_worker::run()
{
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    TXN::assign_reader_bitmap_entry();
#endif
    // XXX. RCU register/deregister should be the outer most one b/c
    // MM::deregister_thread could call cur_lsn inside
	RCU::rcu_register();
    MM::register_thread();
	RCU::rcu_start_tls_cache( 32, 100000 );
	on_run_setup();
	scoped_db_thread_ctx ctx(db, false);
	const workload_desc_vec workload = get_workload();
	txn_counts.resize(workload.size());
	barrier_a->count_down();
	barrier_b->wait_for();
	while (running && (run_mode != RUNMODE_OPS || ntxn_commits < ops_per_worker)) {
		double d = r.next_uniform();
		for (size_t i = 0; i < workload.size(); i++) {
			if ((i + 1) == workload.size() || d < workload[i].frequency) {
retry:
				timer t;
				const unsigned long old_seed = r.get_seed();
				const auto ret = workload[i].fn(this);

                if (likely(not rc_is_abort(ret))) {
					++ntxn_commits;
                    std::get<0>(txn_counts[i])++;
					latency_numer_us += t.lap();
					backoff_shifts >>= 1;
				} else {
					++ntxn_aborts;
                    std::get<1>(txn_counts[i])++;
                    if (ret._val == RC_ABORT_USER) {
                        std::get<3>(txn_counts[i])++;
                    } else {
                        std::get<2>(txn_counts[i])++;
                    }
                    switch (ret._val) {
                        case RC_ABORT_SERIAL: inc_ntxn_serial_aborts(); break;
                        case RC_ABORT_SI_CONFLICT: inc_ntxn_si_aborts(); break;
                        case RC_ABORT_RW_CONFLICT: inc_ntxn_rw_aborts(); break;
                        case RC_ABORT_INTERNAL: inc_ntxn_int_aborts(); break;
                        case RC_ABORT_PHANTOM: inc_ntxn_phantom_aborts(); break;
                        case RC_ABORT_USER: inc_ntxn_user_aborts(); break;
                        default: ALWAYS_ASSERT(false);
                    }
                    if (retry_aborted_transaction && not rc_is_user_abort(ret) && running) {
						if (backoff_aborted_transaction) {
							if (backoff_shifts < 63)
								backoff_shifts++;
							uint64_t spins = 1UL << backoff_shifts;
							spins *= 100; // XXX: tuned pretty arbitrarily
							while (spins) {
								nop_pause();
								spins--;
							}
						}
						r.set_seed(old_seed);
						goto retry;
					}
				}
				break;
			}
			d -= workload[i].frequency;
		}
	}
    MM::deregister_thread();
    RCU::rcu_deregister();
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
    TXN::deassign_reader_bitmap_entry();
#endif
}

void
bench_runner::run()
{
  // Invalidate my own tls_lsn_offset - I'm not doing transactions.
  // The benchmark's ctor should be the last place where it touches
  // the log in this thread.
  logmgr->set_tls_lsn_offset(~uint64_t{0});

  // load data, unless we recover from logs or is a backup server (recover from shipped logs)
  if (not sm_log::need_recovery && not sysconf::is_backup_srv) {
    const vector<bench_loader *> loaders = make_loaders();
    spin_barrier b(loaders.size());
    const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
    {
      scoped_timer t("dataloading", verbose);
      for (vector<bench_loader *>::const_iterator it = loaders.begin();
          it != loaders.end(); ++it) {
        (*it)->set_barrier(b);
        (*it)->start();
      }
      for (vector<bench_loader *>::const_iterator it = loaders.begin();
          it != loaders.end(); ++it)
        (*it)->join();
    }
    RCU::rcu_register();
    RCU::rcu_enter();
    volatile_write(MM::safesnap_lsn._val, logmgr->cur_lsn()._val);
    ALWAYS_ASSERT(MM::safesnap_lsn.offset());
    RCU::rcu_exit();
    RCU::rcu_deregister();

    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    if (verbose)
      cerr << "DB size: " << delta_mb << " MB" << endl;

    delete_pointers(loaders);
  }

  if (sysconf::num_backups) {
    std::cout << "[Primary] Expect " << sysconf::num_backups << " backups\n";
    ALWAYS_ASSERT(not sysconf::is_backup_srv);
    rep::start_as_primary();
    if (sysconf::wait_for_backups) {
      while (volatile_read(sysconf::num_active_backups) != volatile_read(sysconf::num_backups)) {}
      std::cout << "[Primary] " << sysconf::num_backups << " backups\n";
    }
    getchar();
  }

  map<string, size_t> table_sizes_before;

  const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();

  // Start checkpointer after database is ready
  if (enable_chkpt) {
    ASSERT(chkptmgr);
    chkptmgr->start_chkpt_thread();
  }

  // Persist the database
  logmgr->flush_cur_lsn();

  volatile_write(sysconf::loading, false);

  const vector<bench_worker *> workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it)
    (*it)->start();

  barrier_a.wait_for(); // wait for all threads to start up
  if (verbose) {
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->size();
      cerr << "table " << it->first << " size " << s << endl;
      table_sizes_before[it->first] = s;
    }
    cerr << "starting benchmark..." << endl;
  }
  timer t, t_nosync;
  barrier_b.count_down(); // bombs away!

  // Print some results every second
  if (run_mode == RUNMODE_TIME) {
    if (verbose) {
      uint64_t slept = 0;
      uint64_t last_commits = 0, last_aborts = 0;
      printf("[Throughput] Sec,Commits,Aborts\n");
      while (slept < runtime) {
        sleep(1);
        uint64_t sec_commits = 0, sec_aborts = 0;
        for (size_t i = 0; i < sysconf::worker_threads; i++) {
          sec_commits += workers[i]->get_ntxn_commits();
          sec_aborts += workers[i]->get_ntxn_aborts();
        }
        sec_commits -= last_commits;
        sec_aborts -= last_aborts;
        last_commits += sec_commits;
        last_aborts += sec_aborts;
        printf("[Throughput] %lu,%lu,%lu\n", slept+1, sec_commits, sec_aborts);
        slept++;
      };
    }
    else {
      sleep(runtime);
    }
    running = false;
  }

  // Persist whatever still left in the log buffer
  logmgr->flush();

  __sync_synchronize();
  for (size_t i = 0; i < sysconf::worker_threads; i++)
    workers[i]->join();
  const unsigned long elapsed_nosync = t_nosync.lap();
  size_t n_commits = 0;
  size_t n_aborts = 0;
  size_t n_user_aborts = 0;
  size_t n_int_aborts = 0;
  size_t n_si_aborts = 0;
  size_t n_serial_aborts = 0;
  size_t n_rw_aborts = 0;
  size_t n_phantom_aborts = 0;
  size_t n_query_commits= 0;
  uint64_t latency_numer_us = 0;
  for (size_t i = 0; i < sysconf::worker_threads; i++) {
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
    n_int_aborts += workers[i]->get_ntxn_int_aborts();
    n_user_aborts += workers[i]->get_ntxn_user_aborts();
    n_si_aborts += workers[i]->get_ntxn_si_aborts();
    n_serial_aborts += workers[i]->get_ntxn_serial_aborts();
    n_rw_aborts += workers[i]->get_ntxn_rw_aborts();
    n_phantom_aborts += workers[i]->get_ntxn_phantom_aborts();
    n_query_commits+= workers[i]->get_ntxn_query_commits();
    latency_numer_us += workers[i]->get_latency_numer_us();
  }

  const unsigned long elapsed = t.lap();
  const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
  const double agg_nosync_throughput = double(n_commits) / elapsed_nosync_sec;
  const double avg_nosync_per_core_throughput = agg_nosync_throughput / double(workers.size());

  const double elapsed_sec = double(elapsed) / 1000000.0;
  const double agg_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_throughput = agg_throughput / double(workers.size());

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate = agg_abort_rate / double(workers.size());

  const double agg_system_abort_rate = double(n_aborts - n_user_aborts) / elapsed_sec;
  const double agg_user_abort_rate = double(n_user_aborts) / elapsed_sec;
  const double agg_int_abort_rate = double(n_int_aborts) / elapsed_sec;
  const double agg_si_abort_rate = double(n_si_aborts) / elapsed_sec;
  const double agg_serial_abort_rate = double(n_serial_aborts) / elapsed_sec;
  const double agg_phantom_abort_rate = double(n_phantom_aborts) / elapsed_sec;
  const double agg_rw_abort_rate = double(n_rw_aborts) / elapsed_sec;

  // XXX(stephentu): latency currently doesn't account for read-only txns
  const double avg_latency_us =
    double(latency_numer_us) / double(n_commits);
  const double avg_latency_ms = avg_latency_us / 1000.0;

  tx_stat_map agg_txn_counts = workers[0]->get_txn_counts();
  for (size_t i = 1; i < workers.size(); i++) {
    auto &c = workers[i]->get_txn_counts();
    for (auto &t : c) {
      std::get<0>(agg_txn_counts[t.first]) += std::get<0>(t.second);
      std::get<1>(agg_txn_counts[t.first]) += std::get<1>(t.second);
      std::get<2>(agg_txn_counts[t.first]) += std::get<2>(t.second);
      std::get<3>(agg_txn_counts[t.first]) += std::get<3>(t.second);
    }
  }

  if (enable_chkpt)
      delete chkptmgr;

  if (verbose) {
    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    ssize_t size_delta = workers[0]->get_size_delta();
    for (size_t i = 1; i < workers.size(); i++)
      size_delta += workers[i]->get_size_delta();
    const double size_delta_mb = double(size_delta)/1048576.0;

    cerr << "--- table statistics ---" << endl;
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->size();
      const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
      cerr << "table " << it->first << " size " << it->second->size();
      if (delta < 0)
        cerr << " (" << delta << " records)" << endl;
      else
        cerr << " (+" << delta << " records)" << endl;
    }
    cerr << "--- benchmark statistics ---" << endl;
    cerr << "runtime: " << elapsed_sec << " sec" << endl;
    cerr << "memory delta: " << delta_mb  << " MB" << endl;
    cerr << "memory delta rate: " << (delta_mb / elapsed_sec)  << " MB/sec" << endl;
    cerr << "logical memory delta: " << size_delta_mb << " MB" << endl;
    cerr << "logical memory delta rate: " << (size_delta_mb / elapsed_sec) << " MB/sec" << endl;
    cerr << "agg_nosync_throughput: " << agg_nosync_throughput << " ops/sec" << endl;
    cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
    cerr << "avg_latency: " << avg_latency_ms << " ms" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
    cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(), agg_txn_counts.end()) << endl;

#if 0
	RCU::rcu_gc_info gc_info = RCU::rcu_get_gc_info();
	cerr << "--- RCU stat --- " << endl;
	cerr << "gc_passes: " << gc_info.gc_passes << endl;
	cerr << "objects_freed: " << gc_info.objects_freed << endl;
	cerr << "bytes_freed: " << gc_info.bytes_freed << endl;
	cerr << "objects_stashed : " << gc_info.objects_stashed<< endl;
	cerr << "bytes_stashed: " << gc_info.bytes_stashed << endl;
    cerr << "---------------------------------------" << endl;
#endif
  }

  /*
  ALWAYS_ASSERT(n_aborts == n_user_aborts +
                            n_int_aborts +
                            n_si_aborts +
                            n_serial_aborts +
                            n_rw_aborts +
                            n_phantom_aborts);
							*/

  // output for plotting script
  cout << "---------------------------------------\n";
  cout << agg_throughput << " commits/s, "
//       << avg_latency_ms << " "
       << agg_abort_rate << " total_aborts/s, "
       << agg_system_abort_rate << " system_aborts/s, "
       << agg_user_abort_rate << " user_aborts/s, "
       << agg_int_abort_rate << " internal aborts/s, "
       << agg_si_abort_rate << " si_aborts/s, " 
       << agg_serial_abort_rate << " serial_aborts/s, " 
       << agg_rw_abort_rate << " rw_aborts/s, "
       << agg_phantom_abort_rate << " phantom aborts/s."
	   << endl;
  cout << n_commits << " commits, "
	   << n_query_commits << " query_commits, "
       << n_aborts << " total_aborts, "
       << n_aborts - n_user_aborts << " system_aborts, "
       << n_user_aborts << " user_aborts, "
       << n_int_aborts << " internal_aborts, "
	   << n_si_aborts << " si_aborts, "
	   << n_serial_aborts << " serial_aborts, "
	   << n_rw_aborts << " rw_aborts, "
       << n_phantom_aborts << " phantom_aborts"
	   << endl;

  cout << "---------------------------------------\n";
  for (auto &c : agg_txn_counts) {
    cout << c.first << "\t"
         << std::get<0>(c.second) / (double)elapsed_sec << " commits/s\t"
         << std::get<1>(c.second) / (double)elapsed_sec << " aborts/s\t"
         << std::get<2>(c.second) / (double)elapsed_sec << " system aborts/s\t"
         << std::get<3>(c.second) / (double)elapsed_sec << " user aborts/s\n";
  }
  cout.flush();

  if (!slow_exit)
    return;

  map<string, uint64_t> agg_stats;
  for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
       it != open_tables.end(); ++it) {
    map_agg(agg_stats, it->second->clear());
    delete it->second;
  }
  if (verbose) {
    for (auto &p : agg_stats)
      cerr << p.first << " : " << p.second << endl;

  }
  open_tables.clear();

  delete_pointers(workers);
}

template <typename K, typename V>
struct map_maxer {
  typedef map<K, V> map_type;
  void
  operator()(map_type &agg, const map_type &m) const
  {
    for (typename map_type::const_iterator it = m.begin();
        it != m.end(); ++it)
      agg[it->first] = std::max(agg[it->first], it->second);
  }
};

const tx_stat_map
bench_worker::get_txn_counts() const
{
  tx_stat_map m;
  const workload_desc_vec workload = get_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}
