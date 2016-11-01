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
#include "ndb_wrapper.h"

#include "../dbcore/rcu.h"
#include "../dbcore/sm-chkpt.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-file.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-log-recover-impl.h"
#include "../dbcore/sm-rep.h"

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

std::vector<bench_worker*> bench_runner::workers;

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
	ASSERT(a.size() == b.size());
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

void
bench_worker::my_work(char *)
{
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
          if (config::group_commit) {
            logmgr->enqueue_committed_xct(worker_id, t.get_start());
          } else {
            latency_numer_us += t.lap();
          }
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
								NOP_PAUSE;
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
}

void
bench_runner::create_files_task(char *)
{
  ALWAYS_ASSERT(config::log_dir.size());
  ALWAYS_ASSERT(not logmgr);
  ALWAYS_ASSERT(not oidmgr);
  RCU::rcu_enter();
  logmgr = sm_log::new_log(config::recover_functor, nullptr);
  ASSERT(oidmgr);
  RCU::rcu_exit();

  if (not sm_log::need_recovery && not config::is_backup_srv()) {
    // allocate an FID for each table
    for (auto &nm : sm_file_mgr::name_map) {
      auto fid = oidmgr->create_file(true);
      nm.second->fid = fid;
      ALWAYS_ASSERT(!nm.second->index);
      nm.second->index = new ndb_ordered_index(nm.first);
      nm.second->index->set_oid_array(fid);
      nm.second->main_array = oidmgr->get_array(fid);

      // Initialize the fid_map entries
      if (sm_file_mgr::fid_map[nm.second->fid] != nm.second) {
        sm_file_mgr::fid_map[nm.second->fid] = nm.second;
      }

      // Initialize the pdest array
      if (config::is_backup_srv()) {
          sm_file_mgr::get_file(fid)->init_pdest_array();
          std::cout << "[Backup] Created pdest array for FID " << fid << std::endl;
      }

      // log [table name, FID]
      ASSERT(logmgr);
      RCU::rcu_enter();
      sm_tx_log *log = logmgr->new_tx_log();
      log->log_fid(fid, nm.second->name);
      log->commit(NULL);
      RCU::rcu_exit();
    }
  }
}

void
bench_runner::run()
{
  // Now we should already have a list of registered tables in sm_file_mgr::name_map,
  // but all the index, oid_array fileds are empty; only the table name is available.

  // Get a thread to create the logmgr, index and FIDs backing each table
  auto* runner_thread = thread::get_thread();
  thread::sm_thread::task_t runner_task = std::bind(&bench_runner::create_files_task, this, std::placeholders::_1);
  runner_thread->start_task(runner_task);
  runner_thread->join();

  // Get a thread to use benchmark-provided prepare(), which gathers information about
  // index pointers created by create_file_task.
  runner_task = std::bind(&bench_runner::prepare, this, std::placeholders::_1);
  runner_thread->start_task(runner_task);
  runner_thread->join();
  thread::put_thread(runner_thread);

  // load data, unless we recover from logs or is a backup server (recover from shipped logs)
  if (not sm_log::need_recovery && not config::is_backup_srv()) {
    vector<bench_loader *> loaders = make_loaders();
    {
      scoped_timer t("dataloading", verbose);
      uint32_t done = 0;
    process:
      for (uint i = 0; i < loaders.size(); i++) {
        auto* loader = loaders[i];
        if (loader and not loader->is_impersonated() and loader->try_impersonate()) {
          loader->start();
        }
      }

      // Loop over existing loaders to scavenge and reuse available threads
      while (done < loaders.size()) {
        for (uint i = 0; i < loaders.size(); i++) {
          auto* loader = loaders[i];
          if (loader and loader->is_impersonated() and loader->try_join()) {
            delete loader;
            loaders[i] = nullptr;
            done++;
            goto process;
          }
        }
      }
    }
    RCU::rcu_register();
    RCU::rcu_enter();
    volatile_write(MM::safesnap_lsn, logmgr->cur_lsn().offset());
    ALWAYS_ASSERT(MM::safesnap_lsn);

    // Persist the database
    logmgr->flush();
    if(config::enable_chkpt) {
      chkptmgr->do_chkpt();  // this is synchronous
    }

    RCU::rcu_exit();
    RCU::rcu_deregister();
  }
  volatile_write(config::loading, false);

  // Start checkpointer after database is ready
  if(config::is_backup_srv()) {
    getchar();
  } else if(config::num_backups) {
    std::cout << "[Primary] Expect " << config::num_backups << " backups\n";
    ALWAYS_ASSERT(not config::is_backup_srv());
    rep::start_as_primary();
    if (config::wait_for_backups) {
      while (volatile_read(config::num_active_backups) != volatile_read(config::num_backups)) {}
      std::cout << "[Primary] " << config::num_backups << " backups\n";
    }
  }

  if(config::enable_chkpt) {
    chkptmgr->start_chkpt_thread();
  }

  workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it)
    (*it)->start();

  barrier_a.wait_for(); // wait for all threads to start up
  map<string, size_t> table_sizes_before;
  if (verbose) {
    for (map<string, ndb_ordered_index *>::iterator it = open_tables.begin();
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
        for (size_t i = 0; i < config::worker_threads; i++) {
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
  for (size_t i = 0; i < config::worker_threads; i++)
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
  for (size_t i = 0; i < config::worker_threads; i++) {
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
    workers[i]->~bench_worker();
  }

  if (config::enable_chkpt)
      delete chkptmgr;

  if (verbose) {
    cerr << "--- table statistics ---" << endl;
    for (map<string, ndb_ordered_index *>::iterator it = open_tables.begin();
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
  for (map<string, ndb_ordered_index *>::iterator it = open_tables.begin();
       it != open_tables.end(); ++it) {
    map_agg(agg_stats, it->second->clear());
    delete it->second;
  }
  if (verbose) {
    for (auto &p : agg_stats)
      cerr << p.first << " : " << p.second << endl;

  }
  open_tables.clear();
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
