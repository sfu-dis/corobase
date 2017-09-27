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
#include <sys/times.h>
#include <sys/vtimes.h>

#include "bench.h"
#include "ndb_wrapper.h"

#include "../dbcore/rcu.h"
#include "../dbcore/sm-chkpt.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-index.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-log-recover-impl.h"
#include "../dbcore/sm-rep.h"

using namespace std;
using namespace util;

volatile bool running = true;
std::vector<bench_worker *> bench_runner::workers;

void bench_worker::my_work(char *) {
  const workload_desc_vec workload = get_workload();
  txn_counts.resize(workload.size());
  barrier_a->count_down();
  barrier_b->wait_for();
  while (running) {
    double d = r.next_uniform();
    for (size_t i = 0; i < workload.size(); i++) {
      if ((i + 1) == workload.size() || d < workload[i].frequency) {
      retry:
        timer t;
        const unsigned long old_seed = r.get_seed();
        const auto ret = workload[i].fn(this);

        if (!rc_is_abort(ret)) {
          ++ntxn_commits;
          std::get<0>(txn_counts[i])++;
          if (config::num_active_backups > 0 || config::group_commit) {
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
            case RC_ABORT_SERIAL:
              inc_ntxn_serial_aborts();
              break;
            case RC_ABORT_SI_CONFLICT:
              inc_ntxn_si_aborts();
              break;
            case RC_ABORT_RW_CONFLICT:
              inc_ntxn_rw_aborts();
              break;
            case RC_ABORT_INTERNAL:
              inc_ntxn_int_aborts();
              break;
            case RC_ABORT_PHANTOM:
              inc_ntxn_phantom_aborts();
              break;
            case RC_ABORT_USER:
              inc_ntxn_user_aborts();
              break;
            default:
              ALWAYS_ASSERT(false);
          }
          if (config::retry_aborted_transactions && !rc_is_user_abort(ret) &&
              running) {
            if (config::backoff_aborted_transactions) {
              if (backoff_shifts < 63) backoff_shifts++;
              uint64_t spins = 1UL << backoff_shifts;
              spins *= 100;  // XXX: tuned pretty arbitrarily
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

void bench_runner::create_files_task(char *) {
  ALWAYS_ASSERT(!sm_log::need_recovery && !config::is_backup_srv());
  // Allocate an FID for each index, set 2nd indexes to use
  // the primary index's record FID/array
  ASSERT(logmgr);
  RCU::rcu_enter();
  sm_tx_log *log = logmgr->new_tx_log();

  for (auto &nm : IndexDescriptor::name_map) {
    if (!nm.second->IsPrimary()) {
      continue;
    }
    nm.second->Initialize();
    log->log_index(nm.second->GetTupleFid(), nm.second->GetKeyFid(),
                   nm.second->GetName());
  }

  // Now all primary indexes have valid FIDs, handle secondary indexes
  for (auto &nm : IndexDescriptor::name_map) {
    if (nm.second->IsPrimary()) {
      continue;
    }
    nm.second->Initialize();
    // Note: using the same primary's FID here; recovery must know detect this
    log->log_index(nm.second->GetTupleFid(), nm.second->GetKeyFid(),
                   nm.second->GetName());
  }

  log->commit(nullptr);
  RCU::rcu_exit();
}

void bench_runner::run() {
  if (config::is_backup_srv()) {
    rep::BackupStartReplication();
  } else {
    // Now we should already have a list of registered tables in
    // IndexDescriptor::name_map, but all the index, oid_array fileds are
    // empty; only the table name is available.  Create the logmgr here,
    // instead of in an sm-thread: recovery might want to utilize all the
    // worker_threads specified in config.
    RCU::rcu_register();
    ALWAYS_ASSERT(config::log_dir.size());
    ALWAYS_ASSERT(not logmgr);
    ALWAYS_ASSERT(not oidmgr);
    RCU::rcu_enter();
    sm_log::allocate_log_buffer();
    logmgr = sm_log::new_log(config::recover_functor, nullptr);
    sm_oid_mgr::create();
  }
  ALWAYS_ASSERT(logmgr);
  ALWAYS_ASSERT(oidmgr);

  LSN chkpt_lsn = logmgr->get_chkpt_start();
  if (config::enable_chkpt) {
    chkptmgr = new sm_chkpt_mgr(chkpt_lsn);
  } else {
    chkptmgr = nullptr;
  }

  // The backup will want to recover in another thread
  if (sm_log::need_recovery && !config::is_backup_srv()) {
    logmgr->recover();
  }

  RCU::rcu_exit();

  thread::sm_thread *runner_thread = nullptr;
  thread::sm_thread::task_t runner_task;
  if (!sm_log::need_recovery && !config::is_backup_srv()) {
    runner_thread = thread::get_thread();
    // Get a thread to create the index and FIDs backing each table
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread.
    runner_task = std::bind(&bench_runner::create_files_task, this,
                            std::placeholders::_1);
    runner_thread->start_task(runner_task);
    runner_thread->join();
  }

  if (config::worker_threads) {
    // Get a thread to use benchmark-provided prepare(), which gathers
    // information about index pointers created by create_file_task.
    runner_task = std::bind(&bench_runner::prepare, this, std::placeholders::_1);
    if (!runner_thread) {
      runner_thread = thread::get_thread();
    }
    runner_thread->start_task(runner_task);
    runner_thread->join();
    thread::put_thread(runner_thread);
  }

  // load data, unless we recover from logs or is a backup server (recover from
  // shipped logs)
  if (not sm_log::need_recovery && not config::is_backup_srv()) {
    vector<bench_loader *> loaders = make_loaders();
    {
      scoped_timer t("dataloading", config::verbose);
      uint32_t done = 0;
    process:
      for (uint i = 0; i < loaders.size(); i++) {
        auto *loader = loaders[i];
        if (loader and not loader->is_impersonated() and
            loader->try_impersonate()) {
          loader->start();
        }
      }

      // Loop over existing loaders to scavenge and reuse available threads
      while (done < loaders.size()) {
        for (uint i = 0; i < loaders.size(); i++) {
          auto *loader = loaders[i];
          if (loader and loader->is_impersonated() and loader->try_join()) {
            delete loader;
            loaders[i] = nullptr;
            done++;
            goto process;
          }
        }
      }
    }
    RCU::rcu_enter();
    volatile_write(MM::safesnap_lsn, logmgr->cur_lsn().offset());
    ALWAYS_ASSERT(MM::safesnap_lsn);

    // Persist the database
    logmgr->flush();
    if (config::enable_chkpt) {
      chkptmgr->do_chkpt();  // this is synchronous
    }
    RCU::rcu_exit();
  }
  RCU::rcu_deregister();

  // Start checkpointer after database is ready
  if (config::is_backup_srv()) {
    // See if we need to wait for the 'go' signal from the primary
    if (config::wait_for_primary) {
      while (!config::IsForwardProcessing()) {
      }
    }
    if (config::log_ship_by_rdma && !config::quick_bench_start) {
      std::cout << "Press Enter to start benchmark" << std::endl;
      getchar();
    }
  } else {
    if (config::num_backups) {
      ALWAYS_ASSERT(not config::is_backup_srv());
      rep::start_as_primary();
      if (config::wait_for_backups) {
        while (volatile_read(config::num_active_backups) !=
               volatile_read(config::num_backups)) {
        }
      }
      std::cout << "[Primary] " << config::num_backups << " backups\n";
    }

    if (config::enable_chkpt) {
      chkptmgr->start_chkpt_thread();
    }
    volatile_write(config::state, config::kStateForwardProcessing);
  }

  if (config::worker_threads) {
    start_measurement();
  } else {
    LOG(INFO) << "No worker threads available to run benchmarks.";
    std::mutex trigger_lock;
    std::unique_lock<std::mutex> lock(trigger_lock);
    rep::backup_shutdown_trigger.wait(lock);
    if (config::replay_policy != config::kReplayNone &&
        config::replay_policy != config::kReplayBackground) {
      while (volatile_read(rep::replayed_lsn_offset) <
             volatile_read(rep::new_end_lsn_offset)) {
      }
    }
    cerr << "Shutdown successfully" << std::endl;
  }
}

void bench_runner::measure_read_view_lsn() {
  rcu_register();
  DEFER(rcu_deregister());
  std::ofstream out_file(config::read_view_stat_file, std::ios::out | std::ios::trunc);
  LOG_IF(FATAL, !out_file.is_open()) << "Read view stat file not open";
  DEFER(out_file.close());
  out_file << "Time,LSN,DLSN" << std::endl;
  while (!config::IsShutdown()) {
    while (config::IsForwardProcessing()) {
      rcu_enter();
      DEFER(rcu_exit());
      uint64_t lsn = 0;
      uint64_t dlsn = logmgr->durable_flushed_lsn().offset();
      if (config::is_backup_srv()) {
        lsn = rep::GetReadView();
      } else {
        lsn = logmgr->cur_lsn().offset();
      }
      uint64_t t = std::chrono::system_clock::now().time_since_epoch() /
                   std::chrono::milliseconds(1);
      out_file << t << "," << std::hex << lsn << "," << dlsn << std::dec << std::endl;
      usleep(config::read_view_stat_interval_ms * 1000);
    }
  }
}

void bench_runner::start_measurement() {
  workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it) {
    while (!(*it)->is_impersonated()) {
      (*it)->try_impersonate();
    }
    (*it)->start();
  }

  barrier_a.wait_for();  // wait for all threads to start up
  map<string, size_t> table_sizes_before;
  if (config::verbose) {
    for (map<string, OrderedIndex *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->size();
      cerr << "table " << it->first << " size " << s << endl;
      table_sizes_before[it->first] = s;
    }
    cerr << "starting benchmark..." << endl;
  }

  // Print some results every second
  uint64_t slept = 0;
  uint64_t last_commits = 0, last_aborts = 0;

  // Print CPU utilization as well. Code adapted from:
  // https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
  FILE* file;
  struct tms timeSample;
  char line[128];

  clock_t lastCPU = times(&timeSample);
  clock_t lastSysCPU = timeSample.tms_stime;
  clock_t lastUserCPU = timeSample.tms_utime;
  uint32_t nprocs = std::thread::hardware_concurrency();

  file = fopen("/proc/cpuinfo", "r");
  fclose(file);

  auto get_cpu_util = [&]() {
    struct tms timeSample;
    clock_t now;
    double percent;

    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
      percent = -1.0;
    }
    else{
      percent = (timeSample.tms_stime - lastSysCPU) +
      (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      percent /= nprocs;
      percent *= 100;
    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
    return percent;
  };

  // Start a thread that dumps read view LSN
  std::thread read_view_observer;
  if (config::read_view_stat_interval_ms) {
    read_view_observer = std::move(std::thread(measure_read_view_lsn));
  }

  timer t, t_nosync;
  barrier_b.count_down();  // bombs away!
  printf("Sec,Commits,Aborts,CPU\n");

  double total_util = 0;
  double sec_util = 0;
  auto gather_stats = [&]() {
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

    sec_util = get_cpu_util();
    total_util += sec_util;

    printf("%lu,%lu,%lu,%.2f%%\n", slept + 1, sec_commits, sec_aborts, sec_util);
    slept++;
  };

  // Backups run forever until told to stop.
  if (config::is_backup_srv()) {
    while (!config::IsShutdown()) {
      gather_stats();
    }
  } else {
    while (slept < config::benchmark_seconds) {
      gather_stats();
    }
  }
  running = false;

  volatile_write(config::state, config::kStateShutdown);
  if (config::read_view_stat_interval_ms) {
    read_view_observer.join();
  }
  for (size_t i = 0; i < config::worker_threads; i++) {
    workers[i]->join();
  }

  if (config::num_backups) {
    delete logmgr;
    rep::PrimaryShutdown();
  }

  const unsigned long elapsed_nosync = t_nosync.lap();
  size_t n_commits = 0;
  size_t n_aborts = 0;
  size_t n_user_aborts = 0;
  size_t n_int_aborts = 0;
  size_t n_si_aborts = 0;
  size_t n_serial_aborts = 0;
  size_t n_rw_aborts = 0;
  size_t n_phantom_aborts = 0;
  size_t n_query_commits = 0;
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
    n_query_commits += workers[i]->get_ntxn_query_commits();
    latency_numer_us += workers[i]->get_latency_numer_us();
  }

  const unsigned long elapsed = t.lap();
  const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
  const double agg_nosync_throughput = double(n_commits) / elapsed_nosync_sec;
  const double avg_nosync_per_core_throughput =
      agg_nosync_throughput / double(workers.size());

  const double elapsed_sec = double(elapsed) / 1000000.0;
  const double agg_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_throughput =
      agg_throughput / double(workers.size());

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate =
      agg_abort_rate / double(workers.size());

  const double agg_system_abort_rate =
      double(n_aborts - n_user_aborts) / elapsed_sec;
  const double agg_user_abort_rate = double(n_user_aborts) / elapsed_sec;
  const double agg_int_abort_rate = double(n_int_aborts) / elapsed_sec;
  const double agg_si_abort_rate = double(n_si_aborts) / elapsed_sec;
  const double agg_serial_abort_rate = double(n_serial_aborts) / elapsed_sec;
  const double agg_phantom_abort_rate = double(n_phantom_aborts) / elapsed_sec;
  const double agg_rw_abort_rate = double(n_rw_aborts) / elapsed_sec;

  const double avg_latency_us = double(latency_numer_us) / double(n_commits);
  const double avg_latency_ms = avg_latency_us / 1000.0;

  uint64_t agg_latency_us = 0;
  uint64_t agg_redo_batches = 0;
  uint64_t agg_redo_size = 0;
  if (config::is_backup_srv() && config::persist_policy != config::kPersistAsync) {
    parallel_offset_replay *f = (parallel_offset_replay *)logmgr->get_backup_replay_functor();
    if (f) {
      for (auto &r : f->redoers) {
        if (agg_latency_us < r->redo_latency_us) {
          agg_latency_us = r->redo_latency_us;
        }
        agg_redo_batches += r->redo_batches;
        agg_redo_size += r->redo_size;
      }
    }
  }

  const double agg_replay_latency_ms = agg_latency_us / 1000.0;

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

  if (config::enable_chkpt) delete chkptmgr;

  if (config::verbose) {
    cerr << "--- table statistics ---" << endl;
    for (map<string, OrderedIndex *>::iterator it = open_tables.begin();
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
    cerr << "cpu_util: " << total_util / elapsed_sec << "%" << endl;
    cerr << "agg_nosync_throughput: " << agg_nosync_throughput << " ops/sec"
         << endl;
    cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput
         << " ops/sec/core" << endl;
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput
         << " ops/sec/core" << endl;
    cerr << "avg_latency: " << avg_latency_ms << " ms" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate
         << " aborts/sec/core" << endl;
    cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(),
                                             agg_txn_counts.end()) << endl;
    if (config::is_backup_srv()) {
      cerr << "agg_replay_time: " << agg_replay_latency_ms << " ms" << endl;
      cerr << "agg_redo_batches: " << agg_redo_batches << endl;
      cerr << "ms_per_redo_batch: " << agg_replay_latency_ms / (double)agg_redo_batches << endl;
      cerr << "agg_redo_size: " << agg_redo_size << " bytes" << endl;
      cerr << "received_log_size: " << rep::received_log_size << " bytes" << endl;
    } else {
      if (config::num_active_backups) {
        cerr << "log_size_for_ship: " << rep::log_size_for_ship << " bytes" << endl;
        cerr << "shipped_log_size: " << rep::shipped_log_size << " bytes" << endl;
      }
    }
  }

  // output for plotting script
  cout << "---------------------------------------\n";
  cout << agg_throughput << " commits/s, "
       //       << avg_latency_ms << " "
       << agg_abort_rate << " total_aborts/s, " << agg_system_abort_rate
       << " system_aborts/s, " << agg_user_abort_rate << " user_aborts/s, "
       << agg_int_abort_rate << " internal aborts/s, " << agg_si_abort_rate
       << " si_aborts/s, " << agg_serial_abort_rate << " serial_aborts/s, "
       << agg_rw_abort_rate << " rw_aborts/s, " << agg_phantom_abort_rate
       << " phantom aborts/s." << endl;
  cout << n_commits << " commits, " << n_query_commits << " query_commits, "
       << n_aborts << " total_aborts, " << n_aborts - n_user_aborts
       << " system_aborts, " << n_user_aborts << " user_aborts, "
       << n_int_aborts << " internal_aborts, " << n_si_aborts << " si_aborts, "
       << n_serial_aborts << " serial_aborts, " << n_rw_aborts << " rw_aborts, "
       << n_phantom_aborts << " phantom_aborts" << endl;

  cout << "---------------------------------------\n";
  for (auto &c : agg_txn_counts) {
    cout << c.first << "\t" << std::get<0>(c.second) / (double)elapsed_sec
         << " commits/s\t" << std::get<1>(c.second) / (double)elapsed_sec
         << " aborts/s\t" << std::get<2>(c.second) / (double)elapsed_sec
         << " system aborts/s\t" << std::get<3>(c.second) / (double)elapsed_sec
         << " user aborts/s\n";
  }
  cout.flush();
}

template <typename K, typename V>
struct map_maxer {
  typedef map<K, V> map_type;
  void operator()(map_type &agg, const map_type &m) const {
    for (typename map_type::const_iterator it = m.begin(); it != m.end(); ++it)
      agg[it->first] = std::max(agg[it->first], it->second);
  }
};

const tx_stat_map bench_worker::get_txn_counts() const {
  tx_stat_map m;
  const workload_desc_vec workload = get_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}
