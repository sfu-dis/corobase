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
#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>

#include "bench.h"

#include "../dbcore/rcu.h"
#include "../dbcore/sm-chkpt.h"
#include "../dbcore/sm-cmd-log.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-index.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-log-recover-impl.h"
#include "../dbcore/sm-rep.h"

volatile bool running = true;
std::vector<bench_worker *> bench_runner::workers;
std::vector<bench_worker *> bench_runner::cmdlog_redoers;

void bench_worker::do_workload_function(uint32_t i) {
  ASSERT(workload.size() && cmdlog_redo_workload.size() == 0);
retry:
  util::timer t;
  const unsigned long old_seed = r.get_seed();
  const auto ret = workload[i].fn(this);
  if (finish_workload(ret, i, t)) {
    r.set_seed(old_seed);
    goto retry;
  }
}

void bench_worker::do_cmdlog_redo_workload_function(uint32_t i, void *param) {
  ASSERT(workload.size() == 0 && cmdlog_redo_workload.size());
retry:
  util::timer t;
  const unsigned long old_seed = r.get_seed();
  const auto ret = cmdlog_redo_workload[i].fn(this, param);
  if (finish_workload(ret, i, t)) {
    r.set_seed(old_seed);
    goto retry;
  }
}

uint32_t bench_worker::fetch_workload() {
  double d = r.next_uniform();
  for (size_t i = 0; i < workload.size(); i++) {
    if ((i + 1) == workload.size() || d < workload[i].frequency) {
        return i;
        break;
    }
    d -= workload[i].frequency;
  }

  // unreachable
  return 0;
}

bool bench_worker::finish_workload(rc_t ret, uint32_t workload_idx, util::timer &t) {
  if (!ret.IsAbort()) {
    ++ntxn_commits;
    std::get<0>(txn_counts[workload_idx])++;
    if (!ermia::config::is_backup_srv() && ermia::config::group_commit) {
      ermia::logmgr->enqueue_committed_xct(worker_id, t.get_start());
    } else {
      latency_numer_us += t.lap();
    }
    backoff_shifts >>= 1;
  } else {
    ++ntxn_aborts;
    std::get<1>(txn_counts[workload_idx])++;
    if (ret._val == RC_ABORT_USER) {
      std::get<3>(txn_counts[workload_idx])++;
    } else {
      std::get<2>(txn_counts[workload_idx])++;
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
    if (ermia::config::retry_aborted_transactions && !ret.IsUserAbort() && running) {
      if (ermia::config::backoff_aborted_transactions) {
        if (backoff_shifts < 63) backoff_shifts++;
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;  // XXX: tuned pretty arbitrarily
        while (spins) {
          NOP_PAUSE;
          spins--;
        }
      }
      return true;
    }
  }
  return false;
}

void bench_worker::MyWork(char *) {
  if (is_worker) {
    workload = get_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();

    while (running) {
      uint32_t workload_idx = fetch_workload();
      do_workload_function(workload_idx);
    }

  } else {
    cmdlog_redo_workload = get_cmdlog_redo_workload();
    txn_counts.resize(cmdlog_redo_workload.size());
    if (ermia::config::replay_policy == ermia::config::kReplayBackground) {
      ermia::CommandLog::cmd_log->BackgroundReplay(worker_id,
        std::bind(&bench_worker::do_cmdlog_redo_workload_function, this, std::placeholders::_1, std::placeholders::_2));
    } else if (ermia::config::replay_policy != ermia::config::kReplayNone) {
      ermia::CommandLog::cmd_log->BackupRedo(worker_id,
        std::bind(&bench_worker::do_cmdlog_redo_workload_function, this, std::placeholders::_1, std::placeholders::_2));
    }
  }
}

void bench_runner::create_files_task(char *) {
  ALWAYS_ASSERT(!ermia::sm_log::need_recovery && !ermia::config::is_backup_srv());
  // Allocate an FID for each index, set 2nd indexes to use
  // the primary index's record FID/array
  ASSERT(ermia::logmgr);
  ermia::RCU::rcu_enter();
  ermia::sm_tx_log *log = ermia::logmgr->new_tx_log();

  for (auto &nm : ermia::IndexDescriptor::name_map) {
    if (!nm.second->IsPrimary()) {
      continue;
    }
    nm.second->Initialize();
    log->log_index(nm.second->GetTupleFid(), nm.second->GetKeyFid(),
                   nm.second->GetName());
  }

  // Now all primary indexes have valid FIDs, handle secondary indexes
  for (auto &nm : ermia::IndexDescriptor::name_map) {
    if (nm.second->IsPrimary()) {
      continue;
    }
    nm.second->Initialize();
    // Note: using the same primary's FID here; recovery must know detect this
    log->log_index(nm.second->GetTupleFid(), nm.second->GetKeyFid(),
                   nm.second->GetName());
  }

  log->commit(nullptr);
  ermia::RCU::rcu_exit();
}

void bench_runner::run() {
  if (ermia::config::worker_threads ||
      (ermia::config::is_backup_srv() && ermia::config::replay_threads && ermia::config::command_log)) {
    // Get a thread to use benchmark-provided prepare(), which gathers
    // information about index pointers created by create_file_task.
    ermia::thread::Thread::Task runner_task =
      std::bind(&bench_runner::prepare, this, std::placeholders::_1);
    ermia::thread::Thread *runner_thread = ermia::thread::GetThread(true /* physical */);
    runner_thread->StartTask(runner_task);
    runner_thread->Join();
    ermia::thread::PutThread(runner_thread);
  }

  if (!ermia::RCU::rcu_is_registered()) {
    ermia::RCU::rcu_register();
  }

  // load data, unless we recover from logs or is a backup server (recover from
  // shipped logs)
  if (not ermia::sm_log::need_recovery && not ermia::config::is_backup_srv()) {
    std::vector<bench_loader *> loaders = make_loaders();
    {
      util::scoped_timer t("dataloading", ermia::config::verbose);
      uint32_t done = 0;
    process:
      for (uint i = 0; i < loaders.size(); i++) {
        auto *loader = loaders[i];
        if (loader and not loader->IsImpersonated() and
            loader->TryImpersonate()) {
          loader->Start();
        }
      }

      // Loop over existing loaders to scavenge and reuse available threads
      while (done < loaders.size()) {
        for (uint i = 0; i < loaders.size(); i++) {
          auto *loader = loaders[i];
          if (loader and loader->IsImpersonated() and loader->TryJoin()) {
            delete loader;
            loaders[i] = nullptr;
            done++;
            goto process;
          }
        }
      }
    }
    ermia::RCU::rcu_enter();
    ermia::volatile_write(ermia::MM::safesnap_lsn, ermia::logmgr->cur_lsn().offset());
    ALWAYS_ASSERT(ermia::MM::safesnap_lsn);

    // Persist the database
    ermia::logmgr->flush();
    if (ermia::config::enable_chkpt) {
      ermia::chkptmgr->do_chkpt();  // this is synchronous
    }
    ermia::RCU::rcu_exit();
  }
  ermia::RCU::rcu_deregister();

  // Start checkpointer after database is ready
  if (ermia::config::is_backup_srv()) {
    if (ermia::config::command_log &&
        ermia::config::replay_policy != ermia::config::kReplayNone &&
        ermia::config::replay_threads) {
      cmdlog_redoers = make_cmdlog_redoers();
      ermia::CommandLog::redoer_barrier = new spin_barrier(ermia::config::replay_threads);
      if (ermia::config::replay_policy == ermia::config::kReplayBackground) {
        std::thread bg(&ermia::CommandLog::CommandLogManager::BackgroundReplayDaemon, ermia::CommandLog::cmd_log);
        bg.detach();
      }
      for (auto &r : cmdlog_redoers) {
        while (!r->IsImpersonated()) {
          r->TryImpersonate();
        }
        r->Start();
      }
      LOG(INFO) << "Started all redoers";
    }

    // See if we need to wait for the 'go' signal from the primary
    if (ermia::config::wait_for_primary) {
      while (!ermia::config::IsForwardProcessing()) {
      }
    }
    if (ermia::config::log_ship_by_rdma && !ermia::config::quick_bench_start) {
      std::cout << "Press Enter to start benchmark" << std::endl;
      getchar();
    }
  } else {
    if (ermia::config::num_backups) {
      ALWAYS_ASSERT(not ermia::config::is_backup_srv());
      ermia::rep::start_as_primary();
      if (ermia::config::wait_for_backups) {
        while (ermia::volatile_read(ermia::config::num_active_backups) !=
               ermia::volatile_read(ermia::config::num_backups)) {
        }
      }
      std::cout << "[Primary] " << ermia::config::num_backups << " backups\n";
    }

    if (ermia::config::enable_chkpt) {
      ermia::chkptmgr->start_chkpt_thread();
    }
    ermia::volatile_write(ermia::config::state, ermia::config::kStateForwardProcessing);
  }

  // Start a thread that dumps read view LSN
  std::thread read_view_observer;
  if (ermia::config::read_view_stat_interval_ms) {
    read_view_observer = std::move(std::thread(measure_read_view_lsn));
  }

  if (ermia::config::worker_threads) {
    start_measurement();
  } else {
    LOG(INFO) << "No worker threads available to run benchmarks.";
    std::mutex trigger_lock;
    std::unique_lock<std::mutex> lock(trigger_lock);
    ermia::rep::backup_shutdown_trigger.wait(lock);
    if (ermia::config::replay_policy != ermia::config::kReplayNone &&
        ermia::config::replay_policy != ermia::config::kReplayBackground) {
      while (ermia::volatile_read(ermia::rep::replayed_lsn_offset) <
             ermia::volatile_read(ermia::rep::new_end_lsn_offset)) {
      }
    }
    std::cerr << "Shutdown successfully" << std::endl;
  }

  if (ermia::config::is_backup_srv() && ermia::config::command_log && cmdlog_redoers.size()) {
    tx_stat_map agg = cmdlog_redoers[0]->get_cmdlog_txn_counts();
    for (size_t i = 1; i < cmdlog_redoers.size(); i++) {
      cmdlog_redoers[i]->Join();
      auto &c = cmdlog_redoers[i]->get_cmdlog_txn_counts();
      for (auto &t : c) {
        std::get<0>(agg[t.first]) += std::get<0>(t.second);
        std::get<1>(agg[t.first]) += std::get<1>(t.second);
        std::get<2>(agg[t.first]) += std::get<2>(t.second);
        std::get<3>(agg[t.first]) += std::get<3>(t.second);
      }
    }
#ifndef __clang__
    std::cerr << "cmdlog txn breakdown: "
      << util::format_list(agg.begin(), agg.end()) << std::endl;
#endif
  }
  if (ermia::config::read_view_stat_interval_ms) {
    read_view_observer.join();
  }
}

void bench_runner::measure_read_view_lsn() {
  ermia::RCU::rcu_register();
  DEFER(ermia::RCU::rcu_deregister());
  std::ofstream out_file(ermia::config::read_view_stat_file, std::ios::out | std::ios::trunc);
  LOG_IF(FATAL, !out_file.is_open()) << "Read view stat file not open";
  DEFER(out_file.close());
  out_file << "Time,LSN,DLSN" << std::endl;
  while (!ermia::config::IsShutdown()) {
    while (ermia::config::IsForwardProcessing()) {
      ermia::RCU::rcu_enter();
      DEFER(ermia::RCU::rcu_exit());
      uint64_t lsn = 0;
      uint64_t dlsn = ermia::logmgr->durable_flushed_lsn().offset();
      if (ermia::config::is_backup_srv()) {
        lsn = ermia::rep::GetReadView();
      } else {
        lsn = ermia::logmgr->cur_lsn().offset();
      }
      uint64_t t = std::chrono::system_clock::now().time_since_epoch() /
                   std::chrono::milliseconds(1);
      out_file << t << "," << lsn << "," << dlsn << std::endl;
      usleep(ermia::config::read_view_stat_interval_ms * 1000);
    }
  }
}

void bench_runner::start_measurement() {
  workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (std::vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it) {
    while (!(*it)->IsImpersonated()) {
      (*it)->TryImpersonate();
    }
    (*it)->Start();
  }

  pid_t perf_pid;
  if (ermia::config::enable_perf) {
    std::cerr << "start perf..." << std::endl;

    std::stringstream parent_pid;
    parent_pid << getpid();

    pid_t pid = fork();
    // Launch profiler
    if (pid == 0) {
      if(ermia::config::perf_record_event != "") {
        exit(execl("/usr/bin/perf","perf","record", "-F", "99", "-e", ermia::config::perf_record_event.c_str(),
                   "-p", parent_pid.str().c_str(), nullptr));
      } else {
        exit(execl("/usr/bin/perf","perf","stat", "-B", "-e",  "cache-references,cache-misses,cycles,instructions,branches,faults", 
                   "-p", parent_pid.str().c_str(), nullptr));
      }
    } else {
      perf_pid = pid;
    }
  }

  barrier_a.wait_for();  // wait for all threads to start up
  std::map<std::string, size_t> table_sizes_before;
  if (ermia::config::verbose) {
    for (std::map<std::string, ermia::OrderedIndex *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->Size();
      std::cerr << "table " << it->first << " size " << s << std::endl;
      table_sizes_before[it->first] = s;
    }
    std::cerr << "starting benchmark..." << std::endl;
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
    ASSERT(ermia::config::print_cpu_util);
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

  if (ermia::config::truncate_at_bench_start) {
    ermia::rep::TruncateFilesInLogDir();
  }

  if (ermia::config::print_cpu_util) {
    printf("Sec,Commits,Aborts,CPU\n");
  } else {
    printf("Sec,Commits,Aborts\n");
  }

  util::timer t, t_nosync;
  barrier_b.count_down();  // bombs away!

  double total_util = 0;
  double sec_util = 0;
  auto gather_stats = [&]() {
    sleep(1);
    uint64_t sec_commits = 0, sec_aborts = 0;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      sec_commits += workers[i]->get_ntxn_commits();
      sec_aborts += workers[i]->get_ntxn_aborts();
    }
    sec_commits -= last_commits;
    sec_aborts -= last_aborts;
    last_commits += sec_commits;
    last_aborts += sec_aborts;

    if (ermia::config::print_cpu_util) {
      sec_util = get_cpu_util();
      total_util += sec_util;
      printf("%lu,%lu,%lu,%.2f%%\n", slept + 1, sec_commits, sec_aborts, sec_util);
    } else {
      printf("%lu,%lu,%lu\n", slept + 1, sec_commits, sec_aborts);
    }
    slept++;
  };

  // Backups run forever until told to stop.
  if (ermia::config::is_backup_srv()) {
    while (!ermia::config::IsShutdown()) {
      gather_stats();
    }
  } else {
    while (slept < ermia::config::benchmark_seconds) {
      gather_stats();
    }
  }
  running = false;

  ermia::volatile_write(ermia::config::state, ermia::config::kStateShutdown);
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    workers[i]->Join();
  }

  if (ermia::config::num_backups) {
    delete ermia::logmgr;
    if (ermia::config::command_log) {
      delete ermia::CommandLog::cmd_log;
      for (auto &t : bench_runner::cmdlog_redoers) {
        t->Join();
      }
    }
    ermia::rep::PrimaryShutdown();
  }

  const unsigned long elapsed_nosync = t_nosync.lap();

  if (ermia::config::enable_perf) {
    std::cerr << "stop perf..." << std::endl;
    kill(perf_pid, SIGINT);
    waitpid(perf_pid, nullptr, 0);
  }

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
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
    n_int_aborts += workers[i]->get_ntxn_int_aborts();
    n_user_aborts += workers[i]->get_ntxn_user_aborts();
    n_si_aborts += workers[i]->get_ntxn_si_aborts();
    n_serial_aborts += workers[i]->get_ntxn_serial_aborts();
    n_rw_aborts += workers[i]->get_ntxn_rw_aborts();
    n_phantom_aborts += workers[i]->get_ntxn_phantom_aborts();
    n_query_commits += workers[i]->get_ntxn_query_commits();
    if (ermia::config::is_backup_srv() || !ermia::config::group_commit) {
      latency_numer_us += workers[i]->get_latency_numer_us();
    }
  }

  if (!ermia::config::is_backup_srv() && ermia::config::group_commit) {
    latency_numer_us = ermia::sm_log_alloc_mgr::commit_queue::total_latency_us;
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
  if (ermia::config::is_backup_srv() && ermia::config::log_ship_offset_replay) {
    ermia::parallel_offset_replay *f = (ermia::parallel_offset_replay *)ermia::logmgr->get_backup_replay_functor();
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
  }

  if (ermia::config::enable_chkpt) delete ermia::chkptmgr;

  if (ermia::config::verbose) {
    std::cerr << "--- table statistics ---" << std::endl;
    for (std::map<std::string, ermia::OrderedIndex *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->Size();
      const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
      std::cerr << "table " << it->first << " size " << it->second->Size();
      if (delta < 0)
        std::cerr << " (" << delta << " records)" << std::endl;
      else
        std::cerr << " (+" << delta << " records)" << std::endl;
    }
    std::cerr << "--- benchmark statistics ---" << std::endl;
    std::cerr << "runtime: " << elapsed_sec << " sec" << std::endl;
    std::cerr << "cpu_util: " << total_util / elapsed_sec << "%" << std::endl;
    std::cerr << "agg_nosync_throughput: " << agg_nosync_throughput << " ops/sec"
         << std::endl;
    std::cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput
         << " ops/sec/core" << std::endl;
    std::cerr << "agg_throughput: " << agg_throughput << " ops/sec" << std::endl;
    std::cerr << "avg_per_core_throughput: " << avg_per_core_throughput
         << " ops/sec/core" << std::endl;
    std::cerr << "avg_latency: " << avg_latency_ms << " ms" << std::endl;
    std::cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << std::endl;
    std::cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate
         << " aborts/sec/core" << std::endl;
#ifndef __clang__
    std::cerr << "txn breakdown: " << util::format_list(agg_txn_counts.begin(),
                                                   agg_txn_counts.end()) << std::endl;
#endif
    if (ermia::config::is_backup_srv()) {
      std::cerr << "agg_replay_time: " << agg_replay_latency_ms << " ms" << std::endl;
      std::cerr << "agg_redo_batches: " << agg_redo_batches << std::endl;
      std::cerr << "ms_per_redo_batch: " << agg_replay_latency_ms / (double)agg_redo_batches << std::endl;
      std::cerr << "agg_redo_size: " << agg_redo_size << " bytes" << std::endl;
    }
  }

  // output for plotting script
  std::cout << "---------------------------------------\n";
  std::cout << agg_throughput << " commits/s, "
       //       << avg_latency_ms << " "
       << agg_abort_rate << " total_aborts/s, " << agg_system_abort_rate
       << " system_aborts/s, " << agg_user_abort_rate << " user_aborts/s, "
       << agg_int_abort_rate << " internal aborts/s, " << agg_si_abort_rate
       << " si_aborts/s, " << agg_serial_abort_rate << " serial_aborts/s, "
       << agg_rw_abort_rate << " rw_aborts/s, " << agg_phantom_abort_rate
       << " phantom aborts/s." << std::endl;
  std::cout << n_commits << " commits, " << n_query_commits << " query_commits, "
       << n_aborts << " total_aborts, " << n_aborts - n_user_aborts
       << " system_aborts, " << n_user_aborts << " user_aborts, "
       << n_int_aborts << " internal_aborts, " << n_si_aborts << " si_aborts, "
       << n_serial_aborts << " serial_aborts, " << n_rw_aborts << " rw_aborts, "
       << n_phantom_aborts << " phantom_aborts" << std::endl;

  std::cout << "---------------------------------------\n";
  for (auto &c : agg_txn_counts) {
    std::cout << c.first << "\t" << std::get<0>(c.second) / (double)elapsed_sec
         << " commits/s\t" << std::get<1>(c.second) / (double)elapsed_sec
         << " aborts/s\t" << std::get<2>(c.second) / (double)elapsed_sec
         << " system aborts/s\t" << std::get<3>(c.second) / (double)elapsed_sec
         << " user aborts/s\n";
  }
  std::cout.flush();
}

template <typename K, typename V>
struct map_maxer {
  typedef std::map<K, V> map_type;
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

const tx_stat_map bench_worker::get_cmdlog_txn_counts() const {
  tx_stat_map m;
  const cmdlog_redo_workload_desc_vec workload = get_cmdlog_redo_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}
