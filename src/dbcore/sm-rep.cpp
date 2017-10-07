#include "sm-cmd-log.h"
#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
std::condition_variable backup_shutdown_trigger;
uint64_t log_redo_partition_bounds[kMaxLogBufferPartitions] CACHE_ALIGNED;

// for primary server only
std::vector<int> backup_sockfds CACHE_ALIGNED;
std::mutex backup_sockfds_mutex CACHE_ALIGNED;
std::thread primary_async_ship_daemon;
uint64_t shipped_log_size CACHE_ALIGNED;
uint64_t log_size_for_ship CACHE_ALIGNED;

// For backups only
ReplayPipelineStage *pipeline_stages CACHE_ALIGNED;
uint64_t replayed_lsn_offset CACHE_ALIGNED;
uint64_t persisted_nvram_size CACHE_ALIGNED;
uint64_t persisted_nvram_offset CACHE_ALIGNED;
uint64_t new_end_lsn_offset CACHE_ALIGNED;
uint64_t *global_persisted_lsn_ptr CACHE_ALIGNED;
int replay_bounds_fd CACHE_ALIGNED;
std::condition_variable bg_replay_cond CACHE_ALIGNED;
std::mutex bg_replay_mutex CACHE_ALIGNED;
uint64_t received_log_size CACHE_ALIGNED;
std::mutex async_ship_mutex CACHE_ALIGNED;
std::condition_variable async_ship_cond CACHE_ALIGNED;

void start_as_primary() {
  shipped_log_size = 0;
  log_size_for_ship = 0;
  memset(log_redo_partition_bounds, 0,
         sizeof(uint64_t) * kMaxLogBufferPartitions);
  ALWAYS_ASSERT(not config::is_backup_srv());
  if (config::log_ship_by_rdma) {
    std::thread t(primary_daemon_rdma);
    t.detach();
  } else {
    std::thread t(primary_daemon_tcp);
    t.detach();
  }
}

void LogFlushDaemon() {
  new_end_lsn_offset = 0;
  rcu_register();
  DEFER(rcu_deregister());
  rcu_enter();
  DEFER(rcu_exit());
  uint64_t dlsn = logmgr->durable_flushed_lsn().offset();
  while (true) {
    uint64_t lsn = volatile_read(new_end_lsn_offset);
    // Use another variable to record the durable flushed LSN offset
    // here, as the backup daemon might change a new sgment ID's
    // start_offset when it needs to create new one after receiving
    // data from the primary. That might cause the durable_flushed_lsn
    // call to fail when the adjusted start_offset makes the sid think
    // it doesn't contain the LSN.
    if (lsn > dlsn) {
      logmgr->BackupFlushLog(lsn);
      dlsn = lsn;
    }
  }
}

// Daemon for shipping log out of the commit path (ie async log shipping)
void PrimaryAsyncShippingDaemon() {
  ALWAYS_ASSERT(config::persist_policy == config::kPersistAsync);
  uint64_t start_offset = logmgr->durable_flushed_lsn().offset();
  char *buf = (char*)malloc(config::group_commit_bytes);
  // FIXME(tzwang): support segment boundary crossing
  auto* sid = logmgr->get_offset_segment(start_offset);
  int fd = logmgr->open_segment_for_read(sid);
  while (!config::IsShutdown()) {
    while (logmgr->durable_flushed_lsn().offset() - start_offset < config::group_commit_bytes) {
      std::unique_lock<std::mutex> lock(async_ship_mutex);
      async_ship_cond.wait(lock);
    }
    auto off = sid->offset(start_offset);
    uint32_t size = os_pread(fd, buf, config::group_commit_bytes, off);
    start_offset += size;
    rep::primary_ship_log_buffer_all(buf, size, false, 0);
  }
  os_close(fd);
  free(buf);
}

// The major routine that controls background async replay
void BackupBackgroundReplay() {
  LOG_IF(FATAL, config::command_log);
  rcu_register();
  DEFER(rcu_deregister());
  LOG_IF(FATAL, replay_bounds_fd <= 0);
  const uint32_t read_size = config::log_redo_partitions * sizeof(uint64_t);
  off_t off = 0;
  LSN start_lsn = logmgr->durable_flushed_lsn();
  LOG_IF(FATAL, start_lsn == INVALID_LSN) << "Invalid start LSN";
  LSN end_lsn = logmgr->durable_flushed_lsn();

  if (config::persist_policy == config::kPersistAsync) {
    while (!config::IsShutdown()) {
      rcu_enter();
      DEFER(rcu_exit());
      end_lsn = logmgr->durable_flushed_lsn();
      if (end_lsn.offset() > start_lsn.offset()) {
        if (end_lsn.offset() - start_lsn.offset() > config::group_commit_bytes) {
          uint64_t end_offset = start_lsn.offset() + config::group_commit_bytes;
          end_lsn = LSN::make(end_offset, start_lsn.segment(), INVALID_SIZE_CODE);
        }
        DLOG(INFO) << "To replay "  << std::hex << start_lsn.offset() << "-"
          << end_lsn.offset() << std::dec;
        // backup_redo_log_by_oid returns the last log block's starting LSN, so
        // that when we hit an incomplete log block we know where to start in
        // the next round. This is needed only for OID parallel replay (the
        // offset based replay already have primary generated boundaries to
        // follow).
        LSN next_start_lsn = logmgr->backup_redo_log_by_oid(start_lsn, end_lsn);
        LOG_IF(FATAL, next_start_lsn.offset() < start_lsn.offset());
        volatile_write(replayed_lsn_offset, next_start_lsn.offset());
        start_lsn = next_start_lsn;
      }
    }
  } else {
    while (!config::IsShutdown()) {
      for (uint32_t i = 0; i < 2; ++i) {
        ReplayPipelineStage &stage = rep::pipeline_stages[i];
        // Load up ranges from storage
        ReplayPipelineStage tmp_stage;
        while (true) {
          uint32_t nbytes = os_pread(replay_bounds_fd, (char*)&tmp_stage,
                                     sizeof(ReplayPipelineStage), off);
          if (nbytes != sizeof(ReplayPipelineStage)) {
            std::unique_lock<std::mutex> lock(bg_replay_mutex);
            bg_replay_cond.wait(lock);
            continue;
          }
          off += nbytes;
          DLOG(INFO) << "Read " << std::hex << tmp_stage.start_lsn.offset()
                     << "-" << tmp_stage.end_lsn.offset() << std::endl;
          end_lsn = tmp_stage.end_lsn;
          break;
        }

        while (stage.end_lsn.offset() > volatile_read(replayed_lsn_offset)) {}
        DLOG(INFO) << "To replay " << std::hex << tmp_stage.start_lsn.offset()
                   << "-" << tmp_stage.end_lsn.offset() << std::endl;
        memcpy(stage.log_redo_partition_bounds,
               tmp_stage.log_redo_partition_bounds,
               config::log_redo_partitions * sizeof(uint64_t));
        stage.num_replaying_threads = config::replay_threads;
        for (uint32_t i = 0; i < config::log_redo_partitions; ++i) {
          stage.consumed[i] = false;
        }
        LOG_IF(FATAL, start_lsn.offset() != tmp_stage.start_lsn.offset());
        volatile_write(stage.start_lsn._val, start_lsn._val);

        // No read-from-logbuf, at least for now
        while (tmp_stage.end_lsn.offset() > logmgr->durable_flushed_lsn().offset()) {}
        volatile_write(stage.end_lsn._val, tmp_stage.end_lsn._val);

        start_lsn = tmp_stage.end_lsn;
      }
    }
  }
  while (volatile_read(replayed_lsn_offset) < end_lsn.offset()) {};
}

void BackupStartReplication() {
  volatile_write(replayed_lsn_offset, logmgr->cur_lsn().offset());
  ALWAYS_ASSERT(oidmgr);
  logmgr->recover();

  if (config::command_log) {
    std::thread t(BackupDaemonTcpCommandLog);
    t.detach();
  } else {
    volatile_write(persisted_nvram_offset, logmgr->durable_flushed_lsn().offset());
    volatile_write(persisted_nvram_size, 0);

    if (config::replay_policy == config::kReplayBackground) {
      dirent_iterator dir(config::log_dir.c_str());
      int dfd = dir.dup();
      replay_bounds_fd = openat(dfd, "replay_bounds", O_SYNC|O_CREAT|O_RDWR, S_IRUSR|S_IWUSR);
      LOG_IF(FATAL, replay_bounds_fd <= 2) << "Unable to open bounds file: " << replay_bounds_fd;
    } else {
      replay_bounds_fd = -1;
    }

    pipeline_stages = new ReplayPipelineStage[2];
    if (config::replay_policy != config::kReplayNone) {
      if (config::replay_policy == config::kReplayBackground) {
        std::thread t(BackupBackgroundReplay);
        t.detach();
      }
      if (config::persist_policy != config::kPersistAsync) {
        logmgr->start_logbuf_redoers();
      }
    }
    std::thread flusher(LogFlushDaemon);
    flusher.detach();

    if (config::log_ship_by_rdma) {
      // Start a daemon to receive and persist future log records
      std::thread t(BackupDaemonRdma);
      t.detach();
    } else {
      std::thread t(BackupDaemonTcp);
      t.detach();
    }
  }
}

void PrimaryShutdown() {
  if (config::persist_policy == config::kPersistAsync) {
    primary_async_ship_daemon.join();
  }
  if (config::log_ship_by_rdma) {
    PrimaryShutdownRdma();
  } else {
    PrimaryShutdownTcp();
  }
}

void primary_ship_log_buffer_all(const char *buf, uint32_t size, bool new_seg,
                                 uint64_t new_seg_start_offset) {
  backup_sockfds_mutex.lock();
  if (config::log_ship_by_rdma) {
    // This is async - returns immediately. Caller should poll/wait for ack.
    primary_ship_log_buffer_rdma(buf, size, new_seg, new_seg_start_offset);
  } else {
    // This is blocking because of send(), but doesn't wait for backup ack.
    primary_ship_log_buffer_tcp(buf, size);
  }
  backup_sockfds_mutex.unlock();
  shipped_log_size += size;
}

void TruncateFilesInLogDir() {
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for (char const *fname : dir) {
    if (fname[0] == 'o' || fname[0] == 'l') {
      int fd = os_openat(dfd, fname, O_RDWR);
      int unused = ftruncate(fd, 0);
      os_close(fd);
    }
  }
}

// Generate a metadata structure for sending to the new backup.
// No CC whatsoever, single-threaded execution only.
backup_start_metadata *prepare_start_metadata(int &chkpt_fd,
                                              LSN &chkpt_start_lsn) {
  chkpt_fd = -1;
  uint64_t nlogfiles = 0;
  dirent_iterator dir(config::log_dir.c_str());
  for (char const *fname : dir) {
    if (fname[0] == 'l') {
      ++nlogfiles;
    }
  }
  static backup_start_metadata *md = nullptr;
  if (!md || md->num_log_files < nlogfiles) {
    if (md) {
      free(md);
    }
    md = allocate_backup_start_metadata(nlogfiles);
  }
  new (md) backup_start_metadata;
  chkpt_start_lsn = INVALID_LSN;
  int dfd = dir.dup();
  // Find chkpt first
  for (char const *fname : dir) {
    char l = fname[0];
    if (l == 'c') {
      memcpy(md->chkpt_marker, fname, CHKPT_FILE_NAME_BUFSZ);
    } else if (l == 'o') {
      // chkpt file
      ALWAYS_ASSERT(config::enable_chkpt);
      struct stat st;
      chkpt_fd = os_openat(dfd, fname, O_RDONLY);
      int ret = fstat(chkpt_fd, &st);
      THROW_IF(ret != 0, log_file_error, "Error fstat");
      ASSERT(st.st_size);
      md->chkpt_size = st.st_size;
      char canary_unused;
      int n = sscanf(fname, CHKPT_DATA_FILE_NAME_FMT "%c",
                     &chkpt_start_lsn._val, &canary_unused);
      ALWAYS_ASSERT(chkpt_start_lsn != INVALID_LSN);
    }
  }
  LOG(INFO) << "[Primary] Will ship checkpoint taken at 0x" << std::hex
            << chkpt_start_lsn.offset() << std::dec;
  dfd = dir.dup();
  for (char const *fname : dir) {
    // Must send dur-xxxx, chk-xxxx, nxt-xxxx anyway
    char l = fname[0];
    if (l == 'd') {
      // durable lsn marker
      memcpy(md->durable_marker, fname, DURABLE_FILE_NAME_BUFSZ);
    } else if (l == 'n') {
      // nxt segment
      memcpy(md->nxt_marker, fname, NXT_SEG_FILE_NAME_BUFSZ);
    } else if (l == 'l') {
      uint64_t start = 0, end = 0;
      unsigned int seg;
      char canary_unused;
      int n = sscanf(fname, SEGMENT_FILE_NAME_FMT "%c", &seg, &start, &end,
                     &canary_unused);
      struct stat st;
      int log_fd = os_openat(dfd, fname, O_RDONLY);
      int ret = fstat(log_fd, &st);
      os_close(log_fd);
      ASSERT(st.st_size);
      uint64_t size = st.st_size - chkpt_start_lsn.offset();
      md->add_log_segment(seg, start, end, size);
      LOG(INFO) << "Will ship segment " << seg << ", " << size << " bytes";
    } else if (l == 'c' || l == 'o' || l == '.' || l == 'm') {
      // Nothing to do or already handled
    } else {
      LOG(FATAL) << "Unrecognized file name";
    }
  }
  return md;
}

void BackupProcessLogData(ReplayPipelineStage &stage, LSN start_lsn, LSN end_lsn) {
  // Now "notify" the flusher to write log records out, asynchronously.
  volatile_write(new_end_lsn_offset, end_lsn.offset());

  // For non-background replay, setting end_lsn "notifies" redo threads to
  // start. We can start replay regardless of log persistence state - we read
  // speculatively from the log buffer always and check if the data we read is
  // valid.
  // Note: for background replay, the [stage] passed in should be a local
  // variable that is not any of the global two stages which replay threads
  // read from. Instead, the background replay thread controls what to write to
  // the start_lsn/end_lsn fields when it gets the redo partition ranges from
  // storage.
  volatile_write(stage.start_lsn._val, start_lsn._val);
  volatile_write(stage.end_lsn._val, end_lsn._val);

  if (config::persist_policy != config::kPersistAsync &&
      config::replay_policy == config::kReplayBackground) {
    // Spill out to storage for futher use by the background replayer
    // FIXME(tzwang): we have only used tmpfs as 'storage' so the performance
    // impact should be very small). Add some in-memory caching if needed.
    os_write(replay_bounds_fd, (char*)&stage, sizeof(ReplayPipelineStage));
    bg_replay_cond.notify_all();
  }

  // Now handle replay policies:
  // 1. Sync - replay immediately; then when finished ack persitence
  //    immediately if NVRAM is present, otherwise ack persistence when
  //    data is flushed.
  // 2. Pipelined - notify replay daemon to start; then ack persitence
  //    immediately if NVRAM is present, otherwise ack persistence when
  //    data is flushed.
  //
  // Both synchronous and pipelined replay ensure log flush is out of
  // the critical path. The difference is whether log replay is on/out of
  // the critical path, i.e., before ack-ing persistence. The primary can't
  // continue unless it received persistence ack.
  //
  // The role of NVRAM here is solely for making persistence faster and is
  // orthogonal to the choice of replay policy.

  if (config::nvram_log_buffer) {
    uint64_t size = end_lsn.offset() - start_lsn.offset();
    if (config::persist_nvram_on_replay) {
      while (size > volatile_read(persisted_nvram_size)) {
      }
      volatile_write(persisted_nvram_size, 0);
    } else {
      // Impose delays to emulate NVRAM if needed
      if (config::nvram_delay_type == config::kDelayClflush) {
        segment_id* sid = logmgr->get_segment(start_lsn.segment());
        const char* buf =
            sm_log::logbuf->read_buf(sid->buf_offset(start_lsn.offset()), size);
        config::NvramClflush(buf, size);
      } else if (config::nvram_delay_type == config::kDelayClwbEmu) {
        config::NvramClwbEmu(size);
      }
    }
    volatile_write(persisted_nvram_offset, end_lsn.offset());
  } else {
    // Wait for the flusher to finish persisting log if we don't have NVRAM
    while (end_lsn.offset() > logmgr->durable_flushed_lsn().offset()) {
    }
  }

  if (config::replay_policy == config::kReplaySync) {
    while (volatile_read(replayed_lsn_offset) != end_lsn.offset()) {}
    DLOG(INFO) << "[Backup] Rolled forward log " << std::hex
               << start_lsn.offset() << "." << start_lsn.segment() << "-"
               << end_lsn.offset() << "." << end_lsn.segment() << std::dec;
    ASSERT(start_lsn.segment() == end_lsn.segment());
  }
}
}  // namespace rep
