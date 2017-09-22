#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
std::condition_variable backup_shutdown_trigger;
uint64_t log_redo_partition_bounds[kMaxLogBufferPartitions] CACHE_ALIGNED;

// for primary server only
std::vector<int> backup_sockfds CACHE_ALIGNED;
std::mutex backup_sockfds_mutex CACHE_ALIGNED;

// For backups only
ReplayPipelineStage *pipeline_stages CACHE_ALIGNED;
uint64_t replayed_lsn_offset CACHE_ALIGNED;
uint64_t persisted_lsn_offset CACHE_ALIGNED;
uint64_t persisted_nvram_size CACHE_ALIGNED;
uint64_t persisted_nvram_offset CACHE_ALIGNED;
uint64_t new_end_lsn_offset CACHE_ALIGNED;
uint64_t *global_persisted_lsn_ptr CACHE_ALIGNED;
uint64_t received_log_size CACHE_ALIGNED;

void start_as_primary() {
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
  while (true) {
    uint64_t lsn = volatile_read(new_end_lsn_offset);
    // Use another variable to record the durable flushed LSN offset
    // here, as the backup daemon might change a new sgment ID's
    // start_offset when it needs to create new one after receiving
    // data from the primary. That might cause the durable_flushed_lsn
    // call to fail when the adjusted start_offset makes the sid think
    // it doesn't contain the LSN.
    if (lsn > volatile_read(persisted_lsn_offset)) {
      logmgr->BackupFlushLog(lsn);
      volatile_write(persisted_lsn_offset, lsn);
    }
  }
}

void BackupStartReplication() {
  volatile_write(replayed_lsn_offset, logmgr->cur_lsn().offset());
  volatile_write(persisted_lsn_offset, logmgr->durable_flushed_lsn().offset());
  volatile_write(persisted_nvram_offset, logmgr->durable_flushed_lsn().offset());
  volatile_write(persisted_nvram_size, 0);
  ALWAYS_ASSERT(oidmgr);
  logmgr->recover();

  pipeline_stages = new ReplayPipelineStage[2];
  if (config::replay_policy != config::kReplayNone) {
    logmgr->start_logbuf_redoers();
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

void PrimaryShutdown() {
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
    } else if (l == 'c' || l == 'o' || l == '.') {
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

  // Start replay regardless of log persistence state - we read speculatively
  // from the log buffer always and check if the data we read is valid. The
  // write of stage.end_lsn "notifies" redo threads to start.
  volatile_write(stage.start_lsn._val, start_lsn._val);
  volatile_write(stage.end_lsn._val, end_lsn._val);

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
    while (end_lsn.offset() > volatile_read(persisted_lsn_offset)) {
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
