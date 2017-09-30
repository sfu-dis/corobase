#include <sys/stat.h>

#include "sm-cmd-log.h"
#include "sm-index.h"
#include "sm-log-file.h"
#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
tcp::client_context* cctx CACHE_ALIGNED;
uint64_t global_persisted_lsn_tcp CACHE_ALIGNED;

void bring_up_backup_tcp(int backup_sockfd, backup_start_metadata *md, LSN chkpt_start_lsn) {
  auto sent_bytes = send(backup_sockfd, md, md->size(), 0);
  ALWAYS_ASSERT(sent_bytes == md->size());

  int chkpt_fd = -1;
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for (char const *fname : dir) {
    if (fname[0] == 'o') {
      chkpt_fd = os_openat(dfd, fname, O_RDONLY);
      break;
    }
  }
  // TODO(tzwang): support log-only bootstrap
  LOG_IF(FATAL, chkpt_fd == -1) << "Unable to open chkpt";

  off_t offset = 0;
  uint64_t to_send = md->chkpt_size;
  while (to_send > 0) {
    sent_bytes = sendfile(backup_sockfd, chkpt_fd, &offset, to_send);
    ALWAYS_ASSERT(sent_bytes);
    to_send -= sent_bytes;
  }
  os_close(chkpt_fd);

  // Now send the log after chkpt
  send_log_files_after_tcp(backup_sockfd, md, chkpt_start_lsn);

  // Wait for the backup to notify me that it persisted the logs
  tcp::expect_ack(backup_sockfd);
  ++config::num_active_backups;
}

// A daemon that runs on the primary for bringing up backups by shipping
// the latest chkpt (if any) + the log that follows (if any).
void primary_daemon_tcp() {
  ALWAYS_ASSERT(logmgr);
  tcp::server_context primary_tcp_ctx(config::primary_port,
                                      config::num_backups);

  // Got a new backup, send out the latest chkpt (if any)
  // Scan the whole log dir, and send chkpt (if any) + the log that follows,
  // or all the logs if a chkpt doesn't exist.
  int chkpt_fd = -1;
  LSN chkpt_start_lsn = INVALID_LSN;
  auto *md = prepare_start_metadata(chkpt_fd, chkpt_start_lsn);
  os_close(chkpt_fd);

  std::vector<std::thread*> workers;
  for (uint32_t i = 0; i < config::num_backups; ++i) {
    std::cout << "Expecting node " << i << std::endl;
    int backup_sockfd = primary_tcp_ctx.expect_client();
    backup_sockfds.push_back(backup_sockfd);
  }

  // Fire workers to do the real job - must do this after got all backups
  // as we need to broadcast to everyone the complete list of all backup nodes
  for (auto &fd : backup_sockfds) {
    workers.push_back(new std::thread(bring_up_backup_tcp, fd, md, chkpt_start_lsn));
  }

  for (auto &w : workers) {
    w->join();
    delete w;
  }

  // Save tmpfs (memory) space, use with caution for replication: will lose the
  // ability for 'catch' up using logs from storage. Do this here before
  // benchmark begins so we don't get hit by ftruncate-ing a large file.
  if (config::fake_log_write) {
    TruncateFilesInLogDir(); 
  }

  // All done, start async shipping daemon if needed
  if (!config::command_log && config::persist_policy == config::kPersistAsync) {
    primary_async_ship_daemon = std::move(std::thread(PrimaryAsyncShippingDaemon));
  }
}

void send_log_files_after_tcp(int backup_fd, backup_start_metadata* md,
                              LSN chkpt_start) {
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for (uint32_t i = 0; i < md->num_log_files; ++i) {
    uint32_t segnum = 0;
    uint64_t start_offset = 0, end_offset = 0;
    char canary_unused;
    backup_start_metadata::log_segment* ls = md->get_log_segment(i);
    int n = sscanf(ls->file_name.buf, SEGMENT_FILE_NAME_FMT "%c", &segnum,
                   &start_offset, &end_offset, &canary_unused);
    ALWAYS_ASSERT(n == 3);
    uint32_t to_send = ls->size;
    if (to_send) {
      // Ship only the part after chkpt start
      auto* seg = logmgr->get_offset_segment(start_offset);
      off_t file_off = start_offset - seg->start_offset;
      int log_fd = os_openat(dfd, ls->file_name.buf, O_RDONLY);
      lseek(log_fd, file_off, SEEK_SET);
      while (to_send) {
        auto sent_bytes = sendfile(backup_fd, log_fd, &file_off, to_send);
        ALWAYS_ASSERT(sent_bytes);
      }
      os_close(log_fd);
    }
  }
}

void start_as_backup_tcp() {
  memset(log_redo_partition_bounds, 0,
         sizeof(uint64_t) * kMaxLogBufferPartitions);
  ALWAYS_ASSERT(config::is_backup_srv());

  LOG(INFO) << "[Backup] Primary: " << config::primary_srv << ":"
            << config::primary_port;
  cctx = new tcp::client_context(config::primary_srv, config::primary_port);

  // Expect the primary to send metadata, the header first
  const int kNumPreAllocFiles = 10;
  backup_start_metadata* md = allocate_backup_start_metadata(kNumPreAllocFiles);
  tcp::receive(cctx->server_sockfd, (char*)md, sizeof(*md));
  LOG(INFO) << "[Backup] Receive chkpt " << md->chkpt_marker << " "
            << md->chkpt_size << " bytes";
  if (md->num_log_files > kNumPreAllocFiles) {
    auto* d = md;
    md = allocate_backup_start_metadata(d->num_log_files);
    memcpy(md, d, sizeof(*d));
    free(d);
  }
  md->persist_marker_files();

  // Get log file names
  if (md->num_log_files > 0) {
    uint64_t s = md->size() - sizeof(*md);
    tcp::receive(cctx->server_sockfd, (char*)&md->segments[0], s);
  }

  static const uint64_t kBufSize = 512 * 1024 * 1024;
  static char buf[kBufSize];
  if (md->chkpt_size > 0) {
    dirent_iterator dir(config::log_dir.c_str());
    int dfd = dir.dup();
    char canary_unused;
    uint64_t chkpt_start = 0, chkpt_end_unused;
    int n = sscanf(md->chkpt_marker, CHKPT_FILE_NAME_FMT "%c", &chkpt_start,
                   &chkpt_end_unused, &canary_unused);
    static char chkpt_fname[CHKPT_DATA_FILE_NAME_BUFSZ];
    n = os_snprintf(chkpt_fname, sizeof(chkpt_fname), CHKPT_DATA_FILE_NAME_FMT,
                    chkpt_start);
    int chkpt_fd = os_openat(dfd, chkpt_fname, O_CREAT | O_WRONLY);
    LOG(INFO) << "[Backup] Checkpoint " << chkpt_fname;

    while (md->chkpt_size > 0) {
      uint64_t received_bytes =
          recv(cctx->server_sockfd, buf, std::min(kBufSize, md->chkpt_size), 0);
      md->chkpt_size -= received_bytes;
      os_write(chkpt_fd, buf, received_bytes);
    }
    os_fsync(chkpt_fd);
    os_close(chkpt_fd);
  }

  LOG(INFO) << "[Backup] Received checkpoint file.";

  // Now receive the log files
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for (uint64_t i = 0; i < md->num_log_files; ++i) {
    backup_start_metadata::log_segment* ls = md->get_log_segment(i);
    uint64_t file_size = ls->size;
    int log_fd = os_openat(dfd, ls->file_name.buf, O_CREAT | O_WRONLY);
    ALWAYS_ASSERT(log_fd > 0);
    while (file_size > 0) {
      uint64_t received_bytes =
          recv(cctx->server_sockfd, buf, std::min(file_size, kBufSize), 0);
      file_size -= received_bytes;
      os_write(log_fd, buf, received_bytes);
    }
    os_fsync(log_fd);
    os_close(log_fd);
  }

  // Extract system config and set them before new_log
  config::benchmark_scale_factor = md->system_config.scale_factor;
  config::log_segment_mb = md->system_config.log_segment_mb;
  config::persist_policy = md->system_config.persist_policy;
  config::command_log_buffer_mb = md->system_config.command_log_buffer_mb;
  config::command_log = config::command_log_buffer_mb > 0;

  logmgr = sm_log::new_log(config::recover_functor, nullptr);
  sm_oid_mgr::create();

  if (config::command_log) {
    CommandLog::cmd_log = new CommandLog::CommandLogManager();
  }
  LOG(INFO) << "[Backup] Received log file.";
}

// Send the log buffer to backups. Note: here we don't wait for backups' ack.
// The caller (ie logmgr) handles it when necessary.
void primary_ship_log_buffer_tcp(const char* buf, uint32_t size) {
  ASSERT(backup_sockfds.size());
  for (int &fd : backup_sockfds) {
    // Send real log data, size first
    ALWAYS_ASSERT(size);
    uint32_t nbytes = send(fd, (char*)&size, sizeof(uint32_t), 0);
    LOG_IF(FATAL, nbytes != sizeof(uint32_t)) << "Incomplete log shipping (header)";
    nbytes = send(fd, buf, size, 0);
    LOG_IF(FATAL, nbytes != size) << "Incomplete log shipping: " << nbytes << "/"
                                  << size;

    if (config::persist_policy != config::kPersistAsync) {
      // Send redo partition boundary information - after sending real data
      // because we send data size=0 to indicate primary shutdown.
      const uint32_t bounds_size = sizeof(uint64_t) * config::log_redo_partitions;
      nbytes = send(fd, log_redo_partition_bounds, bounds_size, 0);
      LOG_IF(FATAL, nbytes != bounds_size) << "Error sending bounds array";
    }
  }
  if (config::persist_policy == config::kPersistAsync) {
    for (int &fd : backup_sockfds) {
      tcp::expect_ack(fd);
    }
  }
}

// Receives the bounds array sent from the primary.
// The only caller is backup daemon.
void BackupReceiveBoundsArrayTcp(ReplayPipelineStage& pipeline_stage) {
    uint32_t bsize = config::log_redo_partitions * sizeof(uint64_t);
    tcp::receive(cctx->server_sockfd, (char*)log_redo_partition_bounds, bsize);

#ifndef NDEBUG
  for (uint32_t i = 0; i < config::log_redo_partitions; ++i) {
    uint64_t s = 0;
    if (i > 0) {
      s = (log_redo_partition_bounds[i] >> 16) -
          (log_redo_partition_bounds[i - 1] >> 16);
    }
    LOG(INFO) << "Logbuf partition: " << i << " " << std::hex
              << log_redo_partition_bounds[i] << std::dec << " " << s;
  }
#endif
  // Make a stable local copy for replay threads to use
  // Note: for non-background replay, [pipeline_stage] should be
  // one of the two global stages used by redo threads;
  // for background replay, however, it should be a temporary
  // one which will be later copied to the two global stages.
  memcpy(pipeline_stage.log_redo_partition_bounds,
         log_redo_partition_bounds,
         config::log_redo_partitions * sizeof(uint64_t));
  for (uint32_t i = 0; i < config::log_redo_partitions; ++i) {
    pipeline_stage.consumed[i] = false;
  }
  pipeline_stage.num_replaying_threads = config::replay_threads;
}

void BackupDaemonTcp() {
  ALWAYS_ASSERT(logmgr);
  rcu_register();
  DEFER(rcu_deregister());

  global_persisted_lsn_ptr = &global_persisted_lsn_tcp;
  global_persisted_lsn_tcp = logmgr->cur_lsn().offset();

  LSN start_lsn = logmgr->durable_flushed_lsn();

  // Listen to incoming log records from the primary
  uint32_t size = 0;

  // Done with receiving files and they should all be persisted, now ack the
  // primary
  tcp::send_ack(cctx->server_sockfd);
  received_log_size = 0;
  uint32_t recv_idx = 0;
  ReplayPipelineStage *stage = nullptr;
  if (config::replay_policy == config::kReplayBackground) {
    stage = new ReplayPipelineStage;
  }
  while (true) {
    rcu_enter();
    DEFER(rcu_exit());
    if (config::replay_policy != config::kReplayBackground) {
      stage = &pipeline_stages[recv_idx];
      recv_idx = (recv_idx + 1) % 2;
    }
    WaitForLogBufferSpace(start_lsn);

    // expect an integer indicating data size
    tcp::receive(cctx->server_sockfd, (char*)&size, sizeof(size));

    if (!config::IsForwardProcessing()) {
      // Received the first batch, for sure the backup can start benchmarks.
      // FIXME(tzwang): this is not optimal - ideally we should start after
      // the primary starts, instead of when the primary *shipped* the first
      // batch.
      volatile_write(config::state, config::kStateForwardProcessing);
    }

    // Zero size indicates 'shutdown' signal from the primary
    if (size == 0) {
      tcp::send_ack(cctx->server_sockfd);
      volatile_write(config::state, config::kStateShutdown);
      LOG(INFO) << "Got shutdown signal from primary, exit.";
      rep::backup_shutdown_trigger
          .notify_all();  // Actually only needed if no query workers
      break;
    }

    received_log_size += size;

    // prepare segment if needed
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    segment_id* sid =
        logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);
    ALWAYS_ASSERT(sid);
    LSN end_lsn = sid->make_lsn(end_lsn_offset);
    ASSERT(end_lsn_offset == end_lsn.offset());

    // expect the real log data
    DLOG(INFO) << "[Backup] Will receive " << size << " bytes";
    char* buf = sm_log::logbuf->write_buf(sid->buf_offset(start_lsn), size);
    ALWAYS_ASSERT(buf);  // XXX: consider different log buffer sizes than the
                         // primary's later
    tcp::receive(cctx->server_sockfd, buf, size);
    DLOG(INFO) << "[Backup] Recieved " << size << " bytes (" << std::hex
               << start_lsn.offset() << "-" << end_lsn.offset() << std::dec
               << ")";

    uint64_t new_byte = sid->buf_offset(end_lsn_offset);
    sm_log::logbuf->advance_writer(new_byte);  // Extends reader_end too
    ASSERT(sm_log::logbuf->available_to_read() >= size);

    if (config::persist_policy != config::kPersistAsync) {
      // Receive bounds array
      BackupReceiveBoundsArrayTcp(*stage);
    }

    BackupProcessLogData(*stage, start_lsn, end_lsn);

    // Ack the primary after persisting data
    tcp::send_ack(cctx->server_sockfd);

    if (config::persist_policy != config::kPersistAsync) {
      // Get global persisted LSN
      uint64_t glsn = 0;
      tcp::receive(cctx->server_sockfd, (char*)&glsn, sizeof(uint64_t));
      volatile_write(*global_persisted_lsn_ptr, glsn);
    }

    // Next iteration
    start_lsn = end_lsn;
  }
  if (config::replay_policy == config::kReplayBackground) {
    delete stage;
  }
}

void PrimaryShutdownTcp() {
  static const uint32_t kZero = 0;
  backup_sockfds_mutex.lock();
  ASSERT(backup_sockfds.size());
  for (int& fd : backup_sockfds) {
    size_t nbytes = send(fd, (char*)&kZero, sizeof(uint32_t), 0);
    ALWAYS_ASSERT(nbytes == sizeof(uint32_t));
    tcp::expect_ack(fd);
  }
  backup_sockfds_mutex.unlock();
}

void BackupDaemonTcpCommandLog() {
  ALWAYS_ASSERT(CommandLog::cmd_log);
  ALWAYS_ASSERT(cctx);
  rcu_register();
  DEFER(rcu_deregister());
  tcp::send_ack(cctx->server_sockfd);
  uint32_t buf_size = CommandLog::cmd_log->Size();

  uint32_t size = 0;
  uint64_t doff = CommandLog::cmd_log->DurableOffset();
  while (true) {
    // expect an integer indicating data size
    tcp::receive(cctx->server_sockfd, (char*)&size, sizeof(size));

    if (!config::IsForwardProcessing()) {
      // Received the first batch, for sure the backup can start benchmarks.
      // FIXME(tzwang): this is not optimal - ideally we should start after
      // the primary starts, instead of when the primary *shipped* the first
      // batch.
      volatile_write(config::state, config::kStateForwardProcessing);
    }

    // Zero size indicates 'shutdown' signal from the primary
    if (size == 0) {
      tcp::send_ack(cctx->server_sockfd);
      volatile_write(config::state, config::kStateShutdown);
      LOG(INFO) << "Got shutdown signal from primary, exit.";
      rep::backup_shutdown_trigger
          .notify_all();  // Actually only needed if no query workers
      break;
    }

    uint64_t durable_offset = CommandLog::cmd_log->DurableOffset();
    ASSERT(durable_offset == doff);
    uint64_t off = durable_offset % buf_size;
    LOG_IF(FATAL, durable_offset < volatile_read(CommandLog::replayed_offset))
      << "Wrong durable/replayed offset";

    if (config::replay_policy != config::kReplayNone) {
      while (buf_size - (durable_offset - volatile_read(CommandLog::replayed_offset)) < size) {}
    }
    char *buf = CommandLog::cmd_log->GetBuffer() + off;
    tcp::receive(cctx->server_sockfd, buf, size);

    LOG_IF(FATAL, config::replay_policy != config::kReplaySync &&
                  config::replay_policy != config::kReplayNone)
      << "Unspported replay policy";

    // Flush it first so redoers know to start
    CommandLog::cmd_log->BackupFlush(size + durable_offset);
    doff += size;

    if (config::replay_policy == config::kReplaySync) {
      while (volatile_read(CommandLog::replayed_offset) != durable_offset + size) {}
      // Essentially this is a 'two-copy' database, so persist it (as if I'm primary)
      logmgr->flush();
      // Advance read view
      volatile_write(replayed_lsn_offset, logmgr->durable_flushed_lsn().offset());
    }

    // Ack the primary after persisting data
    tcp::send_ack(cctx->server_sockfd);
  }
}
}  // namespace rep
