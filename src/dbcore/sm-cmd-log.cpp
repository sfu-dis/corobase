#include "sm-cmd-log.h"
#include "sm-log.h"
#include "sm-rep.h"
#include "../util.h"

namespace CommandLog {

CommandLogManager *cmd_log CACHE_ALIGNED;

// For log shipping
uint64_t replayed_offset CACHE_ALIGNED;

uint64_t CommandLogManager::Insert(uint32_t partition_id, uint32_t xct_type, uint32_t size) {
  ASSERT(size >= sizeof(LogRecord));
  uint64_t off = allocated_.fetch_add(size);
  uint64_t end_off = off + size;

  if (end_off - volatile_read(durable_offset_) >= config::group_commit_bytes) {
    // Issue a flush
    std::unique_lock<std::mutex> lock(flush_mutex_);
    if (end_off - volatile_read(durable_offset_) >= config::group_commit_bytes) {
      flush_cond_.notify_all();
    }
  }
  while (end_off - volatile_read(durable_offset_) >= buffer_size_) {}

  LogRecord *r = (LogRecord*)&buffer_[off % buffer_size_];
  LOG_IF(FATAL, off % buffer_size_ + size > buffer_size_);
  new (r) LogRecord(partition_id, xct_type);
  volatile_write(tls_offsets_[thread::my_id()], end_off);
}

void CommandLogManager::BackupFlush(uint64_t new_off) {
  allocated_ = new_off;
  Flush(false);
}

void CommandLogManager::Flush(bool check_tls) {
  uint64_t filled_off = allocated_;
  if (check_tls) {
    for (uint32_t i = 0; i < thread::next_thread_id; i++) {
      uint64_t off = volatile_read(tls_offsets_[i]);
      if (off && off < filled_off) {
        filled_off = off;
      }
    }
  }

  uint64_t durable_off = volatile_read(durable_offset_);
  if (filled_off > durable_off) {
    uint32_t size = filled_off - durable_off;
    uint32_t start = durable_off % buffer_size_;
    char *buf = buffer_ + start;
    uint32_t to_write = std::min<uint32_t>(size, buffer_size_ - start);
    os_write(fd_, buf, to_write);
    volatile_write(durable_offset_, durable_off + to_write);

    if (config::num_active_backups && !config::is_backup_srv()) {
      ShipLog(buf, to_write);
      {
        util::timer t;
        logmgr->dequeue_committed_xcts(durable_off + to_write, t.get_start());
      }
    }

    if (to_write < size) {
      to_write = size - to_write;
      buf = buffer_;
      LOG_IF(FATAL, to_write != filled_off % buffer_size_);
      os_write(fd_, buf, to_write);;
      volatile_write(durable_offset_, durable_off + size);
      if (config::num_active_backups && !config::is_backup_srv()) {
        ShipLog(buf, to_write);
        {
          util::timer t;
          logmgr->dequeue_committed_xcts(durable_off + to_write, t.get_start());
        }
      }
    }
  }
}

void CommandLogManager::BackupRedo(uint32_t redoer_id) {
  LOG(INFO) << "Started redo thread " << redoer_id;
  uint64_t doff = 0;
  while (!config::IsShutdown()) {
    uint64_t start_off = volatile_read(durable_offset_);
    if (start_off == doff) {
      // Already replayed
      continue;
    }
    doff = start_off;
    uint64_t roff = volatile_read(replayed_offset);
    DLOG_IF(FATAL, doff < roff) << "Durable offset smaller than replayed offset: "
      << std::hex << doff << "/" << roff << std::dec;
    if (doff == roff) {
      continue;
    }
    uint32_t size = 0;
    DLOG(INFO) << "Redoer " << redoer_id << ": to redo " << std::hex
      << roff << "-" << doff << std::dec;
    while (roff < doff) {
      uint32_t off = roff % buffer_size_;
      LogRecord *r = (LogRecord*)&buffer_[off];
      roff += sizeof(LogRecord);
      if (r->partition_id % config::replay_threads == redoer_id) {
        // REDO IT
        size += sizeof(LogRecord);
      }
    }
    DLOG(INFO) << "Redoer " << redoer_id << ": replayed " << size << " bytes, "
      << size / sizeof(LogRecord) << " records";
    __atomic_add_fetch(&replayed_offset, size, __ATOMIC_SEQ_CST);
  }
}

void CommandLogManager::StartBackupRedoers() {
  LOG_IF(FATAL, config::replay_policy != config::kReplaySync);
  replayed_offset = 0;
  for (uint32_t i = 0; i < config::replay_threads; ++i) {
    backup_redoers.emplace_back(&CommandLogManager::BackupRedo, this, i);
  }
}

void CommandLogManager::ShipLog(char *buf, uint32_t size) {
  ASSERT(config::persist_policy == config::kPersistSync);
  // TCP based synchronous shipping
  LOG_IF(FATAL, config::log_ship_by_rdma) << "RDMA not supported for logical log shipping";
  ASSERT(rep::backup_sockfds.size());
  LOG(INFO) << "Shipping " << size << " bytes";
  for (int &fd : rep::backup_sockfds) {
    uint32_t nbytes = send(fd, (char*)&size, sizeof(uint32_t), 0);
    LOG_IF(FATAL, nbytes != sizeof(uint32_t)) << "Incomplete log shipping (header)";
    nbytes = send(fd, buf, size, 0);
    LOG_IF(FATAL, nbytes != size) << "Incomplete log shipping: " << nbytes << "/"
                                  << size;
  }
  for (int &fd : rep::backup_sockfds) {
    tcp::expect_ack(fd);
  }
}

void CommandLogManager::FlushDaemon() {
  while (!shutdown_) {
    std::unique_lock<std::mutex> lock(flush_mutex_);
    flush_cond_.wait(lock);
    Flush();
  }
  Flush(false);
}

CommandLogManager::~CommandLogManager() {
  shutdown_ = true;
  {
    std::unique_lock<std::mutex> lock(flush_mutex_);
    flush_cond_.notify_all();
  }
  flusher_.join();
  for (auto &t : backup_redoers) {
    t.join();
  }
  os_close(fd_);
}
}  // namespace CommandLog
