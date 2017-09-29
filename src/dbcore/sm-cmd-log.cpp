#include "sm-cmd-log.h"
#include "sm-log.h"
#include "sm-rep.h"
#include "../util.h"

namespace CommandLog {

CommandLogManager *cmd_log CACHE_ALIGNED;

uint64_t CommandLogManager::Insert(uint32_t partition_id, uint32_t xct_type) {
  uint32_t alloc_size = sizeof(LogRecord);
  uint64_t off = allocated_.fetch_add(alloc_size);
  uint64_t end_off = off + alloc_size;

  if (end_off - volatile_read(durable_offset_) >= config::group_commit_bytes) {
    // Issue a flush
    std::unique_lock<std::mutex> lock(flush_mutex_);
    flush_cond_.notify_all();
  }
  while (end_off - volatile_read(durable_offset_) >= buffer_size_) {}

  LogRecord *r = (LogRecord*)&buffer_[off];
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
    char *buf = buffer_ + durable_off % buffer_size_;
    os_write(fd_, buf, size);
    volatile_write(durable_offset_, filled_off);
    if (config::num_active_backups && !config::is_backup_srv()) {
      ShipLog(buf, size);
    }
    {
      util::timer t;
      logmgr->dequeue_committed_xcts(filled_off, t.get_start());
    }
  }
}

/*
void CommandLogManager::BackupRedo() {

}
*/

void CommandLogManager::StartBackupRedoers() {
}

void CommandLogManager::ShipLog(char *buf, uint32_t size) {
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
  while (!config::IsShutdown()) {
    std::unique_lock<std::mutex> lock(flush_mutex_);
    flush_cond_.wait(lock);
    Flush();
  }
  Flush(false);
}

}  // namespace CommandLog
