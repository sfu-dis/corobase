#pragma once
#include <atomic>
#include <thread>

#include <fcntl.h>

#include "sm-common.h"
#include "sm-thread.h"
#include "../macros.h"

/* 
 * A very simple implementation of command logging. Each log record
 * is fixed-size (8 bytes) containing a partition ID and a transaction
 * type (support TPCC so far only).
 */
namespace CommandLog {
extern uint64_t replayed_offset;

struct LogRecord {
  static const uint32_t kInvalidPartition = ~uint32_t{0};
  static const uint32_t kInvalidTransaction = ~uint32_t{0};

  uint32_t partition_id;
  uint32_t transaction_type;

  LogRecord() : partition_id(kInvalidPartition), transaction_type(kInvalidTransaction) {}
  LogRecord(uint32_t part, uint32_t xct) : partition_id(part), transaction_type(xct) {}
};

class CommandLogManager {
private:
  uint32_t buffer_size_;
  std::atomic<bool> shutdown_;
  std::atomic<uint64_t> allocated_;
  uint64_t durable_offset_;
  char *buffer_;
  uint64_t *tls_offsets_;

  std::thread flusher_;
  std::condition_variable flush_cond_;
  std::mutex flush_mutex_;
  int fd_;
  std::vector<std::thread> backup_redoers;

  void ShipLog(char *buf, uint32_t size);
  void Flush(bool check_tls = true);
  void BackupRedo(uint32_t part_id);

public:
  CommandLogManager()
    : buffer_size_(config::command_log_buffer_mb * config::MB)
    , shutdown_(false)
    , allocated_(0)
    , durable_offset_(0) {
    // FIXME(tzwang): allow more flexibility
    // Ensure this so we can blindly flush the whole buffer without worrying
    // about boundaries.
    uint32_t buf_size = config::command_log_buffer_mb * config::MB;
    LOG_IF(FATAL, buf_size % sizeof(LogRecord) != 0);
    buffer_ = (char*)malloc(buf_size);
    memset(buffer_, 0, buf_size);

    tls_offsets_ = (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);
    memset(tls_offsets_, 0, sizeof(uint64_t) * config::MAX_THREADS);

    dirent_iterator dir(config::log_dir.c_str());
    int dfd = dir.dup();
    std::string fname = config::log_dir + std::string("/mlog");
    fd_ = os_openat(dfd, + fname.c_str(), O_CREAT | O_WRONLY | O_SYNC);
    flusher_ = std::thread(&CommandLogManager::FlushDaemon, this);
  }
  ~CommandLogManager();

  uint32_t Size() { return buffer_size_; }
  void StartBackupRedoers();
  void BackupFlush(uint64_t new_off);
  void FlushDaemon();
  uint64_t Insert(uint32_t partition_id, uint32_t xct_type, uint32_t size);
  inline uint64_t GetTlsOffset() {
    return volatile_read(tls_offsets_[thread::my_id()]);
  }
  inline char *GetBuffer() { return buffer_; }
  inline uint64_t DurableOffset() { return volatile_read(durable_offset_); }
};

extern CommandLogManager *cmd_log;

}  // namespace CommandLog

