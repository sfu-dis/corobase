#include "sm-cmd-log.h"
#include "sm-log.h"
#include "sm-rep.h"
#include "../util.h"
#include "../benchmarks/bench.h"

namespace CommandLog {

CommandLogManager *cmd_log CACHE_ALIGNED;

// For log shipping
std::atomic<uint64_t> replayed_offset CACHE_ALIGNED;
spin_barrier *redoer_barrier = nullptr;
std::condition_variable redo_cond;
std::mutex redo_mutex;
uint64_t next_replay_offset[2] CACHE_ALIGNED;
char *bg_buffer = nullptr;

void CommandLogManager::TryFlush() {
  flush_cond_.notify_all();
}

uint64_t CommandLogManager::Insert(uint32_t partition_id, uint32_t xct_type, uint32_t size) {
  ASSERT(size >= sizeof(LogRecord));
  uint64_t off = allocated_.fetch_add(size);
  uint64_t end_off = off + size;

  if (end_off - durable_offset_ >= config::group_commit_bytes) {
    flush_cond_.notify_all();
  }

  LogRecord *r = (LogRecord*)&buffer_[off % buffer_size_];
  LOG_IF(FATAL, off % buffer_size_ + size > buffer_size_);
  new (r) LogRecord(partition_id, xct_type);
  volatile_write(tls_offsets_[thread::my_id()], end_off);
}

void CommandLogManager::BackupFlush(uint64_t new_off) {
  allocated_ = new_off;
  Flush(false);
  LOG_IF(FATAL, new_off < durable_offset_);
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

  uint64_t durable_off = durable_offset_;
  while (filled_off > durable_off) {
    uint32_t size = filled_off - durable_off;
    uint32_t start = durable_off % buffer_size_;
    char *buf = buffer_ + start;
    uint32_t to_write = std::min<uint32_t>(size, buffer_size_ - start);
    if (!config::is_backup_srv()) {
      to_write = std::min<uint32_t>(to_write, config::group_commit_bytes);
    }
    os_pwrite(fd_, buf, to_write, durable_off);

    if (config::num_active_backups && !config::is_backup_srv()) {
      DLOG(INFO) << "Shipping " << std::hex << durable_off << "-"
        << durable_off + to_write << std::dec;
      ShipLog(buf, to_write);
    }
    durable_off += to_write;
    durable_offset_ = durable_off;
    if (!config::is_backup_srv() && config::group_commit) {
      util::timer t;
      logmgr->dequeue_committed_xcts(durable_offset_, t.get_start());
    }
  }

  std::unique_lock<std::mutex> lock(redo_mutex);
  redo_cond.notify_all();
}

void CommandLogManager::BackgroundReplayDaemon() {
  bg_buffer = (char*)malloc(config::group_commit_bytes);
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  std::string fname = config::log_dir + std::string("/mlog");
  int fd = os_openat(dfd, + fname.c_str(), O_RDWR);

  // Keep notifying threads to replay a specified range
  uint64_t off = 0;
  uint32_t idx = 0;
  while (true) {
    if (durable_offset_ >= off + config::group_commit_bytes) {
      uint32_t size = pread(fd, bg_buffer, config::group_commit_bytes, off);
      off += size;
      if (size) {
        volatile_write(next_replay_offset[idx], off);
        while (replayed_offset < off) {}
        idx = (idx + 1) % 2;
      }
    }
  }
}

void CommandLogManager::BackgroundReplay(uint32_t redoer_id, bench_worker *worker) {
  uint64_t last_replayed = replayed_offset;
  ALWAYS_ASSERT(redoer_barrier);
  redoer_barrier->count_down();
  redoer_barrier->wait_for();
  uint32_t idx = 0;
  uint32_t buf_off = 0;
  while (!config::IsShutdown()) {
    uint64_t target_offset = volatile_read(next_replay_offset[idx]);
    while (target_offset <= last_replayed) {
      target_offset = volatile_read(next_replay_offset[idx]);
    }
    idx = (idx + 1) % 2;

    static const uint32_t kRecordSize = 256;
    int64_t to_replay = target_offset - last_replayed;
    uint32_t off = 0;
    uint32_t size = 0;
    while (to_replay > 0) {
      LogRecord *r = (LogRecord*)&bg_buffer[off];
      LOG_IF(FATAL, bg_buffer + off > bg_buffer + config::group_commit_bytes);
      off += kRecordSize;
      uint64_t id = r->partition_id % config::replay_threads;
      if (id == redoer_id) {
        // "Redo" it
        id = r->partition_id;  // Pass the actual partition (warehouse for TPCC)
        LOG_IF(FATAL, r->partition_id < 1);
        worker->do_cmdlog_redo_workload_function(r->transaction_type, (void*)id);
        size += kRecordSize;
      }
      to_replay -= kRecordSize;
      LOG_IF(FATAL, to_replay < 0);
    }
    DLOG(INFO) << "Redoer " << redoer_id << ": replayed "
      << size << " bytes, " << size / kRecordSize << " records";
    last_replayed = target_offset;
    uint64_t n = replayed_offset.fetch_add(size);
    if (n + size == target_offset) {
      logmgr->flush();
      volatile_write(rep::replayed_lsn_offset, logmgr->durable_flushed_lsn().offset());
    }
    while (replayed_offset < target_offset) {}
    DLOG(INFO) << "Redoer " << redoer_id << " " << std::hex << n << "+" << size;
  }
}

// For synchronous and pipelined redo only
void CommandLogManager::BackupRedo(uint32_t redoer_id, bench_worker *worker) {
  if (config::replay_policy == config::kReplayBackground) {
    BackgroundReplay(redoer_id, worker);
    return;
  }
  LOG(INFO) << "Started redo thread " << redoer_id;
  uint64_t last_replayed = replayed_offset;
  ALWAYS_ASSERT(redoer_barrier);
  redoer_barrier->count_down();
  redoer_barrier->wait_for();
  uint32_t idx = 0;
  while (!config::IsShutdown()) {
    uint64_t target_offset = volatile_read(next_replay_offset[idx]);
    while (target_offset <= last_replayed) {
      std::unique_lock<std::mutex> lock(redo_mutex);
      redo_cond.wait(lock);
      target_offset = volatile_read(next_replay_offset[idx]);
    }
    idx = (idx + 1) % 2;

    int64_t to_replay = target_offset - last_replayed;
    uint64_t off = volatile_read(last_replayed) % buffer_size_;
    static const uint32_t kRecordSize = 256;
    uint32_t size = 0;
    DLOG(INFO) << "Redoer " << redoer_id << std::hex << " to replay "
      << last_replayed << "-" << target_offset << std::dec;
    while (to_replay > 0) {
      LogRecord *r = (LogRecord*)&buffer_[off];
      LOG_IF(FATAL, buffer_ + off > buffer_ + buffer_size_);
      off += kRecordSize;
      uint64_t id = r->partition_id % config::replay_threads;
      if (id == redoer_id) {
        // "Redo" it
        id = r->partition_id;  // Pass the actual partition (warehouse for TPCC)
        LOG_IF(FATAL, r->partition_id < 1);
        worker->do_cmdlog_redo_workload_function(r->transaction_type, (void*)id);
        size += kRecordSize;
      }
      to_replay -= kRecordSize;
      LOG_IF(FATAL, to_replay < 0);
    }
    DLOG(INFO) << "Redoer " << redoer_id << ": replayed "
      << size << " bytes, " << size / kRecordSize << " records";
    last_replayed = target_offset;
    uint64_t n = replayed_offset.fetch_add(size);
    if (n + size == target_offset) {
      logmgr->flush();
      volatile_write(rep::replayed_lsn_offset, logmgr->durable_flushed_lsn().offset());
    }
    while (replayed_offset < target_offset) {}
    DLOG(INFO) << "Redoer " << redoer_id << " " << std::hex << n << "+" << size;
  }
}

void CommandLogManager::ShipLog(char *buf, uint32_t size) {
  ASSERT(config::persist_policy == config::kPersistSync);
  // TCP based synchronous shipping
  LOG_IF(FATAL, config::log_ship_by_rdma) << "RDMA not supported for logical log shipping";
  ASSERT(rep::backup_sockfds.size());
  DLOG(INFO) << "Shipping " << size << " bytes";
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
    uint64_t filled_off = allocated_;
    for (uint32_t i = 0; i < thread::next_thread_id; i++) {
      uint64_t off = volatile_read(tls_offsets_[i]);
      if (off && off < filled_off) {
        filled_off = off;
      }
    }
    if (filled_off - durable_offset_ < config::group_commit_bytes) {
      flush_cond_.wait(lock);
    }
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
  for (auto &t : bench_runner::cmdlog_redoers) {
    t->join();
  }
  os_close(fd_);
}
}  // namespace CommandLog
