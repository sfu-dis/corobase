#include "sm-cmd-log.h"
#include "sm-log.h"
#include "sm-rep.h"
#include "../util.h"
#include "../benchmarks/bench.h"

namespace CommandLog {

CommandLogManager *cmd_log CACHE_ALIGNED;

// For log shipping
uint64_t replayed_offset CACHE_ALIGNED;
spin_barrier *redoer_barrier = nullptr;
std::atomic<uint32_t> flusher_status(0);

void CommandLogManager::TryFlush() {
  flush_cond_.notify_all();
}

uint64_t CommandLogManager::Insert(uint32_t partition_id, uint32_t xct_type, uint32_t size) {
  ASSERT(size >= sizeof(LogRecord));
  uint64_t off = allocated_.fetch_add(size);
  uint64_t end_off = off + size;

  if (end_off - volatile_read(durable_offset_) >= config::group_commit_bytes) {
    if (flusher_status == 0 && flusher_status.fetch_add(1) == 0) {
      if (end_off - volatile_read(durable_offset_) >= config::group_commit_bytes) {
        // Issue a flush
        flush_cond_.notify_all();
      }
    }
  }
  if (end_off - volatile_read(durable_offset_) >= buffer_size_) {}

  LogRecord *r = (LogRecord*)&buffer_[off % buffer_size_];
  LOG_IF(FATAL, off % buffer_size_ + size > buffer_size_);
  new (r) LogRecord(partition_id, xct_type);
  volatile_write(tls_offsets_[thread::my_id()], end_off);
}

void CommandLogManager::BackupFlush(uint64_t new_off) {
  allocated_ = new_off;
  Flush(false);
  LOG_IF(FATAL, new_off < volatile_read(durable_offset_));
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
  while (filled_off > durable_off) {
    uint32_t size = filled_off - durable_off;
    uint32_t start = durable_off % buffer_size_;
    char *buf = buffer_ + start;
    uint32_t to_write = std::min<uint32_t>(size, buffer_size_ - start);
    /*
    if (!config::is_backup_srv()) {
      to_write = std::min<uint32_t>(to_write, config::group_commit_bytes);
    }
    */
    os_write(fd_, buf, to_write);

    if (config::num_active_backups && !config::is_backup_srv()) {
      DLOG(INFO) << "Shipping " << std::hex << durable_off << "-"
        << durable_off + to_write << std::dec;
      ShipLog(buf, to_write);
    }
    durable_off += to_write;
    volatile_write(durable_offset_, durable_off);
    if (!config::is_backup_srv() && config::group_commit) {
      util::timer t;
      logmgr->dequeue_committed_xcts(durable_offset_, t.get_start());
    }
  }
  flusher_status = 0;
}

void CommandLogManager::BackupRedo(uint32_t redoer_id, bench_worker *worker) {
  LOG(INFO) << "Started redo thread " << redoer_id;
  uint64_t last_replayed = volatile_read(replayed_offset);
  ALWAYS_ASSERT(redoer_barrier);
  redoer_barrier->count_down();
  redoer_barrier->wait_for();
  while (!config::IsShutdown()) {
    uint64_t new_durable = volatile_read(durable_offset_);
    if (new_durable == last_replayed) {
      continue;
    }

    int64_t to_replay = new_durable - last_replayed;
    uint64_t off = volatile_read(last_replayed) % buffer_size_;
    static const uint32_t kRecordSize = 256;
    uint32_t size = 0;
    DLOG(INFO) << "Redoer " << redoer_id << std::hex << " to replay "
      << last_replayed << "-" << new_durable << std::dec;
    while (to_replay) {
      LogRecord *r = (LogRecord*)&buffer_[off];
      off += kRecordSize;
      uint64_t id = r->partition_id % config::replay_threads;
      if (id == redoer_id) {
        // "Redo" it
        id = r->partition_id;  // Pass the actual partition (warehouse for TPCC)
        ALWAYS_ASSERT(r->partition_id);
        worker->do_cmdlog_redo_workload_function(r->transaction_type, (void*)id);
        size += kRecordSize;
      }
      to_replay -= kRecordSize;
      ALWAYS_ASSERT(to_replay >= 0);
    }
    DLOG(INFO) << "Redoer " << redoer_id << ": replayed "
      << size << " bytes, " << size / kRecordSize << " records";
    last_replayed = new_durable;
    __atomic_add_fetch(&replayed_offset, size, __ATOMIC_SEQ_CST);
    while (volatile_read(replayed_offset) < new_durable) {}
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
  for (auto &t : bench_runner::cmdlog_redoers) {
    t->join();
  }
  os_close(fd_);
}
}  // namespace CommandLog
