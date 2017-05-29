#include <x86intrin.h>
#include "sm-rep.h"
#include "sm-rep-rdma.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
struct RdmaNode* self_rdma_node CACHE_ALIGNED;
std::vector<struct RdmaNode*> nodes;
std::mutex nodes_lock CACHE_ALIGNED;

std::condition_variable backup_shutdown_trigger;

const static uint32_t kRdmaImmNewSeg = 1U << 31;
const static uint32_t kRdmaImmShutdown = 1U << 30;

void primary_rdma_poll_send_cq(uint64_t nops) {
  ALWAYS_ASSERT(!config::is_backup_srv());
  for(auto& n : nodes) {
    n->PollSendCompletionAsPrimary(nops);
  }
}

void primary_rdma_wait_for_message(uint64_t msg, bool reset) {
  ALWAYS_ASSERT(!config::is_backup_srv());
  for(auto& n : nodes) {
    n->WaitForMessageAsPrimary(kRdmaPersisted | kRdmaReadyToReceive, false);
  }
}

// A daemon that runs on the primary for bringing up backups by shipping
// the latest chkpt (if any) + the log that follows (if any). Uses RDMA
// based memcpy to transfer chkpt and log file data.
void primary_daemon_rdma() {
  // Create an RdmaNode object for each backup node
  for(uint32_t i = 0; i < config::num_backups; ++i) {
    std::cout << "Expecting node " << i << std::endl;
    RdmaNode* rn = new RdmaNode(true);
    nodes.push_back(rn);

    int chkpt_fd = -1;
    LSN chkpt_start_lsn = INVALID_LSN;
    auto* md = prepare_start_metadata(chkpt_fd, chkpt_start_lsn);
    ALWAYS_ASSERT(chkpt_fd != -1);
    // Wait for the 'go' signal from the peer
    rn->WaitForMessageAsPrimary(kRdmaReadyToReceive);

    // Now can really send something, metadata first, header must fit in the buffer
    ALWAYS_ASSERT(md->size() < RdmaNode::kDaemonBufferSize);
    char* daemon_buffer = rn->GetDaemonBuffer();
    memcpy(daemon_buffer, md, md->size());

    uint64_t buf_offset = md->size();
    uint64_t to_send = md->chkpt_size;
    while(to_send) {
      ALWAYS_ASSERT(buf_offset <= RdmaNode::kDaemonBufferSize);
      if(buf_offset == RdmaNode::kDaemonBufferSize) {
        // Full, wait for the backup to finish processing this
        rn->WaitForMessageAsPrimary(kRdmaReadyToReceive);
        buf_offset = 0;
      }
      uint64_t n = read(chkpt_fd, daemon_buffer + buf_offset,
                        std::min(to_send, RdmaNode::kDaemonBufferSize - buf_offset));
      to_send -=n;

      // Write it out, then wait
      rn->RdmaWriteImmDaemonBuffer(0, 0, buf_offset + n, buf_offset + n);
      buf_offset = 0;
      rn->WaitForMessageAsPrimary(kRdmaReadyToReceive);
    }
    os_close(chkpt_fd);

    // Done with the chkpt file, now log files
    send_log_files_after_rdma(rn, md, chkpt_start_lsn);
    ++config::num_active_backups;
    __sync_synchronize();
  }
}

void send_log_files_after_rdma(RdmaNode* self, backup_start_metadata* md,
                               LSN chkpt_start) {
  char* daemon_buffer = self->GetDaemonBuffer();
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for(uint32_t i = 0; i < md->num_log_files; ++i) {
    uint32_t segnum = 0;
    uint64_t start_offset = 0, end_offset = 0; 
    char canary_unused;
    backup_start_metadata::log_segment* ls = md->get_log_segment(i);
    int n = sscanf(ls->file_name.buf, SEGMENT_FILE_NAME_FMT "%c",
                   &segnum, &start_offset, &end_offset, &canary_unused);
    ALWAYS_ASSERT(n == 3);
    uint64_t to_send = ls->size;
    if(to_send) {
      // Ship only the part after chkpt start
      auto* seg = logmgr->get_offset_segment(start_offset);
      int log_fd = os_openat(dfd, ls->file_name.buf, O_RDONLY);
      lseek(log_fd, start_offset - seg->start_offset, SEEK_SET);
      while(to_send) {
        uint64_t n = read(log_fd, daemon_buffer,
                          std::min((uint64_t)RdmaNode::kDaemonBufferSize, to_send));
        ALWAYS_ASSERT(n);
        self->RdmaWriteImmDaemonBuffer(0, 0, n, n);
        to_send -= n;
        self->WaitForMessageAsPrimary(kRdmaReadyToReceive);
      }
      os_close(log_fd);
    }
  }
}

// Receives the bounds array sent from the primary.
// Returns false if we should stop. The only caller is backup daemon.
bool BackupReceiveBoundsArray() {
  // Post an RR to get the log buffer partition bounds
  // Must post RR before setting ReadyToReceive msg (i.e., RR should be posted before
  // the peer sends data).
  // This RR also serves the purpose of getting the primary's next step. Normally
  // the primary would send over the bounds array without immediate; when the
  // primary wants to shutdown it will note this in the immediate.
  uint32_t intent = 0;
  auto bounds_array_size = self_rdma_node->ReceiveImm(&intent);
  if(!config::IsForwardProcessing()) {
    // Received the first batch, for sure the backup can start benchmarks.
    // FIXME(tzwang): this is not optimal - ideally we should start after
    // the primary starts, instead of when the primary *shipped* the first batch.
    volatile_write(config::state, config::kStateForwardProcessing);
  }
  if(intent == kRdmaImmShutdown) {
    // Primary signaled shutdown, exit daemon
    volatile_write(config::state, config::kStateShutdown);
    LOG(INFO) << "Got shutdown signal from primary, exit.";
    return false;
  }
  ALWAYS_ASSERT(bounds_array_size == sizeof(uint64_t) * kMaxLogBufferPartitions);

#ifndef NDEBUG
  for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
    uint64_t s = 0;
    if(i > 0) {
      s = (logbuf_partition_bounds[i] >> 16) - (logbuf_partition_bounds[i-1] >> 16);
    }
    LOG(INFO) << "Logbuf partition: " << i << " " << std::hex
              << logbuf_partition_bounds[i] << std::dec << " " << s;
  }
#endif
  return true;
}

// Receive log data from the primary. The only caller is the backup daemon.
LSN BackupReceiveLogData(LSN& start_lsn) {
  // post an RR to get the data and the chunk's begin LSN embedded as an the wr_id
  uint32_t imm = 0;
  uint32_t size = self_rdma_node->ReceiveImm(&imm);
  THROW_IF(not size, illegal_argument, "Invalid data size");
  segment_id *sid = logmgr->get_segment(start_lsn.segment());
  ASSERT(sid->segnum == start_lsn.segment());
  if(imm & kRdmaImmNewSeg) {
    // Extract the segment's start offset (off of the segment's raw, theoreticall start offset)
    start_lsn = LSN::make(
      config::log_segment_mb * config::MB * start_lsn.segment() + (imm & ~kRdmaImmNewSeg),
      start_lsn.segment() + 1);
  }

  uint64_t end_lsn_offset = start_lsn.offset() + size;
  sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);

  // Now we should already have data sitting in the buffer, but we need
  // to use the data size we got to calculate a new durable lsn first.
  ALWAYS_ASSERT(sid->end_offset >= end_lsn_offset);
  ALWAYS_ASSERT(sid && sid->segnum == start_lsn.segment());
  if(imm & kRdmaImmNewSeg) {
    // Fix the start and end offsets for the new segment
    sid->start_offset = start_lsn.offset();
    sid->end_offset = sid->start_offset + config::log_segment_mb * config::MB;

    // Now we're ready to create the file with correct file name
    logmgr->create_segment_file(sid);
    ALWAYS_ASSERT(sid->start_offset == start_lsn.offset());
    ALWAYS_ASSERT(sid->end_offset >= end_lsn_offset);
  }
  DLOG(INFO) << "Assigned " << std::hex << start_lsn.offset() << "-" << end_lsn_offset
    << " to " << std::dec << sid->segnum << " " << std::hex
    << sid->start_offset << " " << sid->byte_offset << std::dec;
  ALWAYS_ASSERT(sid);

  DLOG(INFO) << "[Backup] Received " << size << " bytes ("
    << std::hex << start_lsn.offset() << "-" << end_lsn_offset << std::dec << ")";

  uint64_t new_byte = sid->buf_offset(end_lsn_offset);
  sm_log::logbuf->advance_writer(new_byte);  // Extends reader_end too
  ASSERT(sm_log::logbuf->available_to_read() >= size);
  return LSN::make(end_lsn_offset, start_lsn.segment());
}

void BackupDaemonRdma() {
  ALWAYS_ASSERT(logmgr);
  self_rdma_node->SetMessageAsBackup(kRdmaReadyToReceive);
  rcu_register();
  DEFER(rcu_deregister());
  DEFER(delete self_rdma_node);

  LOG(INFO) << "[Backup] Start to wait for logs from primary";
  LSN start_lsn = logmgr->durable_flushed_lsn();
  std::thread flusher(LogFlushDaemon);
  flusher.detach();

  if(config::replay_policy == config::kReplayPipelined) {
    std::thread redoer(LogRedoDaemon);
    redoer.detach();
  }

  uint32_t recv_idx = 0;
  LSN recv_lsn[2] = {INVALID_LSN, INVALID_LSN};
  while(!config::IsShutdown()) {
    rcu_enter();
    DEFER(rcu_exit());
    WaitForLogBufferSpace(recv_lsn[recv_idx]);

    self_rdma_node->SetMessageAsBackup(kRdmaReadyToReceive);
    if(!BackupReceiveBoundsArray()) {
      rep::backup_shutdown_trigger.notify_all();  // Actually only needed if no query workers
      return;
    }

    LSN end_lsn = BackupReceiveLogData(start_lsn);
    volatile_write(recv_lsn[recv_idx]._val, end_lsn._val);
    recv_idx = (recv_idx + 1) % 2;

    // Now "notify" the flusher to write log records out, asynchronously.
    volatile_write(new_end_lsn_offset, end_lsn.offset());

    // Impose delays to emulate NVRAM if needed
    if(config::nvram_log_buffer &&
       (config::nvram_delay_type != config::kDelayNone)) {
      uint64_t size = end_lsn.offset() - start_lsn.offset();
      if(config::nvram_delay_type == config::kDelayClflush) {
        segment_id *sid = logmgr->get_segment(start_lsn.segment());
        const char* buf = sm_log::logbuf->read_buf(sid->buf_offset(start_lsn.offset()), size);
        uint32_t clines = size / CACHELINE_SIZE;
        for(uint32_t i = 0; i < clines; ++i) {
          _mm_clflush(&buf[i * CACHELINE_SIZE]);
        }
      } else {
        // Emulate clwb, spin
        uint64_t total_cycles = size * config::cycles_per_byte;
        unsigned int unused = 0;
        uint64_t cycle_end = __rdtscp(&unused) + total_cycles;
        while(__rdtscp(&unused) < cycle_end) {}
      }
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
    if(config::replay_policy == config::kReplaySync) {
      logmgr->redo_logbuf(start_lsn, end_lsn);
      DLOG(INFO) << "[Backup] Rolled forward log "
                 << std::hex << start_lsn.offset() << "." << start_lsn.segment()
                 << "-" << end_lsn.offset() << "." << end_lsn.segment() << std::dec;
      ASSERT(start_lsn.segment() == end_lsn.segment());
      // Make the new records visible, but pending persistence
      volatile_write(replayed_lsn_offset, end_lsn.offset());
    } else if(config::replay_policy == config::kReplayPipelined) {
      volatile_write(redo_start_lsn._val, start_lsn._val);
      volatile_write(redo_end_lsn._val, end_lsn._val);
    }

    // Now wait for the flusher to finish persisting log if we don't have NVRAM,
    if(!config::nvram_log_buffer) {
      while(end_lsn.offset() > volatile_read(persisted_lsn_offset)) {}
    }

    // Tell the primary the data is persisted, it can continue
    ASSERT(logmgr->durable_flushed_lsn().offset() <= end_lsn.offset());
    self_rdma_node->SetMessageAsBackup(kRdmaPersisted);

    // Next iteration
    start_lsn = end_lsn;
  }
}

void start_as_backup_rdma() {
  memset(logbuf_partition_bounds, 0, sizeof(uint64_t) * kMaxLogBufferPartitions);
  ALWAYS_ASSERT(config::is_backup_srv());
  self_rdma_node = new RdmaNode(false);

  // Tell the primary to start
  self_rdma_node->SetMessageAsBackup(kRdmaReadyToReceive);

  // Let data come
  uint64_t to_process = self_rdma_node->ReceiveImm();
  ALWAYS_ASSERT(to_process);

  // Process the header first
  char* buf = self_rdma_node->GetDaemonBuffer();
  // Get a copy of md to preserve the log file names
  backup_start_metadata* md = (backup_start_metadata*)buf;
  backup_start_metadata* d = (backup_start_metadata*)malloc(md->size());
  memcpy(d, md, md->size());
  md = d;
  md->persist_marker_files();

  to_process -= md->size();
  if(md->chkpt_size > 0) {
    dirent_iterator dir(config::log_dir.c_str());
    int dfd = dir.dup();
    char canary_unused;
    uint64_t chkpt_start = 0, chkpt_end_unused;
    int n = sscanf(md->chkpt_marker, CHKPT_FILE_NAME_FMT "%c",
                   &chkpt_start, &chkpt_end_unused, &canary_unused);
    static char chkpt_fname[CHKPT_DATA_FILE_NAME_BUFSZ];
    n = os_snprintf(chkpt_fname, sizeof(chkpt_fname),
                           CHKPT_DATA_FILE_NAME_FMT, chkpt_start);
    int chkpt_fd = os_openat(dfd, chkpt_fname, O_CREAT|O_WRONLY);
    LOG(INFO) << "[Backup] Checkpoint " << chkpt_fname << ", " << md->chkpt_size << " bytes";

    // Flush out the bytes in the buffer stored after the metadata first
    if(to_process > 0) {
      uint64_t to_write = std::min(md->chkpt_size, to_process);
      os_write(chkpt_fd, buf + md->size(), to_write);
      os_fsync(chkpt_fd);
      to_process -= to_write;
      md->chkpt_size -= to_write;
    }
    ALWAYS_ASSERT(to_process == 0);  // XXX(tzwang): currently always have chkpt

    // More coming in the buffer
    while(md->chkpt_size > 0) {
      // Let the primary know to begin the next round
      self_rdma_node->SetMessageAsBackup(kRdmaReadyToReceive);
      uint64_t to_write = self_rdma_node->ReceiveImm();
      os_write(chkpt_fd, buf, std::min(md->chkpt_size, to_write));
      md->chkpt_size -= to_write;
    }
    os_fsync(chkpt_fd);
    os_close(chkpt_fd);
    LOG(INFO) << "[Backup] Received " << chkpt_fname;
  }

  // Now get log files
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for(uint64_t i = 0; i < md->num_log_files; ++i) {
    backup_start_metadata::log_segment* ls = md->get_log_segment(i);
    uint64_t file_size = ls->size;
    int log_fd = os_openat(dfd, ls->file_name.buf, O_CREAT|O_WRONLY);
    ALWAYS_ASSERT(log_fd > 0);
    while(file_size > 0) {
      self_rdma_node->SetMessageAsBackup(kRdmaReadyToReceive);
      uint64_t received_bytes = self_rdma_node->ReceiveImm();
      ALWAYS_ASSERT(received_bytes);
      file_size -= received_bytes;
      os_write(log_fd, buf, received_bytes);
    }
    os_fsync(log_fd);
    os_close(log_fd);
  }

  // Extract system config and set them before new_log
  config::benchmark_scale_factor = md->system_config.scale_factor;
  config::log_segment_mb = md->system_config.log_segment_mb;
  config::logbuf_partitions = md->system_config.logbuf_partitions;

  logmgr = sm_log::new_log(config::recover_functor, nullptr);
  sm_oid_mgr::create();
  LOG(INFO) << "[Backup] Received log file.";
}

void primary_ship_log_buffer_rdma(const char *buf, uint32_t size,
                                  bool new_seg, uint64_t new_seg_start_offset) {
#ifndef NDEBUG
  for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
    LOG(INFO) << "RDMA write logbuf bounds: " << i << " " << std::hex
              << logbuf_partition_bounds[i] << std::dec;
  }
#endif
  for(auto& node : nodes) {
    node->WaitForMessageAsPrimary(kRdmaReadyToReceive);

    // Send buffer partition boundary information first
    node->RdmaWriteImmLogBufferPartitionBounds(0, 0,
                                     sizeof(uint64_t) * kMaxLogBufferPartitions,
                                     sizeof(uint64_t) * kMaxLogBufferPartitions,
                                     false /* async */);

    ALWAYS_ASSERT(size);
    uint64_t offset = buf - sm_log::get_logbuf()->_data;
    ASSERT(offset + size <= sm_log::get_logbuf()->window_size() * 2);
#ifndef NDEBUG
    if(new_seg) {
      LOG(INFO) << "new segment start offset=" << std::hex << new_seg_start_offset << std::dec;
    }
#endif
    ALWAYS_ASSERT(new_seg_start_offset <= ~kRdmaImmNewSeg);
    uint32_t imm = new_seg ? kRdmaImmNewSeg | (uint32_t)new_seg_start_offset : 0;
    node->RdmaWriteImmLogBuffer(offset, offset, size, imm, false);
  }
}

void PrimaryShutdownRdma() {
  for(auto& node : nodes) {
    node->WaitForMessageAsPrimary(kRdmaReadyToReceive);

    // Send the shutdown signal using the bounds buffer
    node->RdmaWriteImmLogBufferPartitionBounds(0, 0, 8, kRdmaImmShutdown, true);
  }
}
}  // namespace rep
