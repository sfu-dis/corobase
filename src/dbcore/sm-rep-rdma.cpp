#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
rdma::context* primary_rdma_ctx = nullptr;
rdma::context* backup_rdma_ctx = nullptr;

uint32_t msg_buf_ridx = -1;
uint32_t log_buf_ridx = -1;
static const uint64_t kRdmaWaiting = 1;
static const uint64_t kRdmaReadyToReceive = 2;

// Get myself a dedicated memory buffer for reading in chkpt and log files
static const uint64_t kDaemonBufferSize = 512 * config::MB;
static char daemon_buffer[kDaemonBufferSize];
static uint32_t daemon_buffer_ridx = -1;

// For control flow...
const static int kMessageSize = 8;
static char msg_buf[kMessageSize] CACHE_ALIGNED;

void wait_for_backup() {
  ALWAYS_ASSERT(!config::is_backup_srv());
  while(true) {
    uint64_t m = volatile_read(*(uint64_t *)primary_rdma_ctx->get_memory_region(msg_buf_ridx));
    if(m == kRdmaReadyToReceive) {
      // reset it so I'm not confused next time
      *(uint64_t *)primary_rdma_ctx->get_memory_region(msg_buf_ridx) = kRdmaWaiting;
      break;
    }
  }
}

void set_ready_message() {
  ALWAYS_ASSERT(config::is_backup_srv());
  *(uint64_t *)backup_rdma_ctx->get_memory_region(msg_buf_ridx) = kRdmaReadyToReceive;
  backup_rdma_ctx->rdma_write(msg_buf_ridx, 0, msg_buf_ridx, 0, kMessageSize);
}

// A daemon that runs on the primary for bringing up backups by shipping
// the latest chkpt (if any) + the log that follows (if any). Uses RDMA
// based memcpy to transfer chkpt and log file data.
void primary_daemon_rdma() {

  // Supports only one backup for now. The rdma context will block and wait
  // for our only peer.
  primary_rdma_ctx = new rdma::context(config::primary_port, 1);

  // XXX(tzawng): Assuming the backup is also resitering in the same order
  msg_buf_ridx = primary_rdma_ctx->register_memory(msg_buf, kMessageSize);
  daemon_buffer_ridx = primary_rdma_ctx->register_memory(daemon_buffer, kDaemonBufferSize);
  auto* logbuf = logmgr->get_logbuf();
  log_buf_ridx = primary_rdma_ctx->register_memory(logbuf->_data, logbuf->window_size() * 2);
  primary_rdma_ctx->finish_init();
  LOG(INFO) << "[Primary] RDMA initialized (" << daemon_buffer_ridx << ")";

  int chkpt_fd = -1;
  LSN chkpt_start_lsn = INVALID_LSN;
  auto* md = prepare_start_metadata(chkpt_fd, chkpt_start_lsn);
  ALWAYS_ASSERT(chkpt_fd != -1);
  // Wait for the 'go' signal from the peer
  wait_for_backup();

  // Now can really send something, metadata first, header must fit in the buffer
  ALWAYS_ASSERT(md->size() < kDaemonBufferSize);
  memcpy(primary_rdma_ctx->get_memory_region(daemon_buffer_ridx), md, md->size());

  uint64_t buf_offset = md->size();
  uint64_t to_send = md->chkpt_size;
  while(to_send) {
    ALWAYS_ASSERT(buf_offset <= kDaemonBufferSize);
    if(buf_offset == kDaemonBufferSize) {
      // Full, wait for the backup to finish processing this
      wait_for_backup();
      buf_offset = 0;
    }
    uint64_t n = read(chkpt_fd, daemon_buffer + buf_offset,
                      std::min(to_send, kDaemonBufferSize - buf_offset));
    to_send -=n;

    // Write it out, then wait
    primary_rdma_ctx->rdma_write_imm(daemon_buffer_ridx, 0, daemon_buffer_ridx, 0, buf_offset + n, buf_offset + n);
    buf_offset = 0;
    wait_for_backup();
  }
  os_close(chkpt_fd);

  // Done with the chkpt file, now log files
  send_log_files_after_rdma(md, chkpt_start_lsn);
  ++config::num_active_backups;
  __sync_synchronize();
}

void send_log_files_after_rdma(backup_start_metadata* md, LSN chkpt_start) {
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
        uint64_t n = read(log_fd, daemon_buffer, std::min(kDaemonBufferSize, to_send));
        ALWAYS_ASSERT(n);
        primary_rdma_ctx->rdma_write_imm(daemon_buffer_ridx, 0, daemon_buffer_ridx, 0, 0, n);
        to_send -= n;
        wait_for_backup();
      }
      os_close(log_fd);
    }
  }
}

void start_as_backup_rdma() {
  ALWAYS_ASSERT(config::is_backup_srv());
  backup_rdma_ctx = new rdma::context(config::primary_srv, config::primary_port, 1);
  msg_buf_ridx = backup_rdma_ctx->register_memory(msg_buf, kMessageSize);
  daemon_buffer_ridx = backup_rdma_ctx->register_memory(daemon_buffer, kDaemonBufferSize);
  log_buf_ridx = backup_rdma_ctx->register_memory(sm_log::logbuf->_data, sm_log::logbuf->window_size() * 2);
  backup_rdma_ctx->finish_init();
  LOG(INFO) << "[Backup] RDMA initialized (" << daemon_buffer_ridx << ")";

  // Tell the primary to start
  set_ready_message();

  // Let data come
  uint64_t to_process = backup_rdma_ctx->receive_rdma_with_imm();
  ALWAYS_ASSERT(to_process);

  // Process the header first
  char* buf = backup_rdma_ctx->get_memory_region(daemon_buffer_ridx);
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
      set_ready_message();
      uint64_t to_write = backup_rdma_ctx->receive_rdma_with_imm();
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
      set_ready_message();
      uint64_t received_bytes = backup_rdma_ctx->receive_rdma_with_imm();
      ALWAYS_ASSERT(received_bytes);
      file_size -= received_bytes;
      os_write(log_fd, buf, received_bytes);
    }
    os_fsync(log_fd);
    os_close(log_fd);
  }

  // Done with receiving files and they should all be persisted
  set_ready_message();
  LOG(INFO) << "[Backup] Received log file.";
  std::thread t(backup_daemon_rdam);
  t.detach();
}

void backup_daemon_rdam() {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());
  DEFER(delete backup_rdma_ctx);

  // Listen to incoming log records from the primary
  uint32_t size = 0;
  // Wait for the main thread to create logmgr - it might run slower than me
  while (not volatile_read(logmgr)) {}
  // Now safe to start the redo daemon with a valid durable_flushed_lsn
  if (not config::log_ship_sync_redo) {
    std::thread rt(redo_daemon);
    rt.detach();
  }

  LOG(INFO) << "[Backup] Start to wait for logs from primary (" << log_buf_ridx << ")";
  auto* logbuf = logmgr->get_logbuf();
  while (1) {
    set_ready_message();

    // post an RR to get the data and its size embedded as an immediate
    size = backup_rdma_ctx->receive_rdma_with_imm();
    THROW_IF(not size, illegal_argument, "Invalid data size");

    // now we should already have data sitting in the buffer, but we need
    // to use the data size we got to calculate a new durable lsn first.
    LSN start_lsn =  logmgr->durable_flushed_lsn();
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    segment_id *sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);
    ALWAYS_ASSERT(sid);
    LSN end_lsn = sid->make_lsn(end_lsn_offset);
    ASSERT(end_lsn_offset == end_lsn.offset());

    LOG(INFO) << "[Backup] Recieved " << size << " bytes ("
      << std::hex << start_lsn.offset() << "-" << end_lsn.offset() << std::dec << ")";

    // now got the batch of log records, persist them
    if (config::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf->advance_writer(sid->buf_offset(end_lsn));
    } else {
      logmgr->flush_log_buffer(*logbuf, end_lsn_offset, true);
      ASSERT(logmgr->durable_flushed_lsn() == end_lsn);
    }

    set_ready_message();

    if (config::log_ship_sync_redo) {
      ALWAYS_ASSERT(end_lsn == logmgr->durable_flushed_lsn());
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }
    if (config::nvram_log_buffer) {
      logmgr->flush_log_buffer(*logbuf, end_lsn_offset, true);
    }
  }
}

// Support only one peer for now
void primary_ship_log_buffer_rdma(const char *buf, uint32_t size) {
  ALWAYS_ASSERT(size);
  // wait for the "go" signal
  wait_for_backup();
  uint64_t offset = buf - primary_rdma_ctx->get_memory_region(log_buf_ridx);
  primary_rdma_ctx->rdma_write_imm(log_buf_ridx, offset, log_buf_ridx, offset, size, size);
}

}  // namespace rep
