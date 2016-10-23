#include <sys/stat.h>

#include "sm-file.h"
#include "sm-log-file.h"
#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {

// A daemon that runs on the primary for bringing up backups by shipping
// the latest chkpt (if any) + the log that follows.
void primary_daemon_tcp() {
  ALWAYS_ASSERT(logmgr);
  primary_tcp_ctx = new tcp::server_context(config::primary_port, config::num_backups);

wait_for_backup:
  int backup_sockfd = primary_tcp_ctx->expect_client();
  backup_sockfds.push_back(backup_sockfd);

  // Got a new backup, send out the latest chkpt (if any)
  // Scan the whole log dir, and send chkpt (if any) + the log that follows,
  // or all the logs if a chkpt doesn't exist.
  uint8_t nlogfiles = 0;
  dirent_iterator dir(config::log_dir.c_str());
  for(char const *fname : dir) {
    if(fname[0] == 'l') {
      ++nlogfiles;
    }
  }
  auto* md = allocate_backup_start_metadata(nlogfiles);
  int chkpt_fd = -1;
  LSN chkpt_start_lsn = INVALID_LSN, chkpt_end_lsn_unused = INVALID_LSN;
  int dfd = dir.dup();
  for (char const *fname : dir) {
    // Must send dur-xxxx, chk-xxxx, nxt-xxxx anyway
    char l = fname[0];
    if(l == 'c') {
      // chkpt marker
      memcpy(md->chkpt_marker, fname, CHKPT_FILE_NAME_BUFSZ);
    } else if(l == 'd') {
      // durable lsn marker
      memcpy(md->durable_marker, fname, DURABLE_FILE_NAME_BUFSZ);
    } else if(l == 'n') {
      // nxt segment
      memcpy(md->nxt_marker, fname, NXT_SEG_FILE_NAME_BUFSZ);
    } else if(l == 'o') {
      // chkpt file
      ALWAYS_ASSERT(config::enable_chkpt);
      struct stat st;
      chkpt_fd = os_openat(dfd, fname, O_RDONLY);
      int ret = fstat(chkpt_fd, &st);
      THROW_IF(ret != 0, log_file_error, "Error fstat");
      ASSERT(st.st_size);
      md->chkpt_size = st.st_size;
      char canary_unused;
      int n = sscanf(fname, CHKPT_FILE_NAME_FMT "%c",
                     &chkpt_start_lsn._val, &chkpt_end_lsn_unused._val, &canary_unused);
    } else if(l == 'l') {
      struct stat st;
      int log_fd = os_openat(dfd, fname, O_RDONLY);
      int ret = fstat(log_fd, &st);
      ASSERT(st.st_size);
      os_close(log_fd);
      md->add_log_file(fname, st.st_size);
    } else if(l == '.') {
      // Nothing to do
    } else {
      LOG(FATAL) << "Unrecognized file name";
    }
  }
  
  auto sent_bytes = send(backup_sockfd, md, md->size(), 0);
  ALWAYS_ASSERT(sent_bytes == md->size());

  if(chkpt_fd != -1) {
    while(md->chkpt_size > 0) {
      off_t offset = 0;
      sent_bytes = sendfile(backup_sockfd, chkpt_fd, &offset, md->chkpt_size);
      ALWAYS_ASSERT(sent_bytes);
      md->chkpt_size -= sent_bytes;
    }
    os_close(chkpt_fd);
  }

  // Now send the log after chkpt
  send_log_files_after_tcp(backup_sockfd, md, chkpt_start_lsn.offset());
  goto wait_for_backup;
}

void send_log_files_after_tcp(int backup_fd, backup_start_metadata* md, uint64_t chkpt_start) {
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for(uint64_t i = 0; i < md->num_log_files; ++i) {
    uint32_t segnum = 0;
    uint64_t start_offset = 0, end_offset = 0; 
    char canary_unused;
    int n = sscanf(md->get_log_file(i), SEGMENT_FILE_NAME_FMT "%c",
                   &segnum, &start_offset, &end_offset, &canary_unused);
    ALWAYS_ASSERT(n == 3);
    if(end_offset >= chkpt_start) {
      int log_fd = os_openat(dfd, md->get_log_file(i), O_RDONLY);
      struct stat st;
      int ret = fstat(log_fd, &st);
      ASSERT(st.st_size);
      while(st.st_size) {
        off_t offset = 0;
        auto sent_bytes = sendfile(backup_fd, log_fd, &offset, st.st_size);
        ALWAYS_ASSERT(sent_bytes);
        st.st_size -= sent_bytes;
      }
      os_close(log_fd);
    }
  }
}

void start_as_backup_tcp() {
  ALWAYS_ASSERT(config::is_backup_srv());

  LOG(INFO) << "[Backup] Primary address: " << config::primary_srv << ":" <<
    config::primary_port;
  tcp::client_context *cctx = new tcp::client_context(
    config::primary_srv, config::primary_port);

  // Expect the primary to send metadata, the header first
  backup_start_metadata md;
  tcp::receive(cctx->server_sockfd, (char*)&md, sizeof(md));
  LOG(INFO) << "[Backup] Recevie chkpt " << md.chkpt_marker << " " << md.chkpt_size << " bytes";

  // Write the marker files
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  int marker_fd = os_openat(dfd, md.chkpt_marker, O_CREAT|O_WRONLY);
  os_close(marker_fd);
  marker_fd = os_openat(dfd, md.durable_marker, O_CREAT|O_WRONLY);
  os_close(marker_fd);
  marker_fd = os_openat(dfd, md.nxt_marker, O_CREAT|O_WRONLY);
  os_close(marker_fd);

  // Get log file names
  char* log_file_names = nullptr;
  if(md.num_log_files > 0) {
    uint32_t s = md.size() - sizeof(md);
    log_file_names = (char*)malloc(s);
    tcp::receive(cctx->server_sockfd, log_file_names, s);
  }

  static const uint64_t kBufSize = 512 * 1024 * 1024;
  static char buf[kBufSize];
  if(md.chkpt_size > 0) {
    char canary_unused;
    LSN chkpt_start_lsn = INVALID_LSN, chkpt_end_lsn_unused = INVALID_LSN;
    int n = sscanf(md.chkpt_marker, CHKPT_FILE_NAME_FMT "%c",
                   &chkpt_start_lsn._val, &chkpt_end_lsn_unused._val, &canary_unused);
    static char chkpt_fname[CHKPT_DATA_FILE_NAME_BUFSZ];
    n = os_snprintf(chkpt_fname, sizeof(chkpt_fname),
                           CHKPT_DATA_FILE_NAME_FMT, chkpt_start_lsn._val);
    int chkpt_fd = os_openat(dfd, chkpt_fname, O_CREAT|O_WRONLY);
    LOG(INFO) << "[Backup] Chkpt file " << chkpt_fname;

    while(md.chkpt_size > 0) {
      uint64_t received_bytes =
        recv(cctx->server_sockfd, buf, std::min(kBufSize, md.chkpt_size), 0);
      md.chkpt_size -= received_bytes;
      os_write(chkpt_fd, buf, received_bytes);
      os_fsync(chkpt_fd);
    }
    os_close(chkpt_fd);
  }

  LOG(INFO) << "[Backup] Receved checkpoint file.";

  // Now receive the log files
  if(md.num_log_files > 0) {
    for(uint64_t i = 0; i < md.num_log_files; ++i) {
      uint64_t file_size = *(uint64_t*)(log_file_names +
                           (SEGMENT_FILE_NAME_BUFSZ + sizeof(uint64_t)) * i +
                           SEGMENT_FILE_NAME_BUFSZ);
      char* fname = log_file_names + (SEGMENT_FILE_NAME_BUFSZ + sizeof(uint64_t)) * i;
      int log_fd = os_openat(dfd, fname, O_CREAT|O_WRONLY);
      ALWAYS_ASSERT(log_fd > 0);
      while(file_size > 0) {
        uint64_t received_bytes = recv(cctx->server_sockfd, buf, std::min(file_size, kBufSize), 0);
        file_size -= received_bytes;
        os_write(log_fd, buf, received_bytes);
      }
      os_close(log_fd);
    }
  }
  LOG(INFO) << "[Backup] Receved log file.";
}

// Send the log buffer to backups
void primary_ship_log_buffer_tcp(
  int backup_sockfd, const char* buf, uint32_t size) {
  ALWAYS_ASSERT(size);
  size_t nbytes = send(backup_sockfd, (char *)&size, sizeof(uint32_t), 0);
  THROW_IF(nbytes != sizeof(uint32_t), log_file_error, "Incomplete log shipping (header)");
  //printf("[Primary] Will send %u bytes\n", size);
  nbytes = send(backup_sockfd, buf, size, 0);
  THROW_IF(nbytes != size, log_file_error, "Incomplete log shipping (data)");
  //printf("[Primary] Sent %u bytes\n", size);
  // expect an ack message
  // XXX(tzwang): do this in flush()?
  tcp::expect_ack(backup_sockfd);
}

void backup_daemon_tcp(tcp::client_context *cctx) {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());
  DEFER(delete cctx);

  // Listen to incoming log records from the primary
  uint32_t size = 0;
  // Wait for the main thread to create logmgr - it might run slower than me
  while (not volatile_read(logmgr)) {}
  auto& logbuf = logmgr->get_logbuf();

  // Now safe to start the redo daemon with a valid durable_flushed_lsn
  if (not config::log_ship_sync_redo) {
    std::thread rt(redo_daemon);
    rt.detach();
  }

  while (1) {
    // expect an integer indicating data size
    tcp::receive(cctx->server_sockfd, (char *)&size, sizeof(size));
    ALWAYS_ASSERT(size);

    // prepare segment if needed
    LSN start_lsn =  logmgr->durable_flushed_lsn();
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    segment_id *sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);
    ALWAYS_ASSERT(sid);
    LSN end_lsn = sid->make_lsn(end_lsn_offset);
    ASSERT(end_lsn_offset == end_lsn.offset());

    // expect the real log data
    //std::cout << "[Backup] Will receive " << size << " bytes\n";
    char *buf = logbuf.write_buf(sid->buf_offset(start_lsn), size);
    ALWAYS_ASSERT(buf);   // XXX: consider different log buffer sizes than the primary's later
    tcp::receive(cctx->server_sockfd, buf, size);
    //std::cout << "[Backup] Recieved " << size << " bytes ("
    //  << std::hex << start_lsn.offset() << "-" << end_lsn.offset() << std::dec << ")\n";

    // now got the batch of log records, persist them
    if (config::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf.advance_writer(sid->buf_offset(end_lsn));
    } else {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
      ASSERT(logmgr->durable_flushed_lsn() == end_lsn);
    }

    tcp::send_ack(cctx->server_sockfd);

    if (config::log_ship_sync_redo) {
      ALWAYS_ASSERT(end_lsn == logmgr->durable_flushed_lsn());
      printf("[Backup] Rolling forward %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }
    if (config::nvram_log_buffer)
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
  }
}

}  // namespace rep

