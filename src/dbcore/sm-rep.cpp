#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/sendfile.h>

#include <iostream>
#include <thread>
#include <vector>

#include "rdma.h"
#include "sm-common.h"
#include "sm-config.h"
#include "sm-log.h"
#include "sm-log-offset.h"
#include "sm-rep.h"
#include "tcp.h"

namespace rep {
#define MAX_NBACKUPS 10

// for primary server only
tcp::server_context *primary_tcp_ctx = nullptr;
std::vector<int> backup_sockfds;

rdma::context *primary_rdma_ctx = nullptr;

const static int kMessageSize = 8;
char msgbuf[kMessageSize];

uint32_t msgbuf_index = -1;
uint32_t logbuf_index = -1;

void primary_server_daemon() {
  logmgr->flush();  // persist everything before shipping
  do {
    int backup_sockfd = primary_tcp_ctx->expect_client();
    backup_sockfds.push_back(backup_sockfd);
    // Got a new backup, send out my logs
    // TODO(tzwang): handle backup joining after primary started forward processing
    dirent_iterator dir(sysconf::log_dir.c_str());
    int dfd = dir.dup();

    // tell backup how many files to expect
    uint8_t nfiles = 0;
    for (char const *fname : dir) {
      if (fname[0] != '.')
        nfiles++;
    }

    auto sent_bytes = send(backup_sockfd, &nfiles, sizeof(nfiles), 0);
    ALWAYS_ASSERT(sent_bytes == sizeof(nfiles));

    // we're just starting the db, so send everything we have
    for (char const *fname : dir) {
      if (fname[0] == '.')
        continue;
      int log_fd = os_openat(dfd, fname, O_RDONLY);
      primary_ship_log_file(backup_sockfd, fname, log_fd);
      close(log_fd);
    }

    // wait for ack from the backup
    tcp::expect_ack(backup_sockfd);
    //printf("[Primary] Backup received log\n");
  } while (++sysconf::num_active_backups < sysconf::num_backups);

  delete primary_tcp_ctx;
  if (sysconf::log_ship_by_rdma) {
    auto& logbuf = logmgr->get_logbuf();
    primary_rdma_ctx = new rdma::context(sysconf::primary_port, 1);
    logbuf_index = primary_rdma_ctx->register_memory(logbuf._data, logbuf.window_size() * 2);
    msgbuf_index = primary_rdma_ctx->register_memory(msgbuf, kMessageSize);
    primary_rdma_ctx->finish_init();
  }
}

void primary_ship_log_file(int backup_fd, const char* log_fname, int log_fd) {
  // First send over filename and file size
  static const uint32_t metadata_size = 256;
  char metadata[metadata_size];
  // max filename length: 255 bytes + one uint8 for filename length + one uint32 for file size
  ALWAYS_ASSERT(metadata_size >= sizeof(strlen(log_fname) + sizeof(uint8_t) + sizeof(uint32_t)));

  memset(metadata, 0, metadata_size);

  // Put filename length first
  ASSERT(strlen(log_fname) <= 255);
  uint8_t name_len = strlen(log_fname);
  uint16_t used_bytes = 0;
  memcpy(metadata, &name_len, sizeof(uint8_t));
  used_bytes += sizeof(uint8_t);

  // Now the filename, null-terminated
  memcpy(metadata + used_bytes, log_fname, name_len);
  used_bytes += name_len;
  metadata[used_bytes] = '\0';
  ++used_bytes;

  // Finally the log file's size
  struct stat st;
  int ret = fstat(log_fd, &st);
  THROW_IF(ret != 0, log_file_error, "Error fstat");
  memcpy(metadata + used_bytes, &st.st_size, sizeof(uint32_t));
  used_bytes += sizeof(uint32_t);

  auto sent_bytes = send(backup_fd, metadata, metadata_size, 0);
  ALWAYS_ASSERT(sent_bytes == metadata_size);
  //printf("[Primary] %s: sent %ld bytes of metadata\n", log_fname, sent_bytes);

  // Now we can send the file, in one go with sendfile()
  if (st.st_size) {
    off_t offset = 0;
    sent_bytes = sendfile(backup_fd, log_fd, &offset, st.st_size);
    ALWAYS_ASSERT(sent_bytes == st.st_size);
    ALWAYS_ASSERT(offset == st.st_size);
    //printf("[Primary] %s: %ld bytes, sent %ld bytes\n", log_fname, st.st_size, sent_bytes);
  }
}

// Send the log buffer to backups
void primary_ship_log_buffer(
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

void primary_ship_log_buffer_all(const char *buf, uint32_t size) {
  if (sysconf::log_ship_by_rdma) {
    primary_ship_log_buffer_rdma(buf, size);
  } else {
    ASSERT(backup_sockfds.size());
    for (int &fd : backup_sockfds) {
      primary_ship_log_buffer(fd, buf, size);
    }
  }
}

// Support only one peer for now
void primary_ship_log_buffer_rdma(const char *buf, uint32_t size) {
  ALWAYS_ASSERT(size);
  // wait for the "go" signal
  while (volatile_read(*(uint64_t *)primary_rdma_ctx->get_memory_region(msgbuf_index)) != RDMA_READY_TO_RECEIVE) {}
  // reset it so I'm not confused next time
  *(uint64_t *)primary_rdma_ctx->get_memory_region(msgbuf_index) = RDMA_WAITING;
  uint64_t offset = buf - primary_rdma_ctx->get_memory_region(logbuf_index);
  primary_rdma_ctx->rdma_write(logbuf_index, offset, offset, size, size);
}

void start_as_primary() {
  ALWAYS_ASSERT(not sysconf::is_backup_srv());
  primary_tcp_ctx = new tcp::server_context(sysconf::primary_port, sysconf::num_backups);
  // Spawn a new thread to listen to incoming connections
  std::thread t(primary_server_daemon);
  t.detach();
}

void redo_daemon() {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());

  auto start_lsn = logmgr->durable_flushed_lsn();
  while (true) {
    auto end_lsn = logmgr->durable_flushed_lsn();
    if (end_lsn > start_lsn) {
      printf("[Backup] Rolling forward %lx-%lx\n", start_lsn.offset(), end_lsn.offset());
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn.offset());
      start_lsn = end_lsn;
    }
  }
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
  if (not sysconf::log_ship_sync_redo) {
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
    if (sysconf::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf.advance_writer(sid->buf_offset(end_lsn));
    } else {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
      ASSERT(logmgr->durable_flushed_lsn() == end_lsn);
    }

    tcp::send_ack(cctx->server_sockfd);

    if (sysconf::log_ship_sync_redo) {
      ALWAYS_ASSERT(end_lsn == logmgr->durable_flushed_lsn());
      printf("[Backup] Rolling forward %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }
    if (sysconf::nvram_log_buffer)
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
  }
}

void backup_daemon_rdma() {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());

  // Wait for the main thread to create logmgr - it might run slower than me
  while (not volatile_read(logmgr)) {}
  auto& logbuf = logmgr->get_logbuf();
  rdma::context *cctx = new rdma::context(sysconf::primary_srv, sysconf::primary_port, 1);
  logbuf_index = cctx->register_memory(logbuf._data, logbuf.window_size() * 2);
  msgbuf_index = cctx->register_memory(msgbuf, kMessageSize);
  cctx->finish_init();

  DEFER(delete cctx);

  // Now safe to start the redo daemon with a valid durable_flushed_lsn
  if (not sysconf::log_ship_sync_redo) {
    std::thread rt(redo_daemon);
    rt.detach();
  }

  // Listen to incoming log records from the primary
  uint32_t size = 0;
  while (1) {
    // tell the peer i'm ready
    *(uint64_t *)cctx->get_memory_region(msgbuf_index) = RDMA_READY_TO_RECEIVE;
    cctx->rdma_write(msgbuf_index, 0, 0, kMessageSize);

    // post an RR to get the data and its size embedded as an immediate
    size = cctx->receive_rdma_with_imm();
    THROW_IF(not size, illegal_argument, "Invalid data size");

    // now we should already have data sitting in the buffer, but we need
    // to use the data size we got to calculate a new durable lsn first.
    LSN start_lsn =  logmgr->durable_flushed_lsn();
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    segment_id *sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);
    ALWAYS_ASSERT(sid);
    LSN end_lsn = sid->make_lsn(end_lsn_offset);
    ASSERT(end_lsn_offset == end_lsn.offset());

    std::cout << "[Backup] Recieved " << size << " bytes ("
      << std::hex << start_lsn.offset() << "-" << end_lsn.offset() << std::dec << ")\n";

    // now we have the new durable lsn (end_lsn), persist the data we got
    if (sysconf::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf.advance_writer(sid->buf_offset(end_lsn));
    } else {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
      ASSERT(logmgr->durable_flushed_lsn() == end_lsn);
    }

    // roll forward
    if (sysconf::log_ship_sync_redo) {
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }
    if (sysconf::nvram_log_buffer) {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
    }
  }
}

void start_as_backup() {
  ALWAYS_ASSERT(sysconf::is_backup_srv());
  std::cout << "[Backup] Connecting to " << sysconf::primary_srv << ":"
    << sysconf::primary_port << "\n";
  tcp::client_context *cctx = new tcp::client_context(
    sysconf::primary_srv, sysconf::primary_port);

  // Wait for the primary to ship the first part of the log so I can start "recovery"
  char buffer[4096];
  char fname[251];
  dirent_iterator dir(sysconf::log_dir.c_str());
  int dfd = dir.dup();

  uint8_t nfiles = 0;
  tcp::receive(cctx->server_sockfd, (char *)&nfiles, sizeof(nfiles)); // expect one byte of for nfiles
  ALWAYS_ASSERT(nfiles);

  while (nfiles--) {
    // First packet, get filename etc
    tcp::receive(cctx->server_sockfd, buffer, 256);

    uint8_t namelen = *(uint8_t *)buffer;
    memcpy(fname, buffer + sizeof(uint8_t), namelen);
    fname[namelen] = '\0';
    ASSERT(namelen == strlen(fname));
    uint32_t fsize = *(uint32_t *)(buffer + sizeof(uint8_t) + namelen + 1);
    ALWAYS_ASSERT(fsize >= 0);  // we might send empty files, e.g., the durable marker
    std::cout << "[Backup] Receiving file: " << fname << ", " << fsize << " bytes\n";

    // Now we have everything for this log file, receive it on large chunks
    int fd = os_openat(dfd, fname, O_CREAT|O_WRONLY);
    while (fsize) {
      auto size = std::min(fsize, uint32_t{4096});
      tcp::receive(cctx->server_sockfd, buffer, size);
      os_write(fd, buffer, size);
      fsize -= size;
    }
    ALWAYS_ASSERT(fsize == 0);
    os_fsync(fd);
    os_close(fd);
    std::cout << "[Backup] Written " << fname << std::endl;
  }

  // let the primary know we got all files
  tcp::send_ack(cctx->server_sockfd);

  std::cout << "[Backup] Received initial log records.\n";
  if (sysconf::log_ship_by_rdma) {
    std::thread t(backup_daemon_rdma);
    t.detach();
  } else {
    std::thread t(backup_daemon_tcp, cctx);
    t.detach();
  }
}
}  // namespace rep

