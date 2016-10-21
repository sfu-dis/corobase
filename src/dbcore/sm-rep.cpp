#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
// for primary server only
tcp::server_context *primary_tcp_ctx = nullptr;
std::vector<int> backup_sockfds;

/* Ship a log file (on-disk) to a backup server via TCP using sendfile().
 * For primary server only. So far the only user is when the
 * primary server starts and has loaded the database.
 */
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

// A daemon that runs on the primary to bring up standby servers by
// shipping the log files of a newly loaded database via TCP.
void primary_start_daemon() {
  logmgr->flush();  // persist everything before shipping
  do {
    int backup_sockfd = primary_tcp_ctx->expect_client();
    backup_sockfds.push_back(backup_sockfd);
    // Got a new backup, send out my logs
    // TODO(tzwang): handle backup joining after primary started forward processing
    dirent_iterator dir(config::log_dir.c_str());
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
  } while (++config::num_active_backups < config::num_backups);

  if (config::log_ship_by_rdma) {
    delete primary_tcp_ctx;
    primary_init_rdma();
  }
}

void primary_ship_log_buffer_all(const char *buf, uint32_t size) {
  if (config::log_ship_by_rdma) {
    primary_ship_log_buffer_rdma(buf, size);
  } else {
    ASSERT(backup_sockfds.size());
    for (int &fd : backup_sockfds) {
      primary_ship_log_buffer_tcp(fd, buf, size);
    }
  }
}

void start_as_primary() {
  ALWAYS_ASSERT(not config::is_backup_srv());
  primary_tcp_ctx = new tcp::server_context(config::primary_port, config::num_backups);
  // Spawn a new thread to listen to incoming connections
  std::thread t(primary_start_daemon);
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

void start_as_backup() {
  ALWAYS_ASSERT(config::is_backup_srv());
  std::cout << "[Backup] Connecting to " << config::primary_srv << ":"
    << config::primary_port << "\n";
  tcp::client_context *cctx = new tcp::client_context(
    config::primary_srv, config::primary_port);

  // Wait for the primary to ship the first part of the log so I can start "recovery"
  char buffer[4096];
  char fname[251];
  dirent_iterator dir(config::log_dir.c_str());
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
  if (config::log_ship_by_rdma) {
    std::thread t(backup_daemon_rdma, cctx);
    t.detach();
    // The daemon will delete cctx
  } else {
    std::thread t(backup_daemon_tcp, cctx);
    t.detach();
  }
}
}  // namespace rep

