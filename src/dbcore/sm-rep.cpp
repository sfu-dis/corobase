#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/sendfile.h>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "sm-common.h"
#include "sm-config.h"
#include "sm-log.h"
#include "sm-log-offset.h"
#include "sm-rep.h"

namespace rep {
#define PRIMARY_PORT "10002"
#define MAX_NBACKUPS 10

replication_node primary_server;

// Maps sockfd -> rep_node*
std::unordered_map<int, replication_node*> backup_nodes;
std::mutex nodes_mutex;

void primary_server_daemon() {
  char s[128];
  gethostname(s, 128);
  printf("[Primary] %s:%s\n", s, PRIMARY_PORT);

  // Now start to listen for incoming connections
  do {
    struct sockaddr addr;
    socklen_t addr_size = sizeof(struct sockaddr_storage);
    int fd = accept(primary_server.sockfd, &addr, &addr_size);
    if (fd == -1)
        continue;

    struct sockaddr_in sin;
    socklen_t len = sizeof(struct sockaddr_in);
    getsockname(fd, (struct sockaddr *)&sin, &len);

    auto* rnode = new replication_node;
    rnode->sockfd = fd;
    inet_ntop(AF_INET, &sin.sin_addr, rnode->sock_addr, sizeof(rnode->sock_addr));

    {
      std::lock_guard<std::mutex> lock(nodes_mutex);
      backup_nodes[fd] = rnode;
    }
    printf("[Primary] New backup: %s\n", rnode->sock_addr);

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

    auto sent_bytes = send(rnode->sockfd, &nfiles, sizeof(nfiles), 0);
    ALWAYS_ASSERT(sent_bytes == sizeof(nfiles));

    // we're just starting the db, so send everything we have
    for (char const *fname : dir) {
      if (fname[0] == '.')
        continue;
      int log_fd = os_openat(dfd, fname, O_RDONLY);
      primary_ship_log_file(rnode->sockfd, fname, log_fd);
      close(log_fd);
    }

    // wait for ack from the backup
    expect_ack(rnode->sockfd);
    printf("[Primary] Backup %s received log\n", rnode->sock_addr);
  } while (++sysconf::num_active_backups < sysconf::num_backups);
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
  printf("[Primary] %s: sent %ld bytes of metadata\n", log_fname, sent_bytes);

  // Now we can send the file, in one go with sendfile()
  if (st.st_size) {
    off_t offset = 0;
    sent_bytes = sendfile(backup_fd, log_fd, &offset, st.st_size);
    ALWAYS_ASSERT(sent_bytes == st.st_size);
    ALWAYS_ASSERT(offset == st.st_size);
    printf("[Primary] %s: %ld bytes, sent %ld bytes\n", log_fname, st.st_size, sent_bytes);
  }
}

// Send the log buffer to backups
void primary_ship_log_buffer(
  replication_node *bnode, const char* buf, LSN start_lsn, LSN end_lsn, size_t size) {
  ALWAYS_ASSERT(size);
  ALWAYS_ASSERT(end_lsn > start_lsn);
  log_packet::header lph(size + sizeof(log_packet::header), start_lsn, end_lsn);
  ASSERT(lph.start_lsn.segment() == lph.end_lsn.segment());
  size_t nbytes = send(bnode->sockfd, (char *)&lph, sizeof(lph), 0);
  THROW_IF(nbytes != sizeof(lph), log_file_error, "Incomplete log shipping (header)");
  nbytes = send(bnode->sockfd, buf, size, 0);
  THROW_IF(nbytes != size, log_file_error, "Incomplete log shipping (data)");
  printf("[Primary] Shipped %lu bytes of log (%lx-%lx, segment %d) to backup %s\n",
    nbytes, lph.start_lsn.offset(), lph.end_lsn.offset(), lph.start_lsn.segment(), bnode->sock_addr);

  // expect an ack message
  expect_ack(bnode->sockfd);
  printf("[Primary] Backup %s received log %lx-%lx\n",
    bnode->sock_addr, lph.start_lsn.offset(), lph.end_lsn.offset());
}

void primary_ship_log_buffer_all(const char *buf, LSN start_lsn, LSN end_lsn, size_t size) {
  for (auto& n : backup_nodes) {
    primary_ship_log_buffer(n.second, buf, start_lsn, end_lsn, size);
  }
}

void start_as_primary() {
  ALWAYS_ASSERT(!sysconf::is_backup_srv);
  struct addrinfo hints;
  struct addrinfo *result;

  memset(&hints, '\0', sizeof(struct addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  char *paddr = NULL;
  hints.ai_flags = AI_PASSIVE;

  int ret = getaddrinfo(paddr, PRIMARY_PORT, &hints, &result);
  THROW_IF(ret, illegal_argument, "Error getaddrinfo(): %s", gai_strerror(ret));
  DEFER(freeaddrinfo(result));

  // flush the log so we can really ship it
  LSN dlsn = logmgr->flush_cur_lsn();
  std::cout << "[Primary] durable LSN offset: " << dlsn.offset() << "\n";

  for (auto *r = result; r; r = r->ai_next) {
    int sockfd = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
    if (sockfd == -1)
      continue;

    int yes = 1;
    THROW_IF(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1,
             illegal_argument, "Can't setsocketopt()");

    if (bind(sockfd, r->ai_addr, r->ai_addrlen) == 0) {
      primary_server.sockfd = sockfd;
      struct sockaddr_in sin;
      socklen_t len = sizeof(struct sockaddr_in);
      getsockname(sockfd, (struct sockaddr *)&sin, &len);
      inet_ntop(AF_INET, &sin.sin_addr, primary_server.sock_addr, sizeof(primary_server.sock_addr));
      THROW_IF(listen(sockfd, MAX_NBACKUPS) == -1, illegal_argument, "Can't listen()");

      // Spawn a new thread to listen to incoming connections
      std::thread t (primary_server_daemon);
      t.detach();
      return;
    }
  }
  THROW_IF(true, illegal_argument, "Can't bind()");
}

void backup_daemon() {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());
  // Listen to incoming log records from the primary
  log_packet::header lph;
  while (1) {
    // expect a log_packet header
    receive(primary_server.sockfd, (char *)&lph, sizeof(lph));
    ALWAYS_ASSERT(lph.data_size());
    ALWAYS_ASSERT(lph.start_lsn.segment() == lph.end_lsn.segment());
    ALWAYS_ASSERT(lph.start_lsn < lph.end_lsn);
    ALWAYS_ASSERT(lph.start_lsn == logmgr->durable_flushed_lsn());

    // prepare segment if needed
    segment_id *sid = logmgr->assign_segment(lph.start_lsn.offset(), lph.end_lsn.offset());
    ALWAYS_ASSERT(sid);
    ASSERT(lph.start_lsn == logmgr->durable_flushed_lsn());
    ASSERT(sid->make_lsn(lph.start_lsn.offset()) == lph.start_lsn);
    ASSERT(sid->make_lsn(lph.end_lsn.offset()) == lph.end_lsn);

    // expect the real log data
    std::cout << "[Backup] Will receive " << lph.data_size() << " bytes\n";
    ALWAYS_ASSERT(lph.data_size());
    auto& logbuf = logmgr->get_logbuf();
    char *buf = logbuf.write_buf(sid->buf_offset(lph.start_lsn), lph.data_size());
    ALWAYS_ASSERT(buf);   // XXX: consider different log buffer sizes than the primary's later
    receive(primary_server.sockfd, buf, lph.data_size());
    std::cout << "[Backup] Recieved " << lph.data_size() << " bytes ("
      << std::hex << lph.start_lsn.offset() << "-" << lph.end_lsn.offset() << std::dec << ")\n";

    // now got the batch of log records, persist them
    if (sysconf::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf.advance_writer(sid->buf_offset(lph.end_lsn));
    } else {
      logmgr->flush_log_buffer(logbuf, lph.end_lsn.offset(), true);
      ASSERT(logmgr->durable_flushed_lsn() == lph.end_lsn);
    }

    ack_primary();

    // roll forward
    logmgr->redo_log(lph.start_lsn, lph.end_lsn);
    printf("[Backup] Rolled forward log %lx-%lx\n", lph.start_lsn.offset(), lph.end_lsn.offset());
    if (sysconf::nvram_log_buffer)
      logmgr->flush_log_buffer(logbuf, lph.end_lsn.offset(), true);
  }
}

void start_as_backup(std::string primary_address) {
  std::cout << "[Backup] Connecting to " << primary_address << "\n";
  struct addrinfo hints, *servinfo = NULL;
  memset(&hints, '\0', sizeof(struct addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  char *paddr = NULL;
  paddr = (char *)primary_address.c_str();

  int ret = getaddrinfo(paddr, PRIMARY_PORT, &hints, &servinfo);
  THROW_IF(ret != 0, illegal_argument, "Error getaddrinfo(): %s", gai_strerror(ret));
  DEFER(freeaddrinfo(servinfo));

  for (auto *r = servinfo; r; r = r->ai_next) {
    int sockfd = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
    if (sockfd == -1)
        continue;

    int yes = 1;
    THROW_IF(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1,
             illegal_argument, "Can't setsocketopt()");

    ASSERT(primary_server.sockfd == 0);
    primary_server.sockfd = sockfd;
    if (connect(sockfd, r->ai_addr, r->ai_addrlen) == 0) {
      inet_ntop(r->ai_family, (void *)&((struct sockaddr_in*)primary_server.sock_addr)->sin_addr,
        primary_server.sock_addr, sizeof(primary_server.sock_addr));
      std::cout << "[Backup] Connected to " << primary_address << "\n";

      // Wait for the primary to ship the first part of the log so I can start "recovery"
      char buffer[4096];
      char fname[251];
      dirent_iterator dir(sysconf::log_dir.c_str());
      int dfd = dir.dup();

      uint8_t nfiles = 0;
      receive(sockfd, (char *)&nfiles, sizeof(nfiles)); // expect one byte of for nfiles
      ALWAYS_ASSERT(nfiles);

      while (nfiles--) {
        // First packet, get filename etc
        receive(sockfd, buffer, 256);

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
          receive(sockfd, buffer, size);
          os_write(fd, buffer, size);
          fsize -= size;
        }
        os_fsync(fd);
        os_close(fd);
        std::cout << "[Backup] Written " << fsize << " bytes for " << fname << std::endl;
      }

      // let the primary know we got all files
      ack_primary();

      std::cout << "[Backup] Received initial log records.\n";
      std::thread t(backup_daemon);
      t.detach();
      return;
    }
  }
  THROW_IF(true, illegal_argument, "Can't bind()");
}
}  // namespace rep

