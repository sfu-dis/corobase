#include <sys/fcntl.h>
#include <sys/sendfile.h>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "../macros.h"
#include "sm-common.h"
#include "sm-config.h"
#include "sm-log.h"
#include "sm-rep.h"

namespace rep {
#define PRIMARY_PORT "10002"
#define MAX_NBACKUPS 10

replication_node primary_server;
bool shutdown = false;

// Maps sockfd -> rep_node*
std::unordered_map<int, replication_node*> backup_nodes;
std::mutex nodes_mutex;

void primary_server_daemon() {
  char s[128];
  gethostname(s, 128);
  printf("[Primary] %s:%s\n", s, PRIMARY_PORT);

  // Now start to listen for incoming connections
  while (not volatile_read(shutdown)) {
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

void start_as_primary() {
  ALWAYS_ASSERT(!sysconf::is_backup_srv);
  shutdown = false;

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
      uint32_t to_receive = 1;  // expect one byte of for nfiles
      uint32_t bytes_received = 0;
      while (to_receive) {
        bytes_received = recv(sockfd, &nfiles, to_receive, 0);
        to_receive -= bytes_received;
      }

      ALWAYS_ASSERT(nfiles);

      while (nfiles--) {
        // First packet, get filename etc
        bytes_received = 0;
        to_receive = 256;
        while (to_receive) {
          bytes_received = recv(sockfd, buffer + 256 - to_receive, to_receive, 0);
          to_receive -= bytes_received;
        }

        ALWAYS_ASSERT(bytes_received == 256);

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
          bytes_received = recv(sockfd, buffer, size, 0);
          os_write(fd, buffer, bytes_received);
          std::cout << "[Backup] Written " << bytes_received << " bytes for " << fname << std::endl;
          fsize -= bytes_received;
        }
        os_close(fd);
      }

      std::cout << "[Backup] Received initial log records.\n";
      std::thread t(backup_daemon);
      t.detach();
      return;
    }
  }
  THROW_IF(true, illegal_argument, "Can't bind()");
}
}  // namespace rep

