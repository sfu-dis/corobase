#pragma once

#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/fcntl.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "../macros.h"
#include "sm-common.h"

namespace tcp {

static const int ACK_TEXT_LEN = 4;
static const char* ACK_TEXT = "ACK";

// to_receive must be <= buf's capacity
inline void receive(int fd, char* buf, size_t to_receive) {
  auto total = to_receive;
  while (to_receive) {
    to_receive -= recv(fd, buf + total - to_receive, to_receive, 0);
  }
}

inline void expect_ack(int bfd) {
  static char buf[ACK_TEXT_LEN];
  receive(bfd, buf, ACK_TEXT_LEN);
  ALWAYS_ASSERT(strcmp(buf, ACK_TEXT) == 0);
}

inline void send_ack(int sockfd) {
  auto sent_bytes = send(sockfd, ACK_TEXT, 4, 0);
  ALWAYS_ASSERT(sent_bytes == 4);
}

struct server_context {
 private:
  char sock_addr[INET_ADDRSTRLEN];
  int sockfd;

 public:
  server_context(std::string& port, uint32_t nclients);
  ~server_context() {
    if (sockfd) {
      close(sockfd);
    }
  }
  inline const char* get_sock_addr() { return sock_addr; }
  int expect_client(char *client_addr = nullptr);
};

struct client_context {
  int server_sockfd;
  char server_sock_addr[INET_ADDRSTRLEN];
  client_context(std::string& server, std::string& port);
  ~client_context() {
    if (server_sockfd) {
      close(server_sockfd);
    }
  }
};
}  // namespace tcp
