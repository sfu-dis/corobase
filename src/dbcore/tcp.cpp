#include <unistd.h>
#include <string.h>

#include <sys/mman.h>

#include <iostream>

#include "tcp.h"

namespace tcp {

server_context::server_context(const char *port, uint32_t nclients) : sockfd(0) {
  gethostname(sock_addr, INET_ADDRSTRLEN);
  printf("[Server] %s:%s\n", get_sock_addr(), port);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  struct addrinfo *result = nullptr;
  int ret = getaddrinfo(nullptr, port, &hints, &result);
  THROW_IF(ret, illegal_argument, "Error getaddrinfo(): %s", gai_strerror(ret));
  DEFER(freeaddrinfo(result));

  for (auto *r = result; r; r = r->ai_next) {
    sockfd = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
    if (sockfd == -1)
      continue;

    int yes = 1;
    THROW_IF(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1,
             illegal_argument, "Can't setsocketopt()");

    if (bind(sockfd, r->ai_addr, r->ai_addrlen) == 0) {
      struct sockaddr_in sin;
      socklen_t len = sizeof(struct sockaddr_in);
      getsockname(sockfd, (struct sockaddr *)&sin, &len);
      inet_ntop(AF_INET, &sin.sin_addr, sock_addr, sizeof(sock_addr));
      THROW_IF(listen(sockfd, nclients) == -1, illegal_argument, "Can't listen()");
      return;
    }
  }
  THROW_IF(true, illegal_argument, "Can't bind()");
}

int server_context::expect_client() {
  struct sockaddr addr;
  socklen_t addr_size = sizeof(struct sockaddr_storage);

  int fd = accept(sockfd, &addr, &addr_size);
  ALWAYS_ASSERT(fd);

  struct sockaddr_in sin;
  socklen_t len = sizeof(struct sockaddr_in);
  getsockname(fd, (struct sockaddr *)&sin, &len);

  char s[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &sin.sin_addr, s, INET_ADDRSTRLEN);
  printf("[Server] New client: %s\n", s);
  return fd;
}

client_context::client_context(const char *server, const char *port) : server_sockfd(0) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *servinfo = nullptr;
  int ret = getaddrinfo(server, port, &hints, &servinfo);
  THROW_IF(ret != 0, illegal_argument, "Error getaddrinfo(): %s", gai_strerror(ret));
  DEFER(freeaddrinfo(servinfo));

  for (auto *r = servinfo; r; r = r->ai_next) {
    server_sockfd = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
    if (server_sockfd == -1) {
      continue;
    }

    int yes = 1;
    int ret = setsockopt(server_sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
    THROW_IF(ret == -1, illegal_argument, "setsocketopt() failed");

    if (connect(server_sockfd, r->ai_addr, r->ai_addrlen) == 0) {
      inet_ntop(r->ai_family, (void *)&((struct sockaddr_in*)server_sock_addr)->sin_addr,
        server_sock_addr, sizeof(server_sock_addr));
      std::cout << "[Client] Connected to " << server << "\n";
      return;
    }
  }
  THROW_IF(true, illegal_argument, "Can't bind()");
}
}  // namespace tcp
