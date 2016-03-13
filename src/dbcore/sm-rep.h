#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <thread>
#include <unordered_map>

namespace rep {
  struct replication_node {
    int sockfd;
    char sock_addr[INET_ADDRSTRLEN];
    replication_node() : sockfd(0) {}
    ~replication_node() {
      if (sockfd) {
        close(sockfd);
      }
    }
    void init(std::string& addr_port);
  };

  extern bool shutdown;

  void start_as_primary();
  void start_as_backup(std::string primary_address);
  void primary_ship_log_file(int backup_fd, const char* log_fname, int log_fd);

  struct replication_packet {
    uint32_t payload_size;
    replication_packet(uint32_t size) : payload_size(size) {}
    ~replication_packet() { payload_size = 0; }
    inline uint32_t size() { return payload_size + sizeof(payload_size); }
    inline char* payload() { return (char *)this + sizeof(payload_size); }
  };
};  // namespace rep
