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

#include "../macros.h"

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

  /** Describes a bunch of log records shipped from the primary */
  struct log_packet {
    struct header {
      size_t size;  // packet size, including header + data
      LSN start_lsn;
      LSN end_lsn;
      inline size_t data_size() {
        ALWAYS_ASSERT(size > sizeof(log_packet::header));
        return size - sizeof(header);
      }
      header(size_t sz, LSN slsn, LSN elsn) :
        size(sz), start_lsn(slsn), end_lsn(elsn) {}
      header() : size(0), start_lsn(INVALID_LSN), end_lsn(INVALID_LSN) {}
    };

    header hdr;
    char *data;

    log_packet() : data(NULL) {
      hdr.size = 0;
      hdr.start_lsn = hdr.end_lsn = INVALID_LSN;
    }

    log_packet(char *buf, size_t size, LSN slsn, LSN elsn) {
      data = buf;
      hdr.size = size;
      hdr.start_lsn = slsn;
      hdr.end_lsn = elsn;
    }
  };

  extern replication_node primary_server;

  void start_as_primary();
  void start_as_backup(std::string primary_address);
  void primary_ship_log_file(int backup_fd, const char* log_fname, int log_fd);
  void primary_ship_log_buffer(
    replication_node *bnode, const char* buf, LSN start_lsn, LSN end_lsn, size_t size);
  void primary_ship_log_buffer_all(const char *buf, LSN start_lsn, LSN end_lsn, size_t size);

  // to_receive must be <= buf's capacity
  inline void receive(int fd, char *buf, size_t to_receive) {
    auto total = to_receive;
    while (to_receive) {
      to_receive -= recv(fd, buf + total - to_receive, to_receive, 0);
    }
  }

  static const int ACK_TEXT_LEN = 4;
  static const char *ACK_TEXT= "ACK";
  inline void ack_primary() {
    auto sent_bytes = send(primary_server.sockfd, ACK_TEXT, 4, 0);
    ALWAYS_ASSERT(sent_bytes == 4);
  }
  inline void expect_ack(int bfd) {
    static char buf[ACK_TEXT_LEN];
    receive(bfd, buf, ACK_TEXT_LEN);
    ALWAYS_ASSERT(strcmp(buf, ACK_TEXT) == 0);
  }
};  // namespace rep
