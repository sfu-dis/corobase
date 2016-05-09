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
/** Describes a bunch of log records shipped from the primary */
struct log_packet {
  struct header {
    size_t size;  // packet size, including header + data
    LSN end_lsn;
    inline size_t data_size() {
      ALWAYS_ASSERT(size > sizeof(log_packet::header));
      return size - sizeof(header);
    }
    header(size_t sz, LSN elsn) :
      size(sz), end_lsn(elsn) {}
    header() : size(0), end_lsn(INVALID_LSN) {}
  };

  header hdr;
  char *data;

  log_packet() : data(NULL) {
    hdr.size = 0;
    hdr.end_lsn = INVALID_LSN;
  }

  log_packet(char *buf, size_t size, LSN elsn) {
    data = buf;
    hdr.size = size;
    hdr.end_lsn = elsn;
  }
};

void start_as_primary();
void start_as_backup(std::string primary_address);
void primary_ship_log_file(int backup_fd, const char* log_fname, int log_fd);
void primary_ship_log_buffer(
  int backup_sockfd, const char* buf, LSN end_lsn, size_t size);
void primary_ship_log_buffer_all(const char *buf, LSN end_lsn, size_t size);

};  // namespace rep
