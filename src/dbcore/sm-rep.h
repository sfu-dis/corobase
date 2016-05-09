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
  uint32_t size;
  char *data;

  log_packet() : size(0), data(nullptr) {}
  log_packet(char *buf, size_t size) : size(size), data(buf) {}
};

void start_as_primary();
void start_as_backup(std::string primary_address);
void primary_ship_log_file(int backup_fd, const char* log_fname, int log_fd);
void primary_ship_log_buffer(
  int backup_sockfd, const char* buf, uint32_t size);
void primary_ship_log_buffer_all(const char *buf, uint32_t size);

};  // namespace rep
