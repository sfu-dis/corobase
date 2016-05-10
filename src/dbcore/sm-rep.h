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
void start_as_primary();
void start_as_backup(std::string primary_address);

/* Ships a log file (on-disk) to a backup server via TCP.
 * For primary server only. So far the only user is when the
 * primary server starts and has loaded the database.
 */
void primary_ship_log_file(int backup_fd, const char* log_fname, int log_fd);
void primary_ship_log_buffer(
  int backup_sockfd, const char* buf, uint32_t size);
void primary_ship_log_buffer_all(const char *buf, uint32_t size);

};  // namespace rep
