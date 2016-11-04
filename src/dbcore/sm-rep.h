#pragma once

#include <iostream>
#include <thread>
#include <vector>

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/sendfile.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <thread>

#include "rdma.h"
#include "tcp.h"
#include "../macros.h"

#include "sm-chkpt.h"
#include "sm-config.h"
#include "sm-file.h"
#include "sm-log.h"
#include "sm-log-offset.h"

struct write_record_t;

namespace rep {

extern std::vector<int> backup_sockfds;
extern std::mutex backup_sockfds_mutex;

struct backup_start_metadata {
  char chkpt_marker[CHKPT_FILE_NAME_BUFSZ];
  char durable_marker[DURABLE_FILE_NAME_BUFSZ];
  char nxt_marker[NXT_SEG_FILE_NAME_BUFSZ];
  uint64_t chkpt_size;
  uint64_t log_size;
  uint64_t num_log_files;
  // What follows is a list of <log file name, file size> pairs

  backup_start_metadata() :
    chkpt_size(0)
    , log_size(0)
    , num_log_files(0) {}

  inline void add_log_file(char const* fname, uint64_t size) {
    memcpy((char*)this + sizeof(*this) +
           (SEGMENT_FILE_NAME_BUFSZ + sizeof(uint64_t)) * num_log_files,
           fname, SEGMENT_FILE_NAME_BUFSZ);
    memcpy((char*)this + sizeof(*this) +
           (SEGMENT_FILE_NAME_BUFSZ + sizeof(uint64_t)) * num_log_files + SEGMENT_FILE_NAME_BUFSZ,
           (char*)&size, sizeof(size));
    ++num_log_files;
    log_size += size;
  }
  inline uint64_t size() {
    return sizeof(*this) + (SEGMENT_FILE_NAME_BUFSZ + sizeof(uint64_t))* num_log_files;
  }
  inline char* get_log_file(uint32_t idx) {
    return (char*)this + sizeof(*this) + (sizeof(uint64_t) + SEGMENT_FILE_NAME_BUFSZ) * idx;
  }
};

inline backup_start_metadata* allocate_backup_start_metadata(uint64_t nlogfiles) {
  uint32_t size = sizeof(backup_start_metadata) +
    nlogfiles * (SEGMENT_FILE_NAME_BUFSZ + sizeof(uint64_t));
  backup_start_metadata *md = (backup_start_metadata*)malloc(size);
  return md;
}

// Common functions
void start_as_primary();
void primary_ship_log_buffer_all(const char *buf, uint32_t size);
void redo_daemon();

/* RDMA message states, for log shipping using RDMA only */
static const uint64_t RDMA_READY_TO_RECEIVE = 1UL;
static const uint64_t RDMA_WAITING = 2UL;

// RDMA-specific functions
void primary_init_rdma();
void backup_daemon_rdma(tcp::client_context* tcp_ctx);
void primary_ship_log_buffer_rdma(const char *buf, uint32_t size);
void update_pdest_on_backup_rdma(write_record_t* w);

// TCP-specific functions
void start_as_backup_tcp();
void backup_daemon_tcp(tcp::client_context *cctx);
void primary_daemon_tcp();
void send_log_files_after_tcp(int backup_fd, backup_start_metadata* md, uint64_t chkpt_start);

/* Send a chunk of log records (still in memory log buffer) to a backup via TCP.
 */
void primary_ship_log_buffer_tcp(
  int backup_sockfd, const char* buf, uint32_t size);
};  // namespace rep
