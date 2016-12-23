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
#include "sm-log.h"

struct write_record_t;

namespace rep {

const uint64_t kRdmaWaiting = 0x1;
const uint64_t kRdmaReadyToReceive = 0x2;
const uint64_t kRdmaPersisted = 0x4;
extern bool recover_first;

const uint32_t kMaxLogBufferPartitions = 64;
extern uint64_t logbuf_partition_bounds[kMaxLogBufferPartitions];

extern std::vector<int> backup_sockfds;
extern std::mutex backup_sockfds_mutex;

struct backup_start_metadata {
  struct log_segment {
    segment_file_name file_name;
    uint64_t size;
  };
  char chkpt_marker[CHKPT_FILE_NAME_BUFSZ];
  char durable_marker[DURABLE_FILE_NAME_BUFSZ];
  char nxt_marker[NXT_SEG_FILE_NAME_BUFSZ];
  uint64_t chkpt_size;
  uint64_t log_size;
  uint64_t num_log_files;
  log_segment segments[0];  // must be the last one

  backup_start_metadata() :
    chkpt_size(0)
    , log_size(0)
    , num_log_files(0) {}

  inline void add_log_segment(
    unsigned int segment, uint64_t start_offset, uint64_t end_offset, uint64_t size) {
    // The filename; start_offset should already be adjusted according to chkpt_start
    // so we only ship the part needed
    new (&(segments[num_log_files].file_name)) segment_file_name(segment, start_offset, end_offset);

    // The real size we're going to send
    segments[num_log_files].size = size;
    ++num_log_files;
    log_size += size;
  }
  inline uint64_t size() {
    return sizeof(*this) + sizeof(log_segment) * num_log_files;
  }
  inline log_segment* get_log_segment(uint32_t idx) {
    return &segments[idx];
  }
  void persist_marker_files() {
    ALWAYS_ASSERT(config::is_backup_srv());
    // Write the marker files
    dirent_iterator dir(config::log_dir.c_str());
    int dfd = dir.dup();
    int marker_fd = os_openat(dfd, chkpt_marker, O_CREAT|O_WRONLY);
    os_close(marker_fd);
    marker_fd = os_openat(dfd, durable_marker, O_CREAT|O_WRONLY);
    os_close(marker_fd);
    marker_fd = os_openat(dfd, nxt_marker, O_CREAT|O_WRONLY);
    os_close(marker_fd);
  }
};

inline backup_start_metadata* allocate_backup_start_metadata(uint64_t nlogfiles) {
  uint32_t size = sizeof(backup_start_metadata) + nlogfiles * sizeof(backup_start_metadata::log_segment);
  backup_start_metadata *md = (backup_start_metadata*)malloc(size);
  return md;
}

// Common functions
void start_as_primary();
void primary_ship_log_buffer_all(const char *buf, uint32_t size,
                                 bool new_seg, uint64_t new_seg_start_offset);
void redo_daemon();
backup_start_metadata* prepare_start_metadata(int& chkpt_fd, LSN& chkpt_start_lsn);

// RDMA-specific functions
void primary_daemon_rdma();
void start_as_backup_rdma();
void backup_daemon_rdam();
void primary_init_rdma();
void primary_ship_log_buffer_rdma(const char *buf, uint32_t size,
                                  bool new_seg, uint64_t new_seg_start_offset);
void update_pdest_on_backup_rdma(write_record_t* w);
void send_log_files_after_rdma(backup_start_metadata* md, LSN chkpt_start);
void rdma_wait_for_message(uint64_t msg, bool reset = true);
void set_message(uint64_t msg);
void primary_rdma_poll_send_cq(uint64_t nops);
//void primary_persist_remote_nvram(const char* buf, uint32_t size);

// TCP-specific functions
void start_as_backup_tcp();
void backup_daemon_tcp(tcp::client_context *cctx);
void primary_daemon_tcp();
void send_log_files_after_tcp(int backup_fd, backup_start_metadata* md, LSN chkpt_start);

/* Send a chunk of log records (still in memory log buffer) to a backup via TCP.
 */
void primary_ship_log_buffer_tcp(
  int backup_sockfd, const char* buf, uint32_t size);
};  // namespace rep
