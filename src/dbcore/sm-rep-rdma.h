#pragma once
#include "rdma.h"
#include "sm-rep.h"

namespace rep {

// Describes a node that uses RDMA for log shipping. Both the primary and
// backups use this class to represent others and itself. The primary would
// create multiple RdmaNodes, each encapsulates an RDMA RC connection between
// the primary and a backup. Each backup will create exactly one RdmaNode for
// the connecting and exchanging data with the primary.
class RdmaNode {
 public:
  const static int kMessageSize = 8;
  const static uint64_t kDaemonBufferSize = 128 * config::MB;

 private:
  char msg_buf_[kMessageSize] CACHE_ALIGNED;
  char daemon_buf_[kDaemonBufferSize] CACHE_ALIGNED;
  rdma::context* context_;

  // Memory region indexes - these are specific and local to the underlying
  // rdma::context; since we use a dedicated rdma::context per pair of
  // primary-backup nodes, we can safely assume the indexes we got are
  // correct given the primary and backups register memory regions in the
  // same order (which is handled totally by RdmaNode).
  uint32_t msg_buf_ridx_;
  uint32_t log_buf_ridx_;
  uint32_t log_buf_partition_bounds_ridx_;
  uint32_t daemon_buf_ridx_;
  char client_addr_[INET_ADDRSTRLEN];  // For use when as_primary is true only

 public:
  RdmaNode(bool as_primary)
      : context_(nullptr),
        msg_buf_ridx_(-1),
        log_buf_ridx_(-1),
        log_buf_partition_bounds_ridx_(-1),
        daemon_buf_ridx_(-1) {
    if (as_primary) {
      context_ = new rdma::context(config::primary_port, 1);
    } else {
      context_ =
          new rdma::context(config::primary_srv, config::primary_port, 1);
    }

    memset(msg_buf_, 0, kMessageSize);
    memset(daemon_buf_, 0, kDaemonBufferSize);

    // XXX(tzawng): Assuming the primary and backup register in the same order
    msg_buf_ridx_ = context_->register_memory(msg_buf_, kMessageSize);
    daemon_buf_ridx_ =
        context_->register_memory(daemon_buf_, kDaemonBufferSize);

    auto* logbuf = sm_log::get_logbuf();
    log_buf_ridx_ =
        context_->register_memory(logbuf->_data, logbuf->window_size() * 2);
    log_buf_partition_bounds_ridx_ =
        context_->register_memory((char*)rep::logbuf_partition_bounds,
                                  sizeof(uint64_t) * kMaxLogBufferPartitions);

    context_->finish_init(client_addr_);
    LOG(INFO) << "RDMA initialized";
  }

  ~RdmaNode() { delete context_; }

  inline char *GetClientAddress() { return client_addr_; }
  inline uint32_t GetLogBufferIndex() { return log_buf_ridx_; }
  inline uint32_t GetBoundsIndex() { return log_buf_partition_bounds_ridx_; }

  inline void WaitForMessageAsPrimary(uint64_t msg, bool reset = true) {
    ALWAYS_ASSERT(!config::is_backup_srv());
    uint64_t *p = (uint64_t*)GetMemoryRegion(msg_buf_ridx_);
    while (true) {
      uint64_t m = volatile_read(*p);
      if (m & msg) {
        if (reset) {
          // reset it so I'm not confused next time
          *p = kRdmaWaiting;
        }
        break;
      }
    }
  }

  inline void PollSendCompletionAsPrimary(uint64_t nops) {
    ALWAYS_ASSERT(!config::is_backup_srv());
    context_->poll_send_cq(nops);
  }

  inline void SetMessageAsBackup(uint64_t msg) {
    ALWAYS_ASSERT(config::is_backup_srv());
    *(uint64_t*)GetMemoryRegion(msg_buf_ridx_) = msg;
    context_->rdma_write(msg_buf_ridx_, 0, msg_buf_ridx_, 0, kMessageSize);
  }

  inline rdma::context* GetContext() { return context_; }

  inline void RdmaWriteImmDaemonBuffer(uint64_t local_offset,
                                       uint64_t remote_offset, uint64_t size,
                                       uint32_t imm, bool sync = true) {
    context_->rdma_write_imm(daemon_buf_ridx_, local_offset, daemon_buf_ridx_,
                             remote_offset, size, imm, sync);
  }
  inline void RdmaWriteImmLogBufferPartitionBounds(uint64_t local_offset,
                                                   uint64_t remote_offset,
                                                   uint64_t size, uint32_t imm,
                                                   bool sync = true) {
    context_->rdma_write_imm(log_buf_partition_bounds_ridx_, local_offset,
                             log_buf_partition_bounds_ridx_, remote_offset,
                             size, imm, sync);
  }
  inline void RdmaWriteImmLogBuffer(uint64_t local_offset,
                                    uint64_t remote_offset, uint64_t size,
                                    uint32_t imm, bool sync = true) {
    context_->rdma_write_imm(log_buf_ridx_, local_offset, log_buf_ridx_,
                             remote_offset, size, imm, sync);
  }
  inline uint64_t ReceiveImm(uint32_t* imm = nullptr) {
    return context_->receive_rdma_with_imm(imm);
  }
  inline char* GetDaemonBuffer() { return (char*)daemon_buf_; }

 private:
  inline void* GetMemoryRegion(uint32_t index) {
    return context_->get_memory_region(index);
  }
};

}  // namespace rep
