#pragma once

/*
 * A simple RDMA wrapper based off of the article and code at:
 * https://blog.zhaw.ch/icclab/infiniband-an-introduction-simple-ib-verbs-program-with-rdma-write/
 *
 * Author: Tianzheng Wang
 *
 * A typical usage is:
 * 1. Create an RDMA context, e.g., ctx = new rdma::context().
 * 2. Register one or more memory buffers using ctx->register_memory(...).
 *    The register_memory() function uses ibverbs to register memory regions for the
 *    buffer address provided. It can be called multiple times to register different
 *    buffers. The buffers are fully controlled by the user, this library doesn't
 *    interpret buffer contents.
 * 3. After registering all buffers needed, call ctx->finish_init() which finishes
 *    other needed steps before changing to RTS state.
 *
 * TODO(tzwang): allow dynamic add/delete of buffers.
 */

#include <mutex>
#include <string>
#include <vector>

#include <infiniband/verbs.h>
#include <netdb.h>
#include <string.h>

#include "../macros.h"
#include "sm-common.h"

// Use the experimental verbs and libmlx5 on Connect-IB to do atomic ops
// #define EXP_VERBS 1

namespace rdma {

struct context{
  friend class ib_connection;

private:
  const static int RDMA_WRID = 3;
  const static int tx_depth = 100;
#ifdef EXP_VERBS
  const static int QP_EXP_RTS_ATTR =
    IBV_EXP_QP_STATE | IBV_EXP_QP_TIMEOUT | IBV_EXP_QP_RETRY_CNT |
    IBV_EXP_QP_RNR_RETRY | IBV_EXP_QP_SQ_PSN | IBV_EXP_QP_MAX_QP_RD_ATOMIC;
  const static int QP_EXP_RTR_ATTR = IBV_EXP_QP_STATE | IBV_EXP_QP_AV | IBV_EXP_QP_PATH_MTU |
    IBV_EXP_QP_DEST_QPN | IBV_EXP_QP_RQ_PSN | IBV_EXP_QP_MAX_DEST_RD_ATOMIC | IBV_EXP_QP_MIN_RNR_TIMER;
#else
  const static int QP_RTS_ATTR =
    IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
    IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  const static int QP_RTR_ATTR = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
    IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
#endif

  struct memory_region {
    struct ibv_mr *mr;
    char *buf;
    uint64_t buf_size;
    memory_region(struct ibv_pd *pd, char *addr, uint64_t size) : buf(addr), buf_size(size) {
#ifdef EXP_VERBS
      struct ibv_exp_reg_mr_in in;
      memset(&in, 0, sizeof(in));
      in.pd = pd;
      in.addr = buf;
      in.length = buf_size;
      in.exp_access = IBV_EXP_ACCESS_REMOTE_WRITE |
                      IBV_EXP_ACCESS_REMOTE_READ |
                      IBV_EXP_ACCESS_REMOTE_ATOMIC |
                      IBV_EXP_ACCESS_LOCAL_WRITE;
      in.create_flags = IBV_EXP_REG_MR_CREATE_CONTIG;
      mr = ibv_exp_reg_mr(&in);
#else
      mr = ibv_reg_mr(pd, buf, buf_size,
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE |
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
#endif
      ALWAYS_ASSERT(mr);
      THROW_IF(not mr, illegal_argument, "ibv_reg_mr() failed");
    }
    ~memory_region() {
      ibv_dereg_mr(mr);
      memset(this, 0, sizeof(*this));
    }
  };

  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_qp *qp;
  struct ibv_comp_channel *ch;
  struct ibv_device *ib_dev;

  struct ibv_sge sge_list;
  struct ibv_send_wr wr;
#ifdef EXP_VERBS
  struct ibv_exp_send_wr exp_wr;
#endif

  std::string server_name;
  std::string port;
  int ib_port;

  std::vector<memory_region*> mem_regions;
  std::mutex mr_lock;

  struct ib_connection *local_connection;
  struct ib_connection *remote_connection;

  // Ready-to-receive/ready-to-send attr for state change later
#ifdef EXP_VERBS
  struct ibv_exp_qp_attr rtr_attr;
  struct ibv_exp_qp_attr rts_attr;
#else
  struct ibv_qp_attr rtr_attr;
  struct ibv_qp_attr rts_attr;
#endif

  void tcp_client_connect();
  void exchange_ib_connection_info(int peer_sockfd);
  void qp_change_state_rts();
  void qp_change_state_rtr();

  void init(const char *server);
  inline bool is_server() { return server_name.length() == 0; }

public:
  context(std::string& server, std::string& port, int ib_port) :
    pd(nullptr), port(port), ib_port(ib_port),
    local_connection(nullptr), remote_connection(nullptr) {
    init(server.c_str());
  }
  context(std::string& port, int ib_port) :
    pd(nullptr), port(port), ib_port(ib_port),
    local_connection(nullptr), remote_connection(nullptr) {
    init(nullptr);
  }
  ~context();
  void finish_init();
  inline char *get_memory_region(uint32_t idx) { return mem_regions[idx]->buf; }

  inline uint32_t register_memory(char *address, uint64_t size) {
    mr_lock.lock();
    DEFER(mr_lock.unlock());
    uint32_t idx = mem_regions.size();
    ALWAYS_ASSERT(pd);
    mem_regions.push_back(new memory_region(pd, address, size));
    return idx;
  }

  /* Write [size] of bytes placed at the offset of [local_offset] of the 
   * buffer specified by [index] to remote address + [remote_offset] */
  void rdma_write(
    uint32_t local_index, uint64_t local_offset,
    uint32_t remote_index, uint64_t remote_offset, uint64_t size);

  /* Same as rdma_write() above, but with immediate data [imm_data] */
  void rdma_write(
    uint32_t local_index, uint64_t local_offset,
    uint32_t remote_index, uint64_t remote_offset,
    uint64_t size, uint32_t imm_data);

  /* Read [size] of bytes placed at the [remote_index] buffer with [remote_offset]
   * into [local_index] buffer with [local_offset]. */
  void rdma_read(
    uint32_t local_index, uint64_t local_offset,
    uint32_t remote_index, uint64_t remote_offset,
    uint32_t size);

  /* Conduct a CAS at the offset of the specified remote buffer.
   * Returns the old value. */
  uint64_t rdma_compare_and_swap(
    uint32_t local_index,    // Buffer for storing the old value
    uint64_t local_offset,   // Where in the buffer to store the old value
    uint32_t remote_index,   // Remote buffer for doing the CAS
    uint64_t remote_offset,  // Where in the remote buffer to do the CAS
    uint64_t expected,
    uint64_t new_value);

  /* Post a receive request to "receive" data sent by rdma_write with immediate from the peer.
   * Returns the immediate, the caller should know where to look for the data.
   */
  uint32_t receive_rdma_with_imm();
};

struct ib_connection {
  const static int kMaxMemoryRegions = 256;

  int lid;
  int qpn;
  int psn;
  uint32_t nr_memory_regions;
  unsigned rkeys[kMaxMemoryRegions];
  unsigned long long vaddrs[kMaxMemoryRegions];

  ib_connection(struct context *ctx);
  ib_connection() {}
};

}  // namespace rdma

