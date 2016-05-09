#pragma once

/*
 * A simple RDMA wrapper based off of the article and code at:
 * https://blog.zhaw.ch/icclab/infiniband-an-introduction-simple-ib-verbs-program-with-rdma-write/
 *
 * Author: Tianzheng Wang
 *
 * Currently we use TCP to exchange connection information, without using rdma_cm.
 * Each peer associates itself with a context object (being the server or a client).
 * The context object provides two buffers: one data buffer and one message buffer.
 * The former is fully controlled by the user - customized size and allocation.
 * The latter is a fixed-length MAX_MSG_SIZE bytes (default 8 bytes) mostly for
 * exchanging simple, short messages.
 */

#include <string>

#include <infiniband/verbs.h>
#include <netdb.h>

namespace rdma {

struct context{
  friend class ib_connection;

private:
  const static int RDMA_WRID = 3;
  const static int MSG_WRID = 2;
  const static int MAX_MSG_SIZE = 8;
  const static int tx_depth = 100;
  const static int QP_RTS_ATTR =
    IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
    IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  const static int QP_RTR_ATTR = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
    IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_mr *buf_mr;
  struct ibv_mr *msg_mr;
  struct ibv_cq *rcq;
  struct ibv_cq *scq;
  struct ibv_qp *qp;
  struct ibv_comp_channel *ch;
  struct ibv_device *ib_dev;

  struct ibv_sge sge_list;
  struct ibv_send_wr wr;

  std::string server_name;
  char *port;
  int ib_port;
  char *buf;
  uint64_t buf_size;

  char msg[MAX_MSG_SIZE];

  struct ib_connection *local_connection;
  struct ib_connection *remote_connection;

  // Ready-to-receive/ready-to-send attr for state change later
  struct ibv_qp_attr rtr_attr;
  struct ibv_qp_attr rts_attr;

  void tcp_client_connect();
  void exchange_ib_connection_info(int peer_sockfd);
  void qp_change_state_rts();
  void qp_change_state_rtr();

  void init(char *server);
  inline bool is_server() { return server_name.length() == 0; }

public:
  context(char *server, char *port, int ib_port, char *buf, uint64_t buf_size) :
    port(port), ib_port(ib_port), buf(buf), buf_size(buf_size),
    local_connection(nullptr), remote_connection(nullptr) {
    init(server);
  }
  context(char *port, int ib_port, char *buf, uint64_t buf_size) :
    port(port), ib_port(ib_port), buf(buf), buf_size(buf_size),
    local_connection(nullptr), remote_connection(nullptr) {
    init(nullptr);
  }
  ~context();
  inline char *get_buf() { return buf; }
  inline char *get_msg() { return msg; }
  void rdma_write(uint64_t offset, uint64_t size);
  void rdma_write_msg();
};

struct ib_connection {
  int lid;
  int qpn;
  int psn;
  unsigned buf_rkey;
  unsigned msg_rkey;
  unsigned long long buf_vaddr;
  unsigned long long msg_vaddr;

  ib_connection(struct context *ctx);
  ib_connection(char *msg);
};

}  // namespace rdma

