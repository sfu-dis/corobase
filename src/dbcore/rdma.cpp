#include <unistd.h>
#include <string.h>

#include "../macros.h"
#include "rdma.h"
#include "sm-common.h"
#include "tcp.h"

namespace rdma {

void context::init(char *server) {
  ALWAYS_ASSERT(buf);
  ALWAYS_ASSERT(buf_size);
  memset(buf, 0, buf_size);

  if (server) {
    server_name = std::string(server);
  } else {
    server_name = std::string("");
  }

  struct ibv_device **dev_list = ibv_get_device_list(NULL);
  THROW_IF(not dev_list, illegal_argument, "No IB devices found");

  ib_dev = dev_list[0];
  THROW_IF(not ib_dev, illegal_argument, "Cannot assign IB device");

  ctx = ibv_open_device(ib_dev);
  THROW_IF(not ctx, illegal_argument, "ibv_open_device() failed");

  pd = ibv_alloc_pd(ctx);
  THROW_IF(not pd, illegal_argument, "ibv_alloc_pd() failed");

  buf_mr = ibv_reg_mr(pd, buf, buf_size,
    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
  THROW_IF(not buf_mr, illegal_argument, "ibv_reg_mr() failed");

  msg_mr = ibv_reg_mr(pd, msg, MAX_MSG_SIZE,
    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
  THROW_IF(not msg_mr, illegal_argument, "ibv_reg_mr() failed");

  ch = ibv_create_comp_channel(ctx);
  THROW_IF(not ch, illegal_argument, "ibv_create_comp_channel() failed");

  rcq = ibv_create_cq(ctx, 1, NULL, NULL, 0);
  THROW_IF(not rcq, illegal_argument, "Could not create receive completion queue, ibv_create_cq");

  scq = ibv_create_cq(ctx, tx_depth, (void *)this, ch, 0);
  THROW_IF(not scq, illegal_argument, "Could not create send completion queue, ibv_create_cq");

  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.send_cq = scq;
  qp_init_attr.recv_cq = rcq;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.cap.max_send_wr = tx_depth;
  qp_init_attr.cap.max_recv_wr = 1;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;
  qp_init_attr.cap.max_inline_data = 0;

  qp = ibv_create_qp(pd, &qp_init_attr);
  THROW_IF(not qp, illegal_argument, "Could not create queue pair, ibv_create_qp");

  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = ib_port;
  attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

  int ret = ibv_modify_qp(qp, &attr,
    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
  THROW_IF(ret, illegal_argument, "Could not modify QP to INIT, ibv_modify_qp");

  if (is_server()) {
    tcp::server_context stcp(port, 1);  // FIXME(tzwang): 1 client for now
    exchange_ib_connection_info(stcp.expect_client());
  } else {
    tcp::client_context ctcp(server, port);
    exchange_ib_connection_info(ctcp.server_sockfd);
  }

  qp_change_state_rts();
}

context::~context() {
  ibv_destroy_qp(qp);
  ibv_destroy_cq(scq);
  ibv_destroy_cq(rcq);
  ibv_destroy_comp_channel(ch);
  ibv_dereg_mr(buf_mr);
  ibv_dereg_mr(msg_mr);
  ibv_dealloc_pd(pd);
}

void context::qp_change_state_rts() {
  // first the qp state has to be changed to rtr
  qp_change_state_rtr();

  // Must re-initialize rts_attr each time we use it
  memset(&rts_attr, 0, sizeof(rts_attr));
  rts_attr.qp_state = IBV_QPS_RTS;
  rts_attr.timeout = 14;
  rts_attr.retry_cnt = 7;
  rts_attr.rnr_retry = 7;  // infinite retry
  rts_attr.sq_psn = local_connection->psn;
  rts_attr.max_rd_atomic = 1;
  int ret = ibv_modify_qp(qp, &rts_attr, QP_RTS_ATTR);
  THROW_IF(ret, illegal_argument, "Could not modify QP to RTS state");
}

void context::qp_change_state_rtr() {
  memset(&rtr_attr, 0, sizeof(rtr_attr));
  rtr_attr.qp_state = IBV_QPS_RTR;
  rtr_attr.path_mtu = IBV_MTU_2048;
  rtr_attr.dest_qp_num = remote_connection->qpn;
  rtr_attr.rq_psn = remote_connection->psn;
  rtr_attr.max_dest_rd_atomic = 1;
  rtr_attr.min_rnr_timer = 12;
  rtr_attr.ah_attr.is_global = 0;
  rtr_attr.ah_attr.dlid = remote_connection->lid;
  rtr_attr.ah_attr.sl = 1;
  rtr_attr.ah_attr.src_path_bits = 0;
  rtr_attr.ah_attr.port_num = ib_port;
  int ret = ibv_modify_qp(qp, &rtr_attr, QP_RTR_ATTR);
  THROW_IF(ret, illegal_argument, "Could not modify QP to RTR state");
}

void context::exchange_ib_connection_info(int peer_sockfd) {
  char msg[sizeof("0000:000000:000000:00000000:00000000:0000000000000000:0000000000000000")];
  local_connection = new ib_connection(this);

  sprintf(msg, "%04x:%06x:%06x:%08x:%08x:%016Lx:%016Lx",
    local_connection->lid, local_connection->qpn, local_connection->psn,
    local_connection->buf_rkey, local_connection->msg_rkey,
    local_connection->buf_vaddr, local_connection->msg_vaddr);

  int ret = write(peer_sockfd, msg, sizeof(msg));
  THROW_IF(ret != sizeof(msg), os_error, ret, "Could not send connection_details to peer");
  ret = read(peer_sockfd, msg, sizeof(msg));
  THROW_IF(ret != sizeof(msg), os_error, ret, "Could not receive connection_details to peer");
  remote_connection = new ib_connection(msg);
}

void context::rdma_write(uint64_t offset, uint64_t size) {
  sge_list.addr = (uintptr_t)buf + offset;
  sge_list.length = size;
  sge_list.lkey = buf_mr->lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr.rdma.remote_addr = remote_connection->buf_vaddr;
  wr.wr.rdma.rkey = remote_connection->buf_rkey;
  wr.wr_id = RDMA_WRID;
  wr.sg_list = &sge_list;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL;

  struct ibv_send_wr *bad_wr = nullptr;
  int ret = ibv_post_send(qp, &wr, &bad_wr);
  THROW_IF(ret, illegal_argument, "ibv_post_send() failed");
}

void context::rdma_write_msg() {
  sge_list.addr = (uintptr_t)msg;
  sge_list.length = MAX_MSG_SIZE;
  sge_list.lkey = msg_mr->lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr.rdma.remote_addr = remote_connection->msg_vaddr;
  wr.wr.rdma.rkey = remote_connection->msg_rkey;
  wr.wr_id = RDMA_WRID;
  wr.sg_list = &sge_list;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL;

  struct ibv_send_wr *bad_wr = nullptr;
  int ret = ibv_post_send(qp, &wr, &bad_wr);
  THROW_IF(ret, illegal_argument, "ibv_post_send() failed");
}

ib_connection::ib_connection(char *msg) {
  int parsed = sscanf(msg, "%x:%x:%x:%x:%x:%Lx:%Lx",
    &lid, &qpn, &psn, &buf_rkey, &msg_rkey, &buf_vaddr, &msg_vaddr);
  THROW_IF(parsed != 7, illegal_argument, "Could not parse message from peer");
}

ib_connection::ib_connection(struct context *ctx) {
  // Set up local IB connection attributes that will be exchanged via TCP
  struct ibv_port_attr attr;
  int ret = ibv_query_port(ctx->ctx, ctx->ib_port, &attr);
  THROW_IF(ret, illegal_argument, "Could not get port attributes, ibv_query_port");
  lid = attr.lid;
  qpn = ctx->qp->qp_num;
  psn = lrand48() & 0xffffff;
  buf_rkey = ctx->buf_mr->rkey;
  msg_rkey = ctx->msg_mr->rkey;
  buf_vaddr = (uintptr_t)ctx->buf;
  msg_vaddr = (uintptr_t)ctx->msg;
}
}  // namespace rdma
