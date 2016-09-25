#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
rdma::context *primary_rdma_ctx = nullptr;

const static int kMessageSize = 8;
char msgbuf[kMessageSize] CACHE_ALIGNED;
uint64_t scratch_buf[sysconf::MAX_THREADS];

uint32_t msgbuf_index = -1;
uint32_t logbuf_index = -1;
uint64_t scratch_buf_index[sysconf::MAX_THREADS];  // for RDMA primary only

void primary_init_rdma() {
  auto& logbuf = logmgr->get_logbuf();
  primary_rdma_ctx = new rdma::context(sysconf::primary_port, 1);
  // Must register in the exact sequence below, matching the backup node's sequence
  logbuf_index = primary_rdma_ctx->register_memory(logbuf._data, logbuf.window_size() * 2);
  msgbuf_index = primary_rdma_ctx->register_memory(msgbuf, kMessageSize);
  for (uint32_t i = 0; i < sysconf::worker_threads; ++i) {
    scratch_buf_index[i] = primary_rdma_ctx->register_memory((char*)&scratch_buf[i], sizeof(uint64_t));
  }
  primary_rdma_ctx->finish_init();
  std::cout << "[Primary] RDMA initialized\n";

  // Wait for the standby to send over <FID, memory region index> mappings
  uint32_t msize = (sizeof(FID) + sizeof(int32_t)) * sm_file_mgr::fid_map.size();
  ALWAYS_ASSERT(msize);
  char* mapping = (char*)malloc(msize);
  memset(mapping, 0, msize);

  tcp::server_context stcp(sysconf::primary_port, 1);  // FIXME(tzwang): 1 client for now
  tcp::receive(stcp.expect_client(), mapping, msize);  // 1 client for now
  //tcp::send_ack(backup_sockfds[0]);
  std::cout << "[Primary] Received <FID, memory region index> mappings from standby" << std::endl;
  while (msize) {
    msize -= (sizeof(FID) + sizeof(int32_t));
    FID f = *(FID*)(mapping + msize);
    int32_t index = *(int32_t*)(mapping + msize + sizeof(FID));
    sm_file_mgr::fid_map[f]->pdest_array_mr_index = index;
    // std::cout << "[Primary] FID " << f << " pdest_array_mr_index=" << index << std::endl;
  }
  volatile_write(sysconf::loading, false);
}

void backup_daemon_rdma(tcp::client_context* tcp_ctx) {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());

  // Wait for the main thread to create logmgr - it might run slower than me
  while (not volatile_read(logmgr)) {}
  auto& logbuf = logmgr->get_logbuf();
  rdma::context *cctx = new rdma::context(sysconf::primary_srv, sysconf::primary_port, 1);
  logbuf_index = cctx->register_memory(logbuf._data, logbuf.window_size() * 2);
  msgbuf_index = cctx->register_memory(msgbuf, kMessageSize);
  // After created logmgr, we should have finished recovery as well,
  // i.e., all tables/FIDs are created now. Register the pdest_arrays for RDMA.
  // Also need to send the primary an <FID, memory region index> mapping for each table.
  uint32_t msize = (sizeof(FID) + sizeof(int32_t)) * sm_file_mgr::fid_map.size();
  char* mapping = (char*)malloc(msize);
  memset(mapping, 0, msize);
  uint32_t i = 0;
  for (auto& f : sm_file_mgr::fid_map) {
    auto* pa = f.second->pdest_array;
    // TODO(tzwang): support resizing
    ALWAYS_ASSERT(pa);
    ALWAYS_ASSERT(f.second->fid);
    uint64_t size = pa->_backing_store.size();
    ALWAYS_ASSERT(size);
    f.second->pdest_array_mr_index = cctx->register_memory(pa->_backing_store.data(), size);
    ALWAYS_ASSERT(f.second->pdest_array_mr_index >= 0);
    ALWAYS_ASSERT(f.second->fid == f.first);
    // Copy FID and mr_index
    memcpy(mapping + i * (sizeof(FID) + sizeof(int32_t)), (char*)&f.second->fid, sizeof(FID));
    memcpy(mapping + i * (sizeof(FID) + sizeof(int32_t)) + sizeof(FID),
           (char*)&f.second->pdest_array_mr_index, sizeof(int32_t));
    std::cout << "[Backup] Registered FID " << f.first << "(" << size << ") " << f.second->pdest_array_mr_index << std::endl;
    i++;
  }
  cctx->finish_init();
  std::cout << "[Backup] RDMA initialized" << std::endl;

  // Send over the mapping
  tcp::client_context ctcp(sysconf::primary_srv, sysconf::primary_port);
  int32_t ret = send(ctcp.server_sockfd, mapping, msize, 0);
  THROW_IF(ret != msize, os_error, ret, "Could not send <FID, memory region index> mappings to peer");
  //tcp::expect_ack(tcp_ctx->server_sockfd);
  delete tcp_ctx;
  std::cout << "[Backup] Sent <FID, memory region index> mappings" << std::endl;

  DEFER(delete cctx);

  // Now safe to start the redo daemon with a valid durable_flushed_lsn
  if (not sysconf::log_ship_sync_redo) {
    std::thread rt(redo_daemon);
    rt.detach();
  }

  std::cout << "[Backup] Start to wait for logs from primary" << std::endl;
  // Listen to incoming log records from the primary
  uint32_t size = 0;
  while (1) {
    // tell the peer i'm ready
    *(uint64_t *)cctx->get_memory_region(msgbuf_index) = RDMA_READY_TO_RECEIVE;
    cctx->rdma_write(msgbuf_index, 0, msgbuf_index, 0, kMessageSize);

    // post an RR to get the data and its size embedded as an immediate
    size = cctx->receive_rdma_with_imm();
    THROW_IF(not size, illegal_argument, "Invalid data size");

    // now we should already have data sitting in the buffer, but we need
    // to use the data size we got to calculate a new durable lsn first.
    LSN start_lsn =  logmgr->durable_flushed_lsn();
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    segment_id *sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);
    ALWAYS_ASSERT(sid);
    LSN end_lsn = sid->make_lsn(end_lsn_offset);
    ASSERT(end_lsn_offset == end_lsn.offset());

    std::cout << "[Backup] Recieved " << size << " bytes ("
      << std::hex << start_lsn.offset() << "-" << end_lsn.offset() << std::dec << ")\n";

    // now we have the new durable lsn (end_lsn), persist the data we got
    if (sysconf::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf.advance_writer(sid->buf_offset(end_lsn));
    } else {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
      ASSERT(logmgr->durable_flushed_lsn() == end_lsn);
    }

    // roll forward
    if (sysconf::log_ship_sync_redo) {
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }
    if (sysconf::nvram_log_buffer) {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
    }
  }
}

// Support only one peer for now
void primary_ship_log_buffer_rdma(const char *buf, uint32_t size) {
  ALWAYS_ASSERT(size);
  // wait for the "go" signal
  while (volatile_read(*(uint64_t *)primary_rdma_ctx->get_memory_region(msgbuf_index)) != RDMA_READY_TO_RECEIVE) {}
  // reset it so I'm not confused next time
  *(uint64_t *)primary_rdma_ctx->get_memory_region(msgbuf_index) = RDMA_WAITING;
  uint64_t offset = buf - primary_rdma_ctx->get_memory_region(logbuf_index);
  primary_rdma_ctx->rdma_write(logbuf_index, offset, logbuf_index, offset, size, size);
}

// Propogate OID array changes for the given write set entry to
// backups; for RDMA-based log shipping only
void update_pdest_on_backup_rdma(write_record_t* w) {
    ASSERT(sysconf::log_ship_by_rdma);
    sm_file_descriptor* file_desc = sm_file_mgr::fid_map[w->fid];
    int32_t remote_index = file_desc->pdest_array_mr_index;
    ALWAYS_ASSERT(remote_index > 0);
    uint64_t offset = (uint64_t)w->entry - (uint64_t)file_desc->main_array->_backing_store.data();
    object* obj = (object*)w->entry->offset();
    object* next_obj = (object*)obj->_next.offset();
    uint64_t expected = 0;
    if (next_obj) {
        expected = next_obj->_pdest._ptr;
    }
    fat_ptr ret = NULL_PTR;
    while (true) {
        // FIXME(tzwang): handle cases where the remote pdest array is too small
        ret._ptr = primary_rdma_ctx->rdma_compare_and_swap(
          scratch_buf_index[thread::my_id() % sysconf::worker_threads], 0,
          remote_index, offset, expected, obj->_pdest._ptr);
        if (ret._ptr == expected || ret.offset() > w->entry->offset()) {
            break;
        }
        ASSERT(((fat_ptr)ret).offset() != w->entry->offset());
        expected = ret._ptr;
    }
}

}  // namespace rep
