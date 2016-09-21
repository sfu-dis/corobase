#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
rdma::context *primary_rdma_ctx = nullptr;

const static int kMessageSize = 8;
char msgbuf[kMessageSize] CACHE_ALIGNED;
uint64_t scratch_buf CACHE_ALIGNED;

uint32_t msgbuf_index = -1;
uint32_t logbuf_index = -1;
uint64_t scratch_buf_index = -1;  // for RDMA primary only

void init_rdma() {
  auto& logbuf = logmgr->get_logbuf();
  primary_rdma_ctx = new rdma::context(sysconf::primary_port, 1);
  // Must register in the exact sequence below, matching the backup node's sequence
  logbuf_index = primary_rdma_ctx->register_memory(logbuf._data, logbuf.window_size() * 2);
  msgbuf_index = primary_rdma_ctx->register_memory(msgbuf, kMessageSize);
  scratch_buf_index = primary_rdma_ctx->register_memory((char*)&scratch_buf, sizeof(uint64_t));
  primary_rdma_ctx->finish_init();
}

void backup_daemon_rdma() {
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
  // i.e., all tables/FIDs are created now. Register them for RDMA.
  for (auto& f : sm_file_mgr::fid_map) {
    auto* oa = f.second->index->get_oid_array();
    // TODO(tzwang): support resizing
    ALWAYS_ASSERT(oa);
    uint64_t size = oa->_backing_store.size();
    ALWAYS_ASSERT(size);
    //f.second->rdma_mr_index = cctx->register_memory(oa->_backing_store.data(), size);
    //ALWAYS_ASSERT(f.second->rdma_mr_index >= 0);
    std::cout << "[RDMA] Registered FID " << f.first << "(" << size << ")" << std::endl;
  }
  cctx->finish_init();

  DEFER(delete cctx);

  // Now safe to start the redo daemon with a valid durable_flushed_lsn
  if (not sysconf::log_ship_sync_redo) {
    std::thread rt(redo_daemon);
    rt.detach();
  }

  // Listen to incoming log records from the primary
  uint32_t size = 0;
  while (1) {
    // tell the peer i'm ready
    *(uint64_t *)cctx->get_memory_region(msgbuf_index) = RDMA_READY_TO_RECEIVE;
    cctx->rdma_write(msgbuf_index, 0, 0, kMessageSize);

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
  primary_rdma_ctx->rdma_write(logbuf_index, offset, offset, size, size);
}

// Propogate OID array changes for the given write set entry to
// backups; for RDMA-based log shipping only
void update_oid_on_backup_rdma(write_record_t* w) {
    ASSERT(sysconf::log_ship_by_rdma);
    uint64_t expected = w->get_object()->_next.offset();
    //primary_rdma_ctx->rdma_compare_and_swap(
    //  scratch_buf_index, 0, w->oa->rdma_mr_index, (uint64_t)w - (uint64_t)&oa->entries_[0],
    //  expected, (unit64_t)w);
}
}  // namespace rep
