#include "sm-file.h"
#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {

// Send the log buffer to backups
void primary_ship_log_buffer_tcp(
  int backup_sockfd, const char* buf, uint32_t size) {
  ALWAYS_ASSERT(size);
  size_t nbytes = send(backup_sockfd, (char *)&size, sizeof(uint32_t), 0);
  THROW_IF(nbytes != sizeof(uint32_t), log_file_error, "Incomplete log shipping (header)");
  //printf("[Primary] Will send %u bytes\n", size);
  nbytes = send(backup_sockfd, buf, size, 0);
  THROW_IF(nbytes != size, log_file_error, "Incomplete log shipping (data)");
  //printf("[Primary] Sent %u bytes\n", size);
  // expect an ack message
  // XXX(tzwang): do this in flush()?
  tcp::expect_ack(backup_sockfd);
}

void backup_daemon_tcp(tcp::client_context *cctx) {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());
  DEFER(delete cctx);

  // Listen to incoming log records from the primary
  uint32_t size = 0;
  // Wait for the main thread to create logmgr - it might run slower than me
  while (not volatile_read(logmgr)) {}
  auto& logbuf = logmgr->get_logbuf();

  // Now safe to start the redo daemon with a valid durable_flushed_lsn
  if (not config::log_ship_sync_redo) {
    std::thread rt(redo_daemon);
    rt.detach();
  }

  while (1) {
    // expect an integer indicating data size
    tcp::receive(cctx->server_sockfd, (char *)&size, sizeof(size));
    ALWAYS_ASSERT(size);

    // prepare segment if needed
    LSN start_lsn =  logmgr->durable_flushed_lsn();
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    segment_id *sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);
    ALWAYS_ASSERT(sid);
    LSN end_lsn = sid->make_lsn(end_lsn_offset);
    ASSERT(end_lsn_offset == end_lsn.offset());

    // expect the real log data
    //std::cout << "[Backup] Will receive " << size << " bytes\n";
    char *buf = logbuf.write_buf(sid->buf_offset(start_lsn), size);
    ALWAYS_ASSERT(buf);   // XXX: consider different log buffer sizes than the primary's later
    tcp::receive(cctx->server_sockfd, buf, size);
    //std::cout << "[Backup] Recieved " << size << " bytes ("
    //  << std::hex << start_lsn.offset() << "-" << end_lsn.offset() << std::dec << ")\n";

    // now got the batch of log records, persist them
    if (config::nvram_log_buffer) {
      logmgr->persist_log_buffer();
      logbuf.advance_writer(sid->buf_offset(end_lsn));
    } else {
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
      ASSERT(logmgr->durable_flushed_lsn() == end_lsn);
    }

    tcp::send_ack(cctx->server_sockfd);

    if (config::log_ship_sync_redo) {
      ALWAYS_ASSERT(end_lsn == logmgr->durable_flushed_lsn());
      printf("[Backup] Rolling forward %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }
    if (config::nvram_log_buffer)
      logmgr->flush_log_buffer(logbuf, end_lsn_offset, true);
  }
}

}  // namespace rep

