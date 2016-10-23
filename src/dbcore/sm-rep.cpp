#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
// for primary server only
tcp::server_context *primary_tcp_ctx = nullptr;
std::vector<int> backup_sockfds;

void start_as_primary() {
  ALWAYS_ASSERT(not config::is_backup_srv());
  if(!config::log_ship_by_rdma) {
    std::thread t(primary_daemon_tcp);
    t.detach();
  } else {
    // TODO
  }
}

void primary_ship_log_buffer_all(const char *buf, uint32_t size) {
  if (config::log_ship_by_rdma) {
    primary_ship_log_buffer_rdma(buf, size);
  } else {
    ASSERT(backup_sockfds.size());
    for (int &fd : backup_sockfds) {
      primary_ship_log_buffer_tcp(fd, buf, size);
    }
  }
}

void redo_daemon() {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());
  DEFER(rcu_deregister());

  auto start_lsn = logmgr->durable_flushed_lsn();
  while (true) {
    auto end_lsn = logmgr->durable_flushed_lsn();
    if (end_lsn > start_lsn) {
      printf("[Backup] Rolling forward %lx-%lx\n", start_lsn.offset(), end_lsn.offset());
      logmgr->redo_log(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn.offset());
      start_lsn = end_lsn;
    }
  }
}

}  // namespace rep

