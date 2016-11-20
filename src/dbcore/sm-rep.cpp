#include "sm-rep.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
// for primary server only
std::vector<int> backup_sockfds;
std::mutex backup_sockfds_mutex;

void start_as_primary() {
  ALWAYS_ASSERT(not config::is_backup_srv());
  if(config::log_ship_by_rdma) {
    std::thread t(primary_daemon_rdma);
    t.detach();
  } else {
    std::thread t(primary_daemon_tcp);
    t.detach();
  }
}

void primary_ship_log_buffer_all(const char *buf, uint32_t size) {
  backup_sockfds_mutex.lock();
  if (config::log_ship_by_rdma) {
    primary_ship_log_buffer_rdma(buf, size);
  } else {
    ASSERT(backup_sockfds.size());
    for (int &fd : backup_sockfds) {
      primary_ship_log_buffer_tcp(fd, buf, size);
    }
  }
  backup_sockfds_mutex.unlock();
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

// Generate a metadata structure for sending to the new backup.
// No CC whatsoever, single-threaded execution only.
backup_start_metadata* prepare_start_metadata(int& chkpt_fd, LSN& chkpt_start_lsn) {
  chkpt_fd = -1;
  uint64_t nlogfiles = 0;
  dirent_iterator dir(config::log_dir.c_str());
  for(char const *fname : dir) {
    if(fname[0] == 'l') {
      ++nlogfiles;
    }
  }
  static backup_start_metadata* md = nullptr;
  if(!md || md->num_log_files < nlogfiles) {
    if(md) {
      free(md);
    }
    md = allocate_backup_start_metadata(nlogfiles);
  }
  new (md) backup_start_metadata;
  chkpt_start_lsn = INVALID_LSN;
  int dfd = dir.dup();
  // Find chkpt first
  for(char const *fname : dir) {
    char l = fname[0];
    if(l == 'c') {
      memcpy(md->chkpt_marker, fname, CHKPT_FILE_NAME_BUFSZ);
    } else if(l == 'o') {
      // chkpt file
      ALWAYS_ASSERT(config::enable_chkpt);
      struct stat st;
      chkpt_fd = os_openat(dfd, fname, O_RDONLY);
      int ret = fstat(chkpt_fd, &st);
      THROW_IF(ret != 0, log_file_error, "Error fstat");
      ASSERT(st.st_size);
      md->chkpt_size = st.st_size;
      char canary_unused;
      int n = sscanf(fname, CHKPT_DATA_FILE_NAME_FMT "%c",
                     &chkpt_start_lsn._val, &canary_unused);
      ALWAYS_ASSERT(chkpt_start_lsn != INVALID_LSN);
    }
  }
  LOG(INFO) << "[Primary] Will ship checkpoint taken at 0x"
    << std::hex << chkpt_start_lsn.offset() << std::dec;
  dfd = dir.dup();
  for (char const *fname : dir) {
    // Must send dur-xxxx, chk-xxxx, nxt-xxxx anyway
    char l = fname[0];
    if(l == 'd') {
      // durable lsn marker
      memcpy(md->durable_marker, fname, DURABLE_FILE_NAME_BUFSZ);
    } else if(l == 'n') {
      // nxt segment
      memcpy(md->nxt_marker, fname, NXT_SEG_FILE_NAME_BUFSZ);
    } else if(l == 'l') {
      uint64_t start = 0, end = 0;
      unsigned int seg;
      char canary_unused;
      int n = sscanf(fname, SEGMENT_FILE_NAME_FMT "%c", &seg, &start, &end, &canary_unused);
      struct stat st;
      int log_fd = os_openat(dfd, fname, O_RDONLY);
      int ret = fstat(log_fd, &st);
      os_close(log_fd);
      ASSERT(st.st_size);
      uint64_t size = st.st_size - chkpt_start_lsn.offset();
      md->add_log_segment(seg, start, end, size);
      LOG(INFO) << "Will ship segment " << seg << ", " << size << " bytes";
    } else if(l == 'c' || l == 'o' || l == '.') {
      // Nothing to do or already handled
    } else {
      LOG(FATAL) << "Unrecognized file name";
    }
  }
  return md;
}

}  // namespace rep

