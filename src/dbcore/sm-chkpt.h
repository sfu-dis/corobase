#pragma once
#include <sys/mman.h>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <mutex>
#include "sm-common.h"
#include "sm-log-impl.h"
#include "sm-oid.h"

#define CHKPT_DATA_FILE_NAME_FMT "oac-%016zx"
#define CHKPT_DATA_FILE_NAME_BUFSZ sizeof("chd-0123456789abcdef")

namespace ermia {

class sm_chkpt_mgr {
 public:
  sm_chkpt_mgr(LSN chkpt_begin)
      : _shutdown(false),
        _buf_pos(0),
        _dur_pos(0),
        _fd(-1),
        _last_cstart(chkpt_begin),
        _base_chkpt_lsn(chkpt_begin) {}

  ~sm_chkpt_mgr() {
    volatile_write(_shutdown, true);
    take();
    _daemon->join();
  }

  inline void sync_buffer() {
    if (_buf_pos > _dur_pos) {
      os_write(_fd, _buffer + _dur_pos, _buf_pos - _dur_pos);
      _dur_pos = _buf_pos;
    }
    os_fsync(_fd);
  }

  inline void start_chkpt_thread() {
    ASSERT(logmgr and oidmgr);
    _daemon = new std::thread(&sm_chkpt_mgr::daemon, this);
  }

  void take(bool wait = false);
  void do_chkpt();
  void daemon();
  void write_buffer(void* p, size_t s);
  static void recover(LSN chkpt_start, sm_log_recover_mgr* lm);

  inline char* advance_buffer(uint32_t size) {
    if (_buf_pos + size > kBufferSize) {
      sync_buffer();
      _buf_pos = _dur_pos = 0;
    }
    char* ret = _buffer + _buf_pos;
    _buf_pos += size;
    return ret;
  }

  static int base_chkpt_fd;
  static uint32_t num_recovery_threads;

 private:
  static const size_t kBufferSize = 512 * 1024 * 1024;

  bool _shutdown;
  std::thread* _daemon;
  std::mutex _daemon_mutex;
  std::condition_variable _daemon_cv;
  size_t _buf_pos;
  size_t _dur_pos;
  char _buffer[kBufferSize];
  int _fd;
  LSN _last_cstart;
  LSN _base_chkpt_lsn;
  std::condition_variable _wait_chkpt_cv;
  std::mutex _wait_chkpt_mutex;
  bool _in_progress;
  uint32_t _num_recovery_threads;

  void prepare_file(LSN cstart);
  void scavenge();
  static void do_recovery(char* chkpt_name, OID oid_partition,
                          uint64_t start_offset);
};

extern sm_chkpt_mgr* chkptmgr;
}  // namespace ermia
