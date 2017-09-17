#pragma once

#include <deque>
#include "sm-log-recover.h"

/* The log block allocator.

   Once an LSN has been generated, we need to allocate buffer space
   for the block's data and prepare the log header and skip entry. The
   user is responsible to fill the remaining log records in the block.

   The log buffer is relatively small, so while we're at it we'll
   implement log writing.

   We can't reasonably test any of this without some ability to read
   the log back from disk, so implement that as well.

   NOTE: Don't inherit from sm_log_recover_mgr: we want a clean break
   between this log manager and the pieces it's built out of.
 */
struct sm_log_alloc_mgr {
  sm_log_alloc_mgr(sm_log_recover_impl *rf, void *rfn_arg);

  ~sm_log_alloc_mgr();

  void set_tls_lsn_offset(uint64_t offset);
  uint64_t get_tls_lsn_offset();

  /* Kick the log writer daemon and wait for it to finish flushing
   * the log buffer
   */
  LSN flush();

  /* Retrieve the current end of log
   */
  uint64_t cur_lsn_offset();

  /* Retrieve the current durable end of log
   */
  uint64_t dur_flushed_lsn_offset();

  /* Block the caller until the specified LSN offset has become durable
   */
  void wait_for_durable(uint64_t dlsn_offset);

  /* Block the caller until all log entries before [dlsn_offset]
     have been made durable, and the durable mark updated.
   */
  void update_wait_durable_mark(uint64_t dlsn_offset);

  /* Allocate a log block.
   */
  log_allocation *allocate(uint32_t nrec, size_t payload_bytes);

  /* Release a fully populated allocation. Its contents will be
     written to disk in the background.

     The caller is responsible to assign a valid checksum.

     WARNING: using the passed-in pointer (or the corresponding
     buffer space) after this call produces undefined behavior.
   */
  void release(log_allocation *x);

  /* Discard an allocation. Its contents will be forgotten.

     WARNING: using the passed-in pointer (or the corresponding
     buffer space) after this call produces undefined behavior.
   */
  void discard(log_allocation *x);

  void _log_write_daemon();
  void _kick_log_write_daemon();
  segment_id *PrimaryFlushLog(uint64_t new_dlsn_dlsn,
                              bool update_dmark = false);
  void PrimaryShipLog(segment_id *durable_sid, uint64_t nbytes,
                      bool new_seg, uint64_t new_offset, const char *buf);
  void PrimaryCommitPersistedWork(uint64_t new_offset);
  void BackupFlushLog(uint64_t new_dlsn_dlsn);
  uint64_t smallest_tls_lsn_offset();
  void enqueue_committed_xct(uint32_t worker_id, uint64_t start_time);
  void dequeue_committed_xcts(uint64_t up_to, uint64_t end_time);

  sm_log_recover_mgr _lm;
  window_buffer *_logbuf;
  uint64_t _durable_flushed_lsn_offset;

  pthread_t _write_daemon_tid;
  os_mutex _write_daemon_mutex;
  os_condvar _write_daemon_cond;
  os_condvar _write_complete_cond;
  os_condvar _dmark_updated_cond;

  int _write_daemon_state;

  bool _waiting_for_durable;
  bool _waiting_for_dmark;

  bool _write_daemon_should_wake;
  bool _write_daemon_should_stop;

  // tzwang: use one _tls_lsn_offset per worker thread to record its
  // most-recently committed/aborted transaction's log lsn offset. This array
  // together is used to replace the rcu-slist as a result of the decoupling of
  // getting an LSN and the accounting of log allocation objects (in an
  // rcu-slist). Using the rcu-slist we have to use **at least** one CAS per
  // transaction and there's no bounded number of steps for a thread to
  // successfully get an LSN offset. Tests on a 32-core (4 sockets) and 60-core
  // (4 sockets), and a 240-core (16 sockets) machines show this doesn't scale
  // after around 30 threads.
  //
  // So now we use a single atomic fetch_and_add to get the LSN offset. Each
  // thread is guaranteed to get a valid LSN offset using exactly **one**
  // instruction.
  //
  // Still, the log flusher needs to know the "oldest live log allocation" (in
  // the rcu-slist term), which is the earliest LSN offset/transaction that has
  // written to the log buffer. Note the discrepancy here: in both the
  // rcu-slist and atomic fetch-and-add schemes, we might have holes in the log
  // buffer. So we need to find the earliest guy who has finished writing to
  // the log buffer.
  //
  // Without a centralized rcu-slist using CAS, we have to use a fully
  // decentralized approach - schemes like one rcu-slist per socket won't
  // completely solve the problem because there is no coordination between
  // threads on the same socket. That will direct us back to use CAS again
  // which is what we wanted to avoid.
  //
  // The solution is borrowed from shore-mt's dlog's passive group commit,
  // which solves a similar problem in NVM+dlog settings. Each thread will put
  // its just committed/aborted lsn offset (corresponds to the log allocation
  // block in the rcu-slist scheme) to its own "tls" place, and the log flusher
  // just scans all these tls places, then flush up to the **smallest** lsn it
  // found.
  //
  // In case the log buffer is backed by NVRAM (ie --nvram-log-buffer == 1),
  // the thread setting its tls_lsn_offset will clflush() then continue.  So
  // smallest_tls_lsn_offset() returns the "durable lsn" (possibly still in the
  // log buffer). The daemon is only responsible for flushing, ie making room
  // in the log buffer to take more transactions.
  static const uint64_t kDirtyTlsLsnOffset = uint64_t{1} << 63;
  uint64_t *_tls_lsn_offset CACHE_ALIGNED;
  uint64_t _lsn_offset CACHE_ALIGNED;
  uint64_t _logbuf_partition_size CACHE_ALIGNED;

  // One queue per worker thread to account latency under group commit
  // The flusher dequeues all entries from these vectors up to
  // flushed_durable_lsn
  struct commit_queue {
    // Each entry is a std::pair<lsn_offset, start_time>
    struct Entry {
      uint64_t lsn;
      uint64_t start_time;
      Entry() : lsn(0), start_time(0) {}
    };
    Entry *queue;
    mcs_lock lock;
    uint32_t start;
    uint32_t items;
    sm_log_alloc_mgr *lm;
    commit_queue() : start(0), items(0), lm(nullptr) {
      queue = new Entry[config::group_commit_queue_length];
    }
    ~commit_queue() { delete[] queue; }
    void push_back(uint64_t lsn, uint64_t start_time);
    inline uint32_t size() { return items; }
  };
  commit_queue *_commit_queue CACHE_ALIGNED;
};
