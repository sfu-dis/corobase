#include "sm-cmd-log.h"
#include "sm-log-alloc.h"
#include "sm-rep.h"
#include "stopwatch.h"
#include "../benchmarks/bench.h"
#include "../macros.h"
#include "../util.h"

namespace {

uint64_t get_starting_byte_offset(sm_log_recover_mgr *lm) {
  auto dlsn = lm->get_durable_mark();
  auto *sid = lm->get_segment(dlsn.segment());
  return sid->offset(dlsn);
}

extern "C" void *log_write_daemon_thunk(void *arg) {
  ((sm_log_alloc_mgr *)arg)->_log_write_daemon();
  return NULL;
}

enum { DAEMON_HAS_WORK = 0x1, DAEMON_SLEEPING = 0x2 };

}  // end anonymous namespace

void sm_log_alloc_mgr::set_tls_lsn_offset(uint64_t offset) {
  volatile_write(_tls_lsn_offset[thread::my_id()], offset);
}

uint64_t sm_log_alloc_mgr::get_tls_lsn_offset() {
  return volatile_read(_tls_lsn_offset[thread::my_id()]);
}

/* We have to find the end of the log files on disk before
   constructing the log buffer in memory. It's also a convenient time
   to do the rest of recovery, because it prevents any attempt at
   forward processing before recovery completes.
 */
sm_log_alloc_mgr::sm_log_alloc_mgr(sm_log_recover_impl *rf, void *rfn_arg)
    : _lm(config::null_log_device ? NULL : rf, rfn_arg),
      _durable_flushed_lsn_offset(_lm.get_durable_mark().offset()),
      _write_daemon_state(0),
      _waiting_for_durable(false),
      _waiting_for_dmark(false),
      _write_daemon_should_wake(false),
      _write_daemon_should_stop(false),
      _lsn_offset(_lm.get_durable_mark().offset()) {
  _logbuf_partition_size =
      config::log_buffer_mb * config::MB / config::log_redo_partitions;
  ALWAYS_ASSERT(
      config::log_buffer_mb * config::MB % config::log_redo_partitions == 0);
  _logbuf = sm_log::get_logbuf();
  _logbuf->_head = _logbuf->_tail = get_starting_byte_offset(&_lm);
  if (!config::is_backup_srv() || (config::command_log && config::replay_threads)) {
    _tls_lsn_offset =
        (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);
    memset(_tls_lsn_offset, 0, sizeof(uint64_t) * config::MAX_THREADS);

    uint32_t n = config::is_backup_srv() ? config::replay_threads : config::worker_threads;
    _commit_queue = new commit_queue[n];
    for (uint32_t i = 0; i < n; ++i) {
      _commit_queue[i].lm = this;
    }

    // fire up the log writing daemon
    _write_daemon_mutex.lock();
    DEFER(_write_daemon_mutex.unlock());

    int err =
        pthread_create(&_write_daemon_tid, NULL, &log_write_daemon_thunk, this);
    THROW_IF(err, os_error, err, "Unable to start log writer daemon thread");
  }
}

sm_log_alloc_mgr::~sm_log_alloc_mgr() {
  _write_daemon_should_stop = true;
  int err = pthread_join(_write_daemon_tid, NULL);
  THROW_IF(err, os_error, err, "Unable to join log writer daemon thread");
}

void sm_log_alloc_mgr::enqueue_committed_xct(uint32_t worker_id,
                                             uint64_t start_time) {
  uint64_t lsn = config::command_log ?
                 CommandLog::cmd_log->GetTlsOffset() :
                 get_tls_lsn_offset() & ~kDirtyTlsLsnOffset;
  _commit_queue[worker_id].push_back(lsn, start_time);
}

void sm_log_alloc_mgr::commit_queue::push_back(uint64_t lsn,
                                               uint64_t start_time) {
  bool flush = false;
  bool insert = true;
retry :
  if (flush) {
    if (config::command_log) {
      CommandLog::cmd_log->TryFlush();
    } else {
      lm->flush();
    }
    flush = false;
  }
  if (insert) {
    CRITICAL_SECTION(cs, lock);
    if (items >= config::group_commit_queue_length * 0.8) {
      flush = true;
    }
    if (items < config::group_commit_queue_length) {
      uint32_t idx = (start + items) % config::group_commit_queue_length;
      volatile_write(queue[idx].lsn, lsn);
      volatile_write(queue[idx].start_time, start_time);
      volatile_write(items, items + 1);
      ASSERT(items == size());
      insert = false;
    }
    if (flush) {
      goto retry;
    }
  }
}

void sm_log_alloc_mgr::dequeue_committed_xcts(uint64_t upto,
                                              uint64_t end_time) {
  uint32_t n = config::is_backup_srv() ? config::replay_threads : config::worker_threads;
  for (uint32_t i = 0; i < n; i++) {
    CRITICAL_SECTION(cs, _commit_queue[i].lock);
    uint32_t n = volatile_read(_commit_queue[i].start);
    uint32_t size = _commit_queue[i].size();
    uint32_t dequeue = 0;
    for (uint32_t j = 0; j < size; ++j) {
      uint32_t idx = (n + j) % config::group_commit_queue_length;
      auto &entry = _commit_queue[i].queue[idx];
      if (volatile_read(entry.lsn) > upto) {
        break;
      }
      uint64_t worker_latency =
          volatile_read(bench_runner::workers[i]->latency_numer_us);
      uint64_t latency = end_time - volatile_read(entry.start_time);
      // Must do volatile_write here
      volatile_write(bench_runner::workers[i]->latency_numer_us,
                     worker_latency + latency);
      dequeue++;
    }
    _commit_queue[i].items -= dequeue;
    volatile_write(_commit_queue[i].start, (n + dequeue) % config::group_commit_queue_length);
  }
}

uint64_t sm_log_alloc_mgr::cur_lsn_offset() {
  return volatile_read(_lsn_offset);
}

uint64_t sm_log_alloc_mgr::dur_flushed_lsn_offset() {
  return volatile_read(_durable_flushed_lsn_offset);
}

void sm_log_alloc_mgr::wait_for_durable(uint64_t dlsn_offset) {
  if (dur_flushed_lsn_offset() < dlsn_offset) {
    _write_daemon_mutex.lock();
    DEFER(_write_daemon_mutex.unlock());

    while (dur_flushed_lsn_offset() < dlsn_offset) {
      /* Kickingdaemon here would accomplish nothing. Release of
         new log records ensures that the daemon is (or will
         soon become) awake to process them, so the daemon will
         only stay asleep if there is no work for it to do.
       */
      _waiting_for_durable = true;
      _write_complete_cond.wait(_write_daemon_mutex);
    }
  }
}

void sm_log_alloc_mgr::update_wait_durable_mark(uint64_t lsn_offset) {
  wait_for_durable(lsn_offset);
  _write_daemon_mutex.lock();
  DEFER(_write_daemon_mutex.unlock());
  while (_lm.get_durable_mark().offset() < lsn_offset) {
    _waiting_for_dmark = true;
    _kick_log_write_daemon();
    _dmark_updated_cond.wait(_write_daemon_mutex);
  }
}

/* Flush the log buffer, wait for the daemon to finish, and return
 * a durable lsn.
 */
LSN sm_log_alloc_mgr::flush() {
  _write_daemon_mutex.lock();
  DEFER(_write_daemon_mutex.unlock());
  _waiting_for_dmark = true;
  __sync_fetch_and_or(&_write_daemon_state, DAEMON_HAS_WORK);
  _kick_log_write_daemon();
  _dmark_updated_cond.wait(_write_daemon_mutex);
  return _lm.get_durable_mark();
}

void sm_log_alloc_mgr::BackupFlushLog(uint64_t new_dlsn_offset) {
  ASSERT(config::is_backup_srv());
retry:
  LSN dlsn = _lm.get_durable_mark();
  ASSERT(_durable_flushed_lsn_offset == dlsn.offset());
  ASSERT(_durable_flushed_lsn_offset <= new_dlsn_offset);
  auto *durable_sid = _lm.get_segment(dlsn.segment());
  ALWAYS_ASSERT(durable_sid);
  if (_durable_flushed_lsn_offset >= durable_sid->end_offset) {
    // Crossing a dead zone, update the durable mark by hand
    durable_sid = _lm.get_segment(dlsn.segment() + 1);
    _durable_flushed_lsn_offset = durable_sid->start_offset;
    _lm.update_durable_mark(
        LSN::make(_durable_flushed_lsn_offset, durable_sid->segnum));
    goto retry;
  }
  if (_durable_flushed_lsn_offset >= new_dlsn_offset) {
    return;
  }
  uint64_t durable_byte = durable_sid->buf_offset(_durable_flushed_lsn_offset);
  int active_fd = _lm.open_for_write(durable_sid);
  DEFER(os_close(active_fd));

  /* The block list contains a fluctuating---and usually fairly
     short---set of log_allocation objects. Releasing or
     discarding a block marks it as dead (without removing it)
     and removes all dead blocks that follow it. The list is
     primed at start-up with the durable LSN (as determined by
     startup/recovery), and so is guaranteed to always contain
     at least one (perhaps dead) node that later requests can
     use to acquire a proper LSN.

     Our goal is to find the oldest (= last) live block in the
     list, and write out everything before that block's offset.

     Once we know the offset, we can look up the corresponding
     segment to obtain an LSN.
   */
  LOG_IF(FATAL, new_dlsn_offset < durable_sid->start_offset);
  uint64_t new_byte =
      durable_sid->byte_offset + (new_dlsn_offset - durable_sid->start_offset);

  ASSERT(durable_byte < new_byte);
  ASSERT(new_byte <= sm_log::logbuf->write_end());

  uint64_t nbytes = new_byte - durable_byte;
  ALWAYS_ASSERT(sm_log::logbuf->available_to_read() >= nbytes);
  THROW_IF(sm_log::logbuf->available_to_read() < nbytes, log_file_error,
           "Not enough log bufer to read");

  // perform the write
  auto *buf = sm_log::logbuf->read_buf(durable_byte, nbytes);
  auto file_offset = durable_sid->offset(_durable_flushed_lsn_offset);
  uint64_t n = os_pwrite(active_fd, buf, nbytes, file_offset);
  THROW_IF(n < nbytes, log_file_error, "Incomplete log write");
  _durable_flushed_lsn_offset = new_dlsn_offset;

  // Update cur_lsn_offset so read-only transactions can get fresh data
  if (_lsn_offset < _durable_flushed_lsn_offset) {
    THROW_IF(not config::is_backup_srv(), illegal_argument,
             "Wrong cur_lsn_offset on primary node");
    _lsn_offset = _durable_flushed_lsn_offset;
  }

  // Have to use LSN::make (instead of durable_sid->make_lsn which checks
  // lsn offset ownership): Since we're on a backup server, this new durable
  // lsn offset might end up in the deadzone which isn't contained by any sid,
  // because we ship at log buffer flush boundaries (no info for the next
  // segment
  // until the next shipping).
  _lm.update_durable_mark(
      LSN::make(_durable_flushed_lsn_offset, durable_sid->segnum));
}

void sm_log_alloc_mgr::PrimaryShipLog(segment_id *durable_sid,
                                      uint64_t nbytes, bool new_seg,
                                      uint64_t new_offset, const char *buf) {
  ASSERT(!config::command_log);
  bool have_imm = false;
  uint32_t imm = 0;
  if (config::log_ship_by_rdma) {
    // Ship first, this is async for RDMA
    // Embed the segment's real begin_offset as the immediate: Note that we
    // have 32-bit immmediates only, so we only store the offset off the base
    // segment size boundaries. e.g., for the second segment (segment 2) and
    // with a segment size of 4GB: we'd encode: begin_offset - (2-1) * 4GB.
    // This number won't usually go beyond 32-bit; the backup when received the
    // immediate will extract and recalculate the correct segment begin/end
    // offsets.
    bool seg_first_ship =
        _durable_flushed_lsn_offset == durable_sid->start_offset;
    DLOG(INFO) << "Will ship " << std::hex << _durable_flushed_lsn_offset
               << " " << new_offset << " " << durable_sid->byte_offset
               << " new_seg=" << new_seg
               << " seg_first_ship=" << seg_first_ship << " nbytes=" << nbytes
               << std::dec;
    // Two cases when we cross segment boundaries:
    // 1. We will end in the new segment;
    // 2. We end right before the new segment (ie the end of the old segment).
    // [new_seg] covers the first case; [seg_first_ship] covers the second.
    have_imm = new_seg || seg_first_ship;
    if (have_imm) {
      imm = durable_sid->start_offset -
            (durable_sid->segnum - 1) * config::log_segment_mb * config::MB;
    }
  }
  LOG_IF(FATAL, nbytes > config::group_commit_bytes + MIN_LOG_BLOCK_SIZE)
        << "Trying to ship too much: " << nbytes;
  if (config::persist_policy == config::kPersistPipelined) {
    if (config::log_ship_by_rdma) {
      // Wait for the backup to persist (it might act fast and set
      // ReadyToReceive too)
      rep::primary_rdma_wait_for_message(
          rep::kRdmaPersisted | rep::kRdmaReadyToReceive, false);
    } else {
      // Wait for acks from backup
      for (auto &fd : rep::backup_sockfds) {
        tcp::expect_ack(fd);
      }
    }
    {
      util::timer t;
      dequeue_committed_xcts(_durable_flushed_lsn_offset, t.get_start());
    }
    // Now send global persisted LSN (no need to wait for ack)
    if (config::log_ship_by_rdma) {
      rep::primary_rdma_set_global_persisted_lsn(_durable_flushed_lsn_offset);
    } else {
      for (auto &fd : rep::backup_sockfds) {
        uint32_t nbytes = send(fd, (char*)&_durable_flushed_lsn_offset, sizeof(uint64_t), 0);
        LOG_IF(FATAL, nbytes != sizeof(uint64_t)) << "Error sending global persisted lsn";
      }
    }
  }
  rep::primary_ship_log_buffer_all(buf, nbytes, have_imm, imm);
}

// Wait for persistence ack from backups (if required) and dequeue transactions
// pending persistence. Called only after successfully persisting the log.
void sm_log_alloc_mgr::PrimaryCommitPersistedWork(uint64_t new_offset) {
  if (config::num_active_backups) {
    if (!config::IsLoading() && config::persist_policy == config::kPersistSync) {
      if (config::log_ship_by_rdma) {
        // Wait for the backup to persist (it might act fast and set
        // ReadyToReceive too)
        rep::primary_rdma_wait_for_message(rep::kRdmaPersisted, false);
        {
          // Dequeue transactions since data is persisted everywhere
          util::timer t;
          dequeue_committed_xcts(new_offset, t.get_start());
        }
        // Now set the global persisted LSN in each node to enable querying of
        // the shipped data.
        // Note: this is necessary because we don't want the replicas to
        // return results that are not yet deemed 'durable' by the primary.
        // If this is ignored, some replicas might return result that is
        // not yet made durable in other replicas, i.e., returning results
        // based on uncommitted data.
        rep::primary_rdma_set_global_persisted_lsn(new_offset);
        // Now we need to poll to make sure the RDMA write WQEs are consumed,
        // one for the log buffer partition bounds, the other for data
        rep::primary_rdma_poll_send_cq(2);
      } else {
        for (auto &fd : rep::backup_sockfds) {
          tcp::expect_ack(fd);
        }
        {
          util::timer t;
          dequeue_committed_xcts(new_offset, t.get_start());
        }
        // Set global persisted LSN
        for (auto &fd : rep::backup_sockfds) {
          uint32_t nbytes = send(fd, (char*)&new_offset, sizeof(uint64_t), 0);
          LOG_IF(FATAL, nbytes != sizeof(uint64_t)) << "Error sending global persisted lsn";
        }
      }
    } else if (config::persist_policy == config::kPersistAsync) {
      util::timer t;
      dequeue_committed_xcts(new_offset, t.get_start());
    }
  } else if (config::group_commit) {
    util::timer t;
    dequeue_committed_xcts(new_offset, t.get_start());
  }
}

int sm_log_alloc_mgr::open_segment_for_read(segment_id *sid) {
  return _lm.open_for_read(sid);
}

/* Figure out the corresponding segments in the logbuf and flush them.
 * The caller should enter/exit_rcu().
 */
segment_id *sm_log_alloc_mgr::PrimaryFlushLog(uint64_t new_dlsn_offset,
                                              bool update_dmark) {
  ASSERT(!config::is_backup_srv() || config::command_log);
  /* The primary ships log records at log buffer flush boundaries, and log
   * flushing respects segment boundaries. Threads trying to carve out a range
   * of LSN offset also need to respect segment boundaries, boundary-crossing
   * threads will try again after realizing the current segment isn't enough to
   * hold the requested range, hence we might have daed zones that don't
   * correspond to any valid range.
   *
   * When shipping log records, we never ship across buffer boundaries or
   * segments. The backup always receives a chunk of the log guaranteed to
   * reside in a single segment, but the range might extend into the dead zone.
   * So we might have a durable LSN in the dead zone on the backup, which is
   * fine as we skip it during redo.
   */
  LSN dlsn = _lm.get_durable_mark();
  ASSERT(_durable_flushed_lsn_offset == dlsn.offset());
  ASSERT(_durable_flushed_lsn_offset <= new_dlsn_offset);
  auto *durable_sid = _lm.get_segment(dlsn.segment());
  ALWAYS_ASSERT(durable_sid);
  uint64_t durable_byte = durable_sid->buf_offset(_durable_flushed_lsn_offset);
  int active_fd = _lm.open_for_write(durable_sid);
  DEFER(os_close(active_fd));

  /* The block list contains a fluctuating---and usually fairly
     short---set of log_allocation objects. Releasing or
     discarding a block marks it as dead (without removing it)
     and removes all dead blocks that follow it. The list is
     primed at start-up with the durable LSN (as determined by
     startup/recovery), and so is guaranteed to always contain
     at least one (perhaps dead) node that later requests can
     use to acquire a proper LSN.

     Our goal is to find the oldest (= last) live block in the
     list, and write out everything before that block's offset.

     Once we know the offset, we can look up the corresponding
     segment to obtain an LSN.
   */
  bool new_seg = false;
  while (_durable_flushed_lsn_offset < new_dlsn_offset) {
    segment_id *new_sid;
    uint64_t new_offset;
    uint64_t new_byte;

    if (durable_sid->end_offset < new_dlsn_offset + MIN_LOG_BLOCK_SIZE) {
      /* Watch out for segment boundaries!

         The true end of a segment is somewhere in the last
         MIN_LOG_BLOCK_SIZE bytes, with the exact value
         determined by the start_offset of its
         successor. Fortunately, any request that lands in
         this "red zone" also ensures that the next segment
         has been created, so we can safely access it.
       */
      new_sid = _lm.get_segment((durable_sid->segnum + 1) % NUM_LOG_SEGMENTS);
      ASSERT(new_sid);
      new_offset = new_sid->start_offset;
      new_byte = new_sid->byte_offset;
      DLOG(INFO) << "Crossing segment boundary, new_offset=" << std::hex
                 << new_offset << " new_byte=" << new_byte << std::dec;
    } else {
      new_sid = durable_sid;
      new_offset = new_dlsn_offset;
      new_byte = new_sid->buf_offset(new_dlsn_offset);
    }

    ASSERT(durable_byte == _logbuf->read_begin());
    ASSERT(durable_byte < new_byte);
    ASSERT(new_byte <= _logbuf->write_end());

    /* Log insertions don't advance the buffer window because
       they tend to complete out of order. Do it for them now
       that we know the correct value to use. The only exception
       is when we read and replay the log buffer directly.
     */
    uint64_t nbytes = new_byte - durable_byte;
    if (_logbuf->available_to_read() < nbytes) {
      _logbuf->advance_writer(new_byte);
    }
    THROW_IF(_logbuf->available_to_read() < nbytes, log_file_error,
             "Not enough log bufer to read");

    // perform the write
    auto *buf = _logbuf->read_buf(durable_byte, nbytes);
    auto file_offset = durable_sid->offset(_durable_flushed_lsn_offset);

    // Ship the log to backups, unless we're doing async log shipping
    if (!config::command_log &&
        config::persist_policy != config::kPersistAsync &&
        config::num_active_backups &&
        !config::IsLoading() &&
        !config::command_log) {
      PrimaryShipLog(durable_sid, nbytes, new_seg, new_offset, buf);
      if (new_seg) {
        new_seg = false;
      }
    }

    uint64_t n = 0;
    // Note: Here we actually allow skip log writing on the primary node even in
    // a primary/backup setting, but for benchmarking purpose only. A fully
    // 'correct' setting is to ensure persistence at *all* nodes, including the
    // primary.  Note(tzwang): 20170428: the only reason I added this is due to
    // lack of DRAM space for storing log files in tmpfs.
    if (config::null_log_device && !config::IsLoading()) {
      n = nbytes;
    } else {
      n = os_pwrite(active_fd, buf, nbytes, file_offset);
      if (!config::command_log && config::persist_policy == config::kPersistAsync) {
        rep::async_ship_cond.notify_all();
      }
    }
    LOG_IF(FATAL, n < nbytes) << "Incomplete log write";

    if (!config::command_log) {
      if (config::IsForwardProcessing() && config::num_active_backups) {
        rep::log_size_for_ship += nbytes;
      }

      // Dequeue transactions pending persistence (if pipelined group commit is on)
      PrimaryCommitPersistedWork(new_offset);
    }

    // After this the buffer space will become available for consumption
    _logbuf->advance_reader(new_byte);

    // segment change?
    if (new_sid != durable_sid) {
      os_close(active_fd);
      active_fd = _lm.open_for_write(new_sid);
      ASSERT(!new_seg);
      new_seg = true;
    }

    // update values for next round
    durable_sid = new_sid;
    _durable_flushed_lsn_offset = new_offset;
    durable_byte = new_byte;

    if (update_dmark) {
      // Have to use LSN::make (instead of durable_sid->make_lsn which checks
      // lsn offset ownership): If we're on a backup server, then this new
      // durable lsn offset might end up in the deadzone which isn't contained
      // by any sid, because we ship at log buffer flush boundaries (no info
      // for the next segment until the next shipping).
      _lm.update_durable_mark(
          LSN::make(_durable_flushed_lsn_offset, durable_sid->segnum));
    }
  }
  return durable_sid;
}

/* Allocating a log block is a multi-step process.

   1. Ensure there is sufficient space in the log file for the new
      block. We have to ensure there is always enough log space to
      reclaim at least one segment, or the log could become "wedged"
      (where log reclamation cannot proceed because the log is
      full). Sequence number allocation is not easily undone, so it's
      better to prevent this particular problem than to cure it.

   2. Acquire a sequence number by incrementing the log counter. The
      result is almost an LSN, but lacks log segment information.

   3. Identify the block's log segment. Most of the time this is as
      simple as looking up the currently active segment (and verifying
      that it contains the obtained sequence number), but segment
      boundaries complicate things. Due to the way we install new log
      segments, each segment change involves a pattern like the
      following:

      | ... segment i | dead zone | segment i+1 ... |
          |   A   |   B   |   C   |   D   |   E   |

      Block A is the common case discussed already, and does not
      overlap with the segment change. Block B overflows the segment
      and is thus unusable; the owner of that block is responsible to
      "close" the segment by logging a "segment change" record (really
      just a skip record) so that recovery proceeds to the new segment
      rather than truncating the log. Block C lost the race to install
      a new segment, and ended up in the "dead zone" between the two
      segments; that block does not map to any physical location in
      the log and must be discarded. Block D won the race to install
      the new segment, and thus becomes the first block of the new
      segment. Block E lost the segment-change race, but was lucky to
      have a predecessor win. It becomes a valid block in the new
      segment once the dust settles.

   4. Wait for buffer space to become available. A fixed-size buffer
      holds a sliding window of the log, with space for new records
      becoming available as old ones reach disk. Assuming the log
      cannot become wedged, it's just a matter of time until the
      buffer space is ready.

 */
log_allocation *sm_log_alloc_mgr::allocate(uint32_t nrec,
                                           size_t payload_bytes) {
#warning TODO: protocol to prevent log from becoming wedged
/* ^^^

   In any logging scheme that uses checkpoints to reclaim log
   space, a catch-22 lies in wait for the unwary implementor: the
   checkpoint must be logged, so the log will become permanently
   wedged if we allow it to completely fill. The solution is to
   reserve some amount of log space for an "emergency" checkpoint
   that can reclaim at least one segment and avert disaster. In
   our case, checkpointing doesn't actually let us reclaim space,
   but the segment-recovery protocol we use has the same problem.

   Unfortunately, our single-CAS scheme for acquiring a LSN offset
   means we can't easily detect that the log is almost full until
   after we've already acquired an LSN and made the problem worse.

   The (as yet unimplemented) solution to this quandary is for
   each thread to check whether its newly-acquired block lands in
   a "red zone" near the end of the log capacity. If so, it must
   discard the record, abort, and block until space has been
   reclaimed. The red zone has to be large enough that every
   transaction-executing thread in the system could make a
   maximum-sized request and still leave room for a checkpoint.
 */

#warning TODO: prevent reclamation of uncommitted overflow records
  /* ^^^

     Before the system can reclaim a log segment, it must ensure
     that the segment contains no uncommitted overflow blocks. The
     simplest way is to wait for all in-flight transactions to end,
     if we know write transactions will end reasonably
     soon. Alternatively, we could track the oldest uncommitted LSN
     generated by each transaction (a loose lower bound should
     suffice) and only do the wait if that bound impinges on the
     segment we're trying to reclaim.
   */

  ASSERT(is_aligned(payload_bytes));
/* Step #1: join the log list to obtain an LSN offset.

   All we need here is the LSN offset for the new block; we don't
   yet know what segment (if any) actually contains that offset.
 */

  uint64_t *my_off = &_tls_lsn_offset[thread::my_id()];
  volatile_write(*my_off, *my_off | kDirtyTlsLsnOffset);

start_over:
  size_t nbytes = log_block::size(nrec, payload_bytes);
  auto lsn_offset = __sync_fetch_and_add(&_lsn_offset, nbytes);
  auto next_lsn_offset = lsn_offset + nbytes;

  /* We are now the proud owners of an LSN offset range, most likely
     backed by space on disk. If the rest of the insert protocol
     succeeds, the caller becomes responsible for releasing the
     block properly. However, a hole in the log will result if any
     unexpected exception interrupts the allocation protocol.

     Why? DEFER will delete the node from the block list on abnormal
     return, but leaving the corresponding physical log space
     uninitialized would effectively truncate the log at that
     point. An abnormal return means we *can't* write the log record
     to disk for whatever reason, so we DIE instead to be safe.
  */
  /* Step #2: assign the range to a segment
   */
  auto rval = _lm.assign_segment(lsn_offset, next_lsn_offset);
  auto *sid = rval.sid;
  if (not sid) {
    goto start_over;
  }

  LSN lsn = sid->make_lsn(lsn_offset);

  // If adding my payload, we're crossing a log buffer partition boundary,
  // make sure the flusher knows about this location (for log shipping).
  if (!config::IsLoading() && config::num_active_backups &&
      !config::command_log) {
    uint64_t start_partition =
        (lsn_offset / _logbuf_partition_size) % config::log_redo_partitions;
    uint64_t next_partition =
        (next_lsn_offset / _logbuf_partition_size) % config::log_redo_partitions;
    if (start_partition != next_partition) {
      ALWAYS_ASSERT((start_partition + 1) % config::log_redo_partitions ==
                    next_partition);
      volatile_write(rep::log_redo_partition_bounds[start_partition],
                     rval.next_lsn._val);
      DLOG(INFO) << "Log buffer partition: " << start_partition << " "
                 << std::hex << rep::log_redo_partition_bounds[start_partition] << std::dec
                 << " "
                 << next_lsn_offset - LSN{rep::log_redo_partition_bounds[(start_partition-1) % config::log_redo_partitions]}.offset();
    }
  }

  /* Step #3: claim buffer space (wait if it's not yet available).

     Save copies of the request parameters in case our block went
     past-end and we have to retry.
   */
  auto tmp_nbytes = nbytes;
  auto tmp_nrec = nrec;
  auto tmp_payload_bytes = payload_bytes;
  if (not rval.full_size) {
    /* Block didn't fit in the available space. Adjust the request
       parameters so we create an empty log block.
     */
    uint64_t newsz = sid->end_offset - lsn_offset;
    ASSERT(newsz < nbytes);
    tmp_nbytes = newsz;
    tmp_nrec = 0;
    tmp_payload_bytes = 0;
  }

grab_buffer:
  char *buf = _logbuf->write_buf(sid->buf_offset(lsn), tmp_nbytes);
  if (not buf) {
    /* Unavailable write buffer space is due to unconsumed reads,
       which in turn are really just due to non-durable
       log. Figure out which durable LSN corresponds to the buffer
       space we need, and wait for it. The nonlinear mapping
       between buffer offsets and LSN offsets means we may guess
       high, but that's harmless.
     */
    _write_daemon_mutex.lock();
    DEFER(_write_daemon_mutex.unlock());
    _waiting_for_durable = true;

    _kick_log_write_daemon();
    _write_complete_cond.wait(_write_daemon_mutex);
    goto grab_buffer;
  }

  log_block *b = (log_block *)buf;
  b->lsn = lsn;
  b->nrec = tmp_nrec;
  fill_skip_record(&b->records[tmp_nrec], rval.next_lsn, tmp_payload_bytes,
                   false);

  if (not rval.full_size) {
    goto start_over;
  }

  log_allocation *x = RCU::rcu_alloc();
  x->lsn_offset = lsn_offset;
  x->block = b;

  // success!
  return x;
}

void sm_log_alloc_mgr::release(log_allocation *x) {
  // Include the size of our allocation, indicated by next_lsn.
  // Otherwise we might lose committed work.
  if (!config::is_backup_srv() || (config::command_log && config::replay_threads)) {
    // Only need to do this for the primary server - worker threads on
    // backups don't do updates
    set_tls_lsn_offset(x->block->next_lsn().offset());
  }
  RCU::rcu_free(x);
  bool should_kick = config::group_commit ?
      cur_lsn_offset() - _durable_flushed_lsn_offset >= config::group_commit_bytes :
      cur_lsn_offset() - _durable_flushed_lsn_offset >= config::log_buffer_mb * config::MB / 2;

  /* Hopefully the log daemon is already awake, but be ready to give
     it a kick if need be.
   */
  if (should_kick and
      not(volatile_read(_write_daemon_state) & DAEMON_HAS_WORK)) {
    // have to at least announce the new log record
    auto old_state = __sync_fetch_and_or(&_write_daemon_state, DAEMON_HAS_WORK);
    if (old_state == DAEMON_SLEEPING) {
      // first to arrive, have to kick daemon
      _write_daemon_mutex.lock();
      DEFER(_write_daemon_mutex.unlock());

      _kick_log_write_daemon();
    }
  }
}

void sm_log_alloc_mgr::discard(log_allocation *x) {
  /* Move the skip to front, set payload size to zero, and compute
     the resulting checksum. Then release as normal.
   */
  log_block *b = x->block;
  size_t nrec = b->nrec;
  ASSERT(b->records[nrec].type == LOG_SKIP);
  b->records[0] = b->records[nrec];
  b->records[0].payload_end = 0;
  b->nrec = 0;
  b->checksum = b->full_checksum();
  release(x);
}

uint64_t sm_log_alloc_mgr::smallest_tls_lsn_offset() {
  bool found = false;
  uint64_t min_dirty = _lsn_offset;
  uint64_t max_clean = 0;
  for (uint32_t i = 0; i < thread::next_thread_id; i++) {
    uint64_t off = volatile_read(_tls_lsn_offset[i]);
    if (off) {
      if (off & kDirtyTlsLsnOffset) {
        min_dirty = std::min(off & ~kDirtyTlsLsnOffset, min_dirty);
        found = true;
      } else {
        if (max_clean < off) {
          max_clean = off;
        }
      }
    }
  }
  return found ? min_dirty : max_clean;
}

/* This guy's only job is to write released log blocks to disk. In
   steady state, new log blocks will be released during each log
   write, keeping the daemon busy most of the time. Whenever the log
   is fully durable, it sleeps. During a clean shutdown, the daemon
   will exit only after it has written everything to disk. It is the
   system's responsibility to ensure that the shutdown flag is not
   raised while new log records might still be generated.
 */
void sm_log_alloc_mgr::_log_write_daemon() {
  RCU::rcu_register();
  RCU::rcu_enter();
  DEFER(RCU::rcu_exit());

  // every 100 ms or so, update the durable mark on disk
  static uint64_t const DURABLE_MARK_TIMEOUT_NS = uint64_t(5000) * 1000 * 1000;
  uint64_t last_dmark = stopwatch_t::now();
  while (true) {
    uint64_t cur_offset = cur_lsn_offset();
    uint64_t min_tls = smallest_tls_lsn_offset();
    uint64_t new_dlsn_offset = min_tls;
    if (!config::IsLoading() && config::num_active_backups > 0 && !config::command_log) {
      uint64_t max_size = config::group_commit_bytes + MIN_LOG_BLOCK_SIZE;
      if (new_dlsn_offset - _durable_flushed_lsn_offset > max_size) {
        // Find the maximum that will cause us to ship at most [group_commit_size_kb]
        uint64_t max = 0;
        for (uint64_t i = 0; i < config::log_redo_partitions; ++i) {
          uint64_t off = LSN{rep::log_redo_partition_bounds[i]}.offset();
          if (off <= min_tls && off > max && (off - _durable_flushed_lsn_offset <= max_size)) {
            max = off;
          }
        }
        new_dlsn_offset = max;
      }
    }
    segment_id *durable_sid = nullptr;
    if (new_dlsn_offset > _durable_flushed_lsn_offset) {
      durable_sid = PrimaryFlushLog(new_dlsn_offset);
    }

    RCU::rcu_exit();

    /* Having completed a round of writes, notify waiting threads
       and take care of special cases
     */
    _write_daemon_mutex.lock();
    DEFER(_write_daemon_mutex.unlock());

    // wake up any waiters if the old value was smaller than the waited-for one
    if (_waiting_for_durable) {
      _waiting_for_durable = false;
      _write_complete_cond.broadcast();
    }

    // update dmark?
    if (_lm.get_durable_mark().offset() < _durable_flushed_lsn_offset) {
      auto now = stopwatch_t::now();
      bool timeout = DURABLE_MARK_TIMEOUT_NS <= (last_dmark - now);
      bool should_update = timeout or _waiting_for_durable;
      if (_durable_flushed_lsn_offset == cur_offset and
          _write_daemon_should_stop)
        should_update = true;

      if (should_update) {
        ALWAYS_ASSERT(durable_sid);
        last_dmark = now;
        _lm.update_durable_mark(
            durable_sid->make_lsn(_durable_flushed_lsn_offset));
      }
    }

    if (_waiting_for_dmark) {
      _waiting_for_dmark = false;
      _dmark_updated_cond.broadcast();
    }

    // time to quit? (only if everything in the log reached disk)
    if (_write_daemon_should_stop and
        cur_offset == _durable_flushed_lsn_offset) {
      if (new_dlsn_offset == cur_offset) break;
    }

    // time to sleep?
    while (!_write_daemon_should_stop && !(volatile_read(_write_daemon_state) & DAEMON_HAS_WORK)) {
      // looks like we can sleep
      auto old_state =
          __sync_fetch_and_or(&_write_daemon_state, DAEMON_SLEEPING);
      if (old_state & DAEMON_HAS_WORK or _write_daemon_should_wake) {
        // never mind!
        volatile_write(_write_daemon_state, DAEMON_HAS_WORK);
      } else {
        // wake up after 5 seconds if nobody kicks me
        // to prevent when there's nobody writing to the case of:
        // logbuf => nobody kicking => log buffer never flushed
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 5000;
        _write_daemon_cond.timedwait(_write_daemon_mutex, &ts);
      }

      _write_daemon_should_wake = false;
    }

    // next loop iteration!
    volatile_write(_write_daemon_state, 0);
    RCU::rcu_enter();
  }
}

/* Wake up the log write daemon if it happens to be alseep.

   WARNING: caller must hold the log write mutex!
 */
void sm_log_alloc_mgr::_kick_log_write_daemon() {
  _write_daemon_should_wake = true;
  _write_daemon_cond.signal();
}
