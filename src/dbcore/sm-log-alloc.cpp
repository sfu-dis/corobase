#include "sm-log-alloc.h"
#include "stopwatch.h"
#include "../macros.h"
#include "sm-config.h"

using namespace RCU;

namespace {

    uint64_t
    get_starting_byte_offset(sm_log_recover_mgr *lm)
    {
        auto dlsn = lm->get_durable_mark();
        auto *sid = lm->get_segment(dlsn.segment());
        return sid->offset(dlsn);
    }

    extern "C"
    void*
    log_write_daemon_thunk(void *arg)
    {
        ((sm_log_alloc_mgr*) arg)->_log_write_daemon();
        return NULL;
    }

    enum { DAEMON_HAS_WORK=0x1, DAEMON_SLEEPING=0x2 };

} // end anonymous namespace

void
sm_log_alloc_mgr::set_tls_lsn_offset(uint64_t offset)
{
    volatile_write(_tls_lsn_offset[sysconf::my_thread_id()], offset);
}

/* We have to find the end of the log files on disk before
   constructing the log buffer in memory. It's also a convenient time
   to do the rest of recovery, because it prevents any attempt at
   forward processing before recovery completes. 
 */
sm_log_alloc_mgr::sm_log_alloc_mgr(sm_log_recover_function *rfn, void *rfn_arg)
    : _lm(sysconf::null_log_device ? NULL : rfn, rfn_arg)
    , _logbuf(sysconf::log_buffer_mb * 1024 * 1024, get_starting_byte_offset(&_lm))
    , _durable_lsn_offset(_lm.get_durable_mark().offset())
    , _write_daemon_state(0)
    , _waiting_for_durable(false)
    , _waiting_for_dmark(false)
    , _write_daemon_should_wake(false)
    , _write_daemon_should_stop(false)
    , _lsn_offset(_lm.get_durable_mark().offset())
{
    sysconf::_active_threads = 0;
    _tls_lsn_offset = (uint64_t *)malloc(sizeof(uint64_t) * sysconf::MAX_THREADS);
    for (uint32_t i = 0; i < sysconf::MAX_THREADS; i++)
        _tls_lsn_offset[i] = _lm.get_durable_mark().offset();

    // fire up the log writing daemon
    _write_daemon_mutex.lock();
    DEFER(_write_daemon_mutex.unlock());
    
    int err = pthread_create(&_write_daemon_tid, NULL,
                             &log_write_daemon_thunk, this);
    THROW_IF(err, os_error, err, "Unable to start log writer daemon thread");

}

sm_log_alloc_mgr::~sm_log_alloc_mgr()
{
    {
        _write_daemon_mutex.lock();
        DEFER(_write_daemon_mutex.unlock());
        
        _write_daemon_should_stop = true;
        flush_cur_lsn();
    }
    
    int err = pthread_join(_write_daemon_tid, NULL);
    THROW_IF(err, os_error, err, "Unable to join log writer daemon thread");
}

uint64_t
sm_log_alloc_mgr::cur_lsn_offset()
{
    return volatile_read(_lsn_offset);
}

uint64_t
sm_log_alloc_mgr::dur_lsn_offset()
{
    return volatile_read(_durable_lsn_offset);
}

void
sm_log_alloc_mgr::wait_for_durable(uint64_t dlsn_offset)
{
    if (dur_lsn_offset() < dlsn_offset) {
        _write_daemon_mutex.lock();
        DEFER(_write_daemon_mutex.unlock());

        while (dur_lsn_offset() < dlsn_offset) {
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

void
sm_log_alloc_mgr::update_wait_durable_mark(uint64_t lsn_offset)
{
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
 *
 * tzwang: the caller so far is only the chkpt thread.
 */
LSN
sm_log_alloc_mgr::flush()
{
    _write_daemon_mutex.lock();
    DEFER(_write_daemon_mutex.unlock());
    _waiting_for_dmark = true;
    __sync_fetch_and_or(&_write_daemon_state, DAEMON_HAS_WORK);
    _kick_log_write_daemon();
    _dmark_updated_cond.wait(_write_daemon_mutex);
    return _lm.get_durable_mark();
}

/* Flush **everything** up to the current largest thread-local committed LSN.
 * This implicitly assumes **all** the transactions with a clsn < the largest
 * TLS commit lsn offset have concluded, so flushing up to the most recent offset
 * won't make any holes.
 *
 * **USE THIS ONLY WHEN YOU SURE ABOUT WHAT YOU'RE DOING**
 *
 * So far the only users of this function are ~sm_log_alloc_mgr and the loader
 * (after loaded the database).
 */
LSN
sm_log_alloc_mgr::flush_cur_lsn()
{
    for (uint32_t i = 0; i < sysconf::_active_threads; ++i) {
        volatile_write(_tls_lsn_offset[i], cur_lsn_offset());
    }
    return flush();
}

/* Figure out the corresponding segments in the logbuf and flush them.
 * The caller should enter/exit_rcu().
 */
segment_id *
sm_log_alloc_mgr::flush_log_buffer(window_buffer &logbuf, uint64_t new_dlsn_offset, bool update_dmark)
{
    LSN dlsn = _lm.get_durable_mark();
    ASSERT(_durable_lsn_offset == dlsn.offset());
    auto *durable_sid = _lm.get_segment(dlsn.segment());
    uint64_t durable_byte = durable_sid->buf_offset(_durable_lsn_offset);
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
    while (_durable_lsn_offset < new_dlsn_offset) {
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
            new_sid = _lm.get_segment((durable_sid->segnum+1) % NUM_LOG_SEGMENTS);
            ASSERT(new_sid);
            new_offset = new_sid->start_offset;
            new_byte = new_sid->byte_offset;
        }
        else {
            new_sid = durable_sid;
            new_offset = new_dlsn_offset;
            new_byte = new_sid->buf_offset(new_dlsn_offset);
        }

        ASSERT(durable_byte == logbuf.read_begin());
        ASSERT(durable_byte < new_byte);
        ASSERT(new_byte <= logbuf.write_end());

        /* Log insertions don't advance the buffer window because
           they tend to complete out of order. Do it for them now
           that we know the correct value to use.
         */
        logbuf.advance_writer(new_byte);

        // perform the write
        uint64_t nbytes = new_byte - durable_byte;
        auto *buf = logbuf.read_buf(durable_byte, nbytes);
        auto file_offset = durable_sid->offset(_durable_lsn_offset);
        if (!sysconf::null_log_device) {
          uint64_t n = os_pwrite(active_fd, buf, nbytes, file_offset);
          THROW_IF(n < nbytes, log_file_error, "Incomplete log write");
        }
        if (sysconf::num_active_backups) {
          auto start_lsn = durable_sid->make_lsn(_durable_lsn_offset);
          auto end_lsn = durable_sid->make_lsn(new_offset);
          ASSERT(start_lsn.segment() == end_lsn.segment());
          rep::primary_ship_log_buffer_all(
            buf,
            durable_sid->make_lsn(_durable_lsn_offset),
            durable_sid->make_lsn(new_offset),
            nbytes);
        }

        logbuf.advance_reader(new_byte);

        // segment change?
        if (new_sid != durable_sid) {
            os_close(active_fd);
            active_fd = _lm.open_for_write(new_sid);
        }

        // update values for next round
        durable_sid = new_sid;
        _durable_lsn_offset = new_offset;
        durable_byte = new_byte;

        if (update_dmark)
            _lm.update_durable_mark(durable_sid->make_lsn(_durable_lsn_offset));
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
log_allocation *
sm_log_alloc_mgr::allocate(uint32_t nrec, size_t payload_bytes)
{
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

    ASSERT (is_aligned(payload_bytes));
    /* Step #1: join the log list to obtain an LSN offset.

       All we need here is the LSN offset for the new block; we don't
       yet know what segment (if any) actually contains that offset.
     */
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
    char *buf = _logbuf.write_buf(sid->buf_offset(lsn), tmp_nbytes);
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

    log_block *b = (log_block*) buf;
    b->lsn = lsn;
    b->nrec = tmp_nrec;
    fill_skip_record(&b->records[tmp_nrec], rval.next_lsn, tmp_payload_bytes, false);

    if (not rval.full_size) {
        goto start_over;
    }

    log_allocation *x = rcu_alloc();
    x->lsn_offset = lsn_offset;
    x->block = b;

    // success!
    return x;
}

void
sm_log_alloc_mgr::release(log_allocation *x)
{
    set_tls_lsn_offset(x->lsn_offset);
    rcu_free(x);
    bool should_kick = cur_lsn_offset() - dur_lsn_offset() >= _logbuf.window_size() / 2;

    /* Hopefully the log daemon is already awake, but be ready to give
       it a kick if need be.
     */
    if (should_kick and not (volatile_read(_write_daemon_state) & DAEMON_HAS_WORK)) {
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

void
sm_log_alloc_mgr::discard(log_allocation *x)
{
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

uint64_t
sm_log_alloc_mgr::latest_durable_lsn_offset()
{
    uint64_t oldest_offset = cur_lsn_offset();
    for (uint32_t i = 0; i < sysconf::_active_threads; i++) {
        // Skip too small/stale LSNs (maybe due to running read-only
        // transactions using SSN's safesnap) so that we can make progress.
        if (_tls_lsn_offset[i] > _durable_lsn_offset)
            oldest_offset = std::min(_tls_lsn_offset[i], oldest_offset);
    }
    return oldest_offset;
}

/* This guy's only job is to write released log blocks to disk. In
   steady state, new log blocks will be released during each log
   write, keeping the daemon busy most of the time. Whenever the log
   is fully durable, it sleeps. During a clean shutdown, the daemon
   will exit only after it has written everything to disk. It is the
   system's responsibility to ensure that the shutdown flag is not
   raised while new log records might still be generated.
 */
void
sm_log_alloc_mgr::_log_write_daemon()
{
    rcu_register();
    rcu_enter();
    DEFER(rcu_exit());

    // every 100 ms or so, update the durable mark on disk
    static uint64_t const DURABLE_MARK_TIMEOUT_NS = uint64_t(5000)*1000*1000;
    uint64_t last_dmark = stopwatch_t::now();
    for (;;) {
        auto cur_offset = cur_lsn_offset();
        auto new_dlsn_offset = latest_durable_lsn_offset();
        auto *durable_sid = flush_log_buffer(_logbuf, new_dlsn_offset);

        rcu_exit();

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
        if (_lm.get_durable_mark().offset() < _durable_lsn_offset) {
            auto now = stopwatch_t::now();
            bool timeout = DURABLE_MARK_TIMEOUT_NS <= (last_dmark - now);
            bool should_update = timeout or _waiting_for_durable;
            if (_durable_lsn_offset == cur_offset and _write_daemon_should_stop)
                should_update = true;
        
            if (should_update) {
                last_dmark = now;
                _lm.update_durable_mark(durable_sid->make_lsn(_durable_lsn_offset));
            }
        }

        if (_waiting_for_dmark) {
            _waiting_for_dmark = false;
            _dmark_updated_cond.broadcast();
        }

        // time to quit? (only if everything in the log reached disk)
        if (_write_daemon_should_stop and cur_offset == _durable_lsn_offset) {
            if (new_dlsn_offset == cur_offset)
                return;
        }

        // time to sleep?
        while (not (volatile_read(_write_daemon_state) & DAEMON_HAS_WORK)) {
            // looks like we can sleep
            auto old_state = __sync_fetch_and_or(&_write_daemon_state, DAEMON_SLEEPING);
            if (old_state & DAEMON_HAS_WORK or _write_daemon_should_wake) {
                // never mind!
                volatile_write(_write_daemon_state, DAEMON_HAS_WORK);
            }
            else {
                // wake up after 5 seconds if nobody kicks me
                // to prevent when there's nobody writing to the case of:
                // logbuf => nobody kicking => log buffer never flushed
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 5;
                _write_daemon_cond.timedwait(_write_daemon_mutex, &ts);
            }
            
            _write_daemon_should_wake = false;
        }
        
        // next loop iteration!
        volatile_write(_write_daemon_state, 0);
        rcu_enter();
    }
}

/* Wake up the log write daemon if it happens to be alseep.

   WARNING: caller must hold the log write mutex!
 */
void
sm_log_alloc_mgr::_kick_log_write_daemon()
{
    _write_daemon_should_wake = true; 
    _write_daemon_cond.signal();
}
