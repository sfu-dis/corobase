// -*- mode:c++ -*-
#ifndef __SM_LOG_ALLOC_H
#define __SM_LOG_ALLOC_H

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
    sm_log_alloc_mgr(char const *dname, size_t segment_size,
                      sm_log_recover_function *rfn, void *rfn_arg,
                      size_t bufsz);
    
    ~sm_log_alloc_mgr();

    void setup_tls_lsn_offset(uint32_t threads);
    void set_tls_lsn_offset(uint64_t offset);

    /* Kick the log writer daemon and wait for it to finish flushing
     * the log buffer
     */
    LSN flush();
    LSN flush_cur_lsn();

    /* Retrieve the current end of log
     */
    uint64_t cur_lsn_offset();

    /* Retrieve the current durable end of log
     */
    uint64_t dur_lsn_offset();

    /* Block the caller until the specified LSN offset has become durable
     */
    void wait_for_durable(uint64_t dlsn_offset);

    /* Block the caller until all log entries before [dlsn_offset]
       have been made durable, and the durable mark updated.
     */
    void update_durable_mark(uint64_t dlsn_offset);
    
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

    sm_log_recover_mgr _lm;
    window_buffer _logbuf;
    uint64_t _durable_lsn_offset;

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

    // tzwang: use one _tls_lsn_offset per worker thread to record its most-recently
    // committed/aborted transaction's log lsn offset. This array together is used to
    // replace the rcu-slist as a result of the decoupling of getting an LSN and the
    // accounting of log allocation objects (in an rcu-slist). Using the rcu-slist we
    // have to use **at least** one CAS per transaction and there's no bounded number
    // of steps for a thread to successfully get an LSN offset. Tests on a 32-core 
    // (4 sockets) and 60-core (4 sockets), and a 240-core (16 sockets) machines show
    // this doesn't scale after around 30 threads.
    //
    // So now we use a single atomic fetch_and_add to get the LSN offset. Each thread
    // is guaranteed to get a valid LSN offset using exactly **one** instruction.
    //
    // Still, the log flusher needs to know the "oldest live log allocation" (in the
    // rcu-slist term), which is the earliest LSN offset/transaction that has written
    // to the log buffer. Note the discrepancy here: in both the rcu-slist and atomic
    // fetch-and-add schemes, we might have holes in the log buffer. So we need to find
    // the earliest guy who has finished writing to the log buffer.
    //
    // Without a centralized rcu-slist using CAS, we have to use a fully decentralized
    // approach - schemes like one rcu-slist per socket won't completely solve the
    // problem because there is no coordination between threads on the same socket.
    // That will direct us back to use CAS again which is what we wanted to avoid.
    //
    // The solution is borrowed from shore-mt's dlog's passive group commit, which solves
    // a similar problem in NVM+dlog settings. Each thread will put its just committed/aborted
    // lsn offset (corresponds to the log allocation block in the rcu-slist scheme) to its
    // own "tls" place, and the log flusher just scans all these tls places, then flush up
    // to the **smallest** lsn it found.
    uint64_t _lsn_offset;
    uint64_t *_tls_lsn_offset;
};

#endif
