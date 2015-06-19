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
    
    typedef rcu_slist<log_allocation> rcu_block_list;
    
    sm_log_alloc_mgr(char const *dname, size_t segment_size,
                      sm_log_recover_function *rfn, void *rfn_arg,
                      size_t bufsz);
    
    ~sm_log_alloc_mgr();

    /* Kick the log writer daemon and wait for it to finish flushing
     * the log buffer
     */
    LSN flush();

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
    rcu_block_list _block_list;
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

};

#endif
