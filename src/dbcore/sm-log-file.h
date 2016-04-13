// -*- mode:c++ -*-
#ifndef __SM_LOG_FILE_H
#define __SM_LOG_FILE_H

#include "sm-log-defs.h"

#include <deque>

/* The file management part of the log.

   This class is responsible for the naming, creation, and deletion of
   log-related files. It is *not* concerned with the contents of any
   of these files (some of which are always empty), nor is it
   concerned with *why* a file exists, is updated, or deleted.
 */

struct segment_id {
    int fd;
    uint32_t segnum;
    uint64_t start_offset;
    uint64_t end_offset;
    uint64_t byte_offset;
    
    bool contains(uint64_t lsn_offset) {
        return start_offset <= lsn_offset
            and lsn_offset+MIN_LOG_BLOCK_SIZE <= end_offset;
    }

    bool contains(LSN lsn) {
        if (lsn.segment() != segnum % NUM_LOG_SEGMENTS)
            return false;
        return contains(lsn.offset());
    }

    /* Compute the segment offset of an LSN in this segment
     */
    uint64_t offset(uint64_t lsn_offset) {
        ASSERT(contains(lsn_offset));
        return lsn_offset - start_offset;
    }
    uint64_t offset(LSN lsn) {
        ASSERT(contains(lsn));
        return offset(lsn.offset());
    }

    /* Compute the buffer offset of an LSN in this segment.
     */
    uint64_t buf_offset(uint64_t lsn_offset) {
        return byte_offset + offset(lsn_offset);
    }

    /* Compute the buffer offset of an LSN in this segment.
     */
    uint64_t buf_offset(LSN lsn) {
        return buf_offset(lsn.offset());
    }

    LSN make_lsn(uint64_t lsn_offset) {
        ASSERT(contains(lsn_offset));
        return LSN::make(lsn_offset, segnum % NUM_LOG_SEGMENTS);
    }

};

struct sm_log_file_mgr {
    /* A volatile modulo-indexed array, which forms part of the
       segment race-riddled assignment protocol.
     */
    struct segment_array {
        segment_array();
        ~segment_array();

        segment_id * volatile &operator[](size_t i) {
            return arr[i % NUM_LOG_SEGMENTS];
        }
        
        segment_id * volatile arr[NUM_LOG_SEGMENTS];
    };

    sm_log_file_mgr(char const *dname, size_t segment_size);

    /* Change the segment size.

       All new segments created after the point will use the new size.
     */
    void set_segment_size(size_t ssize);
    
    /* Update on-disk information to reflect a new durable LSN.

       The given LSN should point past-end of some known-durable log
       block. 

       At start-up, the system must find the end of the log by
       validating checksums, and will start that search from the most
       recent durable mark to reach disk. The actual end of log could
       be quite a bit after the most recent mark, if the mark has not
       been set recently, or the update process did not complete due
       to a crash.

       Implementations should call this function fairly often, perhaps
       once per second, to limit the amount of work required to find
       the end of the log.

       WARNING: the caller is responsible to guarantee that the log is
       really durable up to the named LSN.
    */
    void update_durable_mark(LSN lsn);

    LSN get_durable_mark() { return _durable_lsn; }

    
    /* Update on-disk information to reflect a new (durable)
       checkpoint.

       The start LSN is the point where the checkpointing process
       began, and all records before it have are accounted for in the
       checkpoint.

       The end LSN is the actual location of the checkpoint
       transaction, which should be replayed to start recovery. No
       records after it are accounted for in this checkpoint.

       Records between the start and end LSN may or may not be
       accounted for, and will be applied blindly to be safe.

       The checkpoint mark must not be later than the durable mark.
    */
    void update_chkpt_mark(LSN start, LSN end);

    LSN get_chkpt_start() { return _chkpt_start_lsn; }
    LSN get_chkpt_end() { return _chkpt_end_lsn; }

    /* Open a writable file descriptor for the passed-in log
       segment. The segment must already exist.
     */
    int open_for_write(segment_id *sid);
    
    /* Create a new log segment file, with segment number one higher
       than the current highest segnum.

       WARNING: this function is *not* threadsafe: multiple
       simultaneous calls will result in multiple new segments (or,
       more likely, raise an exception because the offsets overlap
       each other).
     */
    bool create_segment(segment_id *sid);

    /* Allocate a new segment_id as if for a new segment, but do not
       actually create the segment yet. If the passed-in start segment
       is provably not past-end, return NULL.

       A non-NULL return value of this call can then be passed to
       create_segment, if desired
     */
    segment_id *prepare_new_segment(uint64_t start);

    /* Truncate the log at the given segment and offset.

       All segments that follow [segnum] are destroyed, and the
       segment is truncated to offset [new_end].

       The durable mark must entirely precede the truncated region.
     */
    void truncate_after(uint32_t segnum, uint64_t new_end);

    /* Reclaim (destroy) log segments that precede [segnum].

       The caller is responsible to ensure that all data stored in the
       log has either been invalidated by later writes, or moved to
       safer locations.

       The reclaimed segments must all precede the checkpoint mark.
     */
    void reclaim_before(uint32_t segnum);

    /* WARNING: these are only safe to access while holding the
       file_mutex. The STL makes no guarantees whatsoever about what
       happens during races to create or destroy segments. These could
       return the correct value... but they could also return garbage,
       or even seg fault.
     */
    segment_id *_oldest_segment();
    segment_id *_newest_segment();
    void _pop_oldest();
    void _pop_newest();

    
    void _create_nxt_seg_file(bool force);
    segment_id* _prepare_new_segment(uint32_t segnum, uint64_t start, uint64_t byte_offset);
    void _make_new_log();

    // log file directory
    int dfd;

    size_t volatile segment_size;

    segment_array segments;
    segment_id *active_segment;
    uint32_t oldest_segnum;

    uint64_t nxt_segment_fd;
    
    LSN _durable_lsn;
    
    LSN _chkpt_start_lsn;
    LSN _chkpt_end_lsn;

    os_mutex file_mutex;
};


#endif
