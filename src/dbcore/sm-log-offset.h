// -*- mode:c++ -*-
#ifndef __SM_LOG_OFFSET_H
#define __SM_LOG_OFFSET_H

#include "sm-log-file.h"

/* The LSN generator of the log.

   When allocating a log block, a thread must acquire an LSN offset
   and then map it to a corresponding location on disk as a separate
   step. The benefit is that the monotonic part of LSN generation (the
   expensive part) simplifies to a single atomic add, thus easing a
   notorious global contention point (the move to O(1) log record per
   transaction also helps a lot there). The downside is the
   introduction of race conditions when mapping those offsets to
   segments, in particular at segment boundaries.  Whenever a thread
   acquires an LSN offset that is partly or fully past-end of the last
   segment, it must install a new segment, in a race with all other
   threads that find themselves in the same situation.
 */
struct sm_log_offset_mgr : sm_log_file_mgr {
  struct segment_assignment {
    // segment of this log block
    segment_id *sid;

    // LSN of the next log block
    LSN next_lsn;

    // allocation big enough for the requested log block?
    bool full_size;
  };

  using sm_log_file_mgr::sm_log_file_mgr;

  /* Convert an LSN into the fat_ptr that can be used to access the
     log record at that LSN. Normally the LSN should point to the
     payload section of a record. If [is_ext] is true, the returned
     pointer has ASI_EXT rather than ASI_LOG.
  */
  fat_ptr lsn2ptr(LSN lsn, bool is_ext);

  /* Convert a fat_ptr into the LSN it corresponds to.

     Throw illegal_argument if the pointer does not correspond to
     any LSN.
  */
  LSN ptr2lsn(fat_ptr ptr);

  /* Retrieve the segment descriptor that corresponds to
     [mod_segnum]. The latter is a segment number modulo
     NUM_LOG_SEGMENTS, as if returned by LSN::segment.

     The caller is responsible to ensure that the segment exists and
     is the correct one. The latter should not be too difficult to
     achieve, given that the log never contains more than
     NUM_LOG_SEGMENTS segments.
   */
  segment_id *get_segment(uint32_t mod_segnum);

  /* Retrieve the segment descriptor that corresponds to the given
     [lsn_offset]. If the offset is not part of any segment, return
     NULL instead. This function should only be called for offsets
     that have already been assigned a segment.
  */
  segment_id *get_offset_segment(uint64_t lsn_offset);

  /* Assign a segment to the given range of LSN offsets, hopefully
     yielding a full LSN.

     There are three possible outcomes:

     1. LSN assignment is successful (the common case). Return
        {sid,next_lsn,true}, where [sid] is the segment the LSN
        belongs to and [next_lsn] is the LSN that follows (possibly
        in a different segment).

     2. LSN assignment succeeded, but the requested block runs past
        the end of the segment. Return {sid,next_lsn,false}, where
        [sid] is the segment the LSN belongs to, and [next_lsn] is
        the first LSN of the next segment. The caller must "close"
        the segment with a skip record, and can then retry.

     3. The given LSN offset does not correspond to any physical
        location in the log. Return {NULL,INVALID_LSN,false}. The
        caller should discard this LSN offset and request a new one.
   */
  segment_assignment assign_segment(uint64_t lsn_begin, uint64_t lsn_end);

  /* Install a new segment.
   */
  segment_id *_install_segment(segment_id *sid, uint64_t lsn_offset);
};

#endif
