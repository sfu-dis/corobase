// -*- mode:c++ -*-
#ifndef __SM_LOG_SEGMENTS_H
#define __SM_LOG_SEGMENTS_H

#include "sm-common.h"

#include <pthread.h>
#include <utility>
#include <memory>

/* Log segment-tracking machinery, factored out of the main
   implementation to allow easier/better testing.
 */

struct log_segment_desc {
  /* Offset of the starting LSN of this segment.  Used to convert
     addresses to LSN and vice versa.
  */
  uint64_t start_offset;

  /* The first LSN offset that is past-end for this segment (based
     on the allowed file size at the time the segment was installed;
     for now we don't support changing a segment's size after it
     opens).
  */
  uint64_t end_offset;

  /* The number of bytes of log data that precede the start of this
     segment. Used to simplify buffering of log records in
     memory. Unlike start_offset, this sequence has no holes. It is
     computed as the sum of the sizes of all preceding segments,
     including their segment-close records.
   */
  uint64_t byte_offset;

  /* The logical segment number in use. The sequence of segement
     numbers increases monotonically and without gaps, and
     identifies (mod 16) the physical segment number in use.
  */
  int64_t segment_number;

  /* Compute the buffer offset of an LSN in this segment.
   */
  uint64_t buf_offset(uint64_t lsn_offset) {
    ASSERT(start_offset <= lsn_offset and lsn_offset <= end_offset);
    return byte_offset + (lsn_offset - start_offset);
  }
};

struct log_segment_mgr {
  /* Create a new segment manager, with the specified segment size.
   */
  log_segment_mgr(uint64_t ssize, uint64_t scsize);

  /* Return the segment descriptor that corresponds to a segment
     number.
   */
  log_segment_desc *get_segment(int segnum);

  /* Assign a segment to the given sequence number, opening a new
     segment if necessary.

     Throw log_is_full if a new segment is needed, but cannot be
     created because NUM_LOG_SEGMENTS already exist.

     WARNING: this code makes no provision for the case where a
     single log record block approaches or passes the segment size.
   */
  std::pair<log_segment_desc *, bool> assign_segment(uint64_t lsn_begin,
                                                     uint64_t lsn_end);

  /* Attempt to install a new segment---successor to sd---that starts at
     lsn_offset. Threads race to call this function if they obtain a log
     block that spills past-end for the current active segment.

     Return the new segment if the given lsn_offset ended up inside it
     (either because we won the race, or because the winner had an
     earlier offset). Otherwise, return NULL.
   */
  log_segment_desc *install_segment(log_segment_desc *sd, uint64_t lsn_offset);

  /* What size is a new segment? This can be changed any time, but
     only affects new segments at the moment they are opened.

     WARNING: segments must be closed by a skip-only log block,
     which is *NOT* included in this segment size. Instead, the
     skip-only block is accounted for by segment_close_size.
   */
  uint64_t volatile segment_size;

  /* What size is a segment-close record? We must reserve this many
     bytes at the end of each segment.
   */
  uint64_t const segment_close_size;

  /* The currently active segment. To avoid races, we do not store
     the current segment in the segment array until just before a
     new segment is installed.
  */
  log_segment_desc *volatile active_segment;

  /* The holding area for all segments *except* the currently active
     segment, which usually has a NULL entry. We do this to avoid
     race conditions where LSN decoding overlaps a segment change.
   */
  struct log_segment_array {
    log_segment_desc *volatile arr[NUM_LOG_SEGMENTS];
    log_segment_desc *volatile &operator[](size_t i) {
      return arr[i % NUM_LOG_SEGMENTS];
    }
    log_segment_array() {
      log_segment_desc *sd = NULL;
      std::uninitialized_fill(arr, arr + NUM_LOG_SEGMENTS, sd);
    }
  };

  log_segment_array all_segments;

  pthread_mutex_t segment_change_mutex;
};

#endif
