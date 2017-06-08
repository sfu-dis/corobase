#include "sm-log-segments.h"

#include <memory>

log_segment_mgr::log_segment_mgr(uint64_t ssize, uint64_t scsize)
    : segment_size(ssize),
      segment_close_size(scsize),
      active_segment(rcu_alloc()),
      segment_change_mutex(PTHREAD_MUTEX_INITIALIZER) {
  active_segment->start_offset = 0;
  active_segment->end_offset = segment_size;
  active_segment->byte_offset = 0;
  active_segment->segment_number = 1;
}

/* At a segment change, all threads racing to open a new segment will
   copy the old active segment to its proper slot. This ensures that
   LSN lookups for that segment succeed even after the new one opens.
   The winner of the race places the new segment's pointer in
   [active_segment], which is exactly where we look if we find a NULL
   entry in all_segments (being careful to verify in those cases that
   the active_segment found has a compatible segment_number; if not,
   retry with all_segments[]).
*/
log_segment_desc *log_segment_mgr::get_segment(int segnum) {
  ASSERT(0 <= segnum and segnum < NUM_LOG_SEGMENTS);
  auto &slot = all_segments[segnum];
  log_segment_desc *sd = volatile_read(slot);
  if (not sd) {
    /* Must be the active segment, try there.

       If we race with a segment change, we might end up with the
       wrong active_segment, but when that happens the descriptor
       we want is guaranteed to be in the array afterward.
    */
    sd = active_segment;
    if (segnum != sd->segment_number % NUM_LOG_SEGMENTS) {
      sd = volatile_read(slot);
      ASSERT(sd);
      // fall out
    }

    // fall out
  }

  ASSERT(segnum == sd->segment_number % NUM_LOG_SEGMENTS);
  return sd;
}

/* Most of the time, assigning a segment number to a log sequence
   offset is as simple as looking up the currently active segment, and
   verifying that it contains the obtained sequence number. Segment
   boundaries complicate things, though. Due to the way we install new
   log segments, each segment change involves a pattern like the
   following:

       | ... segment i | dead zone | segment i+1 ... |
           |   A   |   B   |   C   |   D   |   E   |

   Block A is the common case discussed already, and does not overlap
   with the segment change. Block B overflows the segment and is thus
   unusable; the owner of that block is responsible to "close" the
   segment by logging a "segment change" record (really just a skip
   record) so that recovery proceeds to the new segment rather than
   truncating the log. Block C lost the race to install a new segment,
   and ended up in the "dead zone" between the two segments; that
   block does not map to any physical location in the log and must be
   discarded. Block D won the race to install the new segment, and
   thus becomes the first block of the new segment. Block E lost the
   segment-change race, but was lucky to have a predecessor win. It
   becomes a valid block in the new segment once the dust settles.

*/
std::pair<log_segment_desc *, bool> log_segment_mgr::assign_segment(
    uint64_t lsn_begin, uint64_t lsn_end) {
  ASSERT(lsn_begin < lsn_end);
  log_segment_desc *sd = volatile_read(active_segment);
  auto rval = std::make_pair(sd, false);

  // deal with blocks that spill past end
  if (sd->end_offset < lsn_end) {
    if (lsn_begin < sd->end_offset) {
      // block straddles segment end, caller must close segment
      sd = install_segment(sd, lsn_end);
      ASSERT(not sd or lsn_end == sd->start_offset);
      rval.second = true;
      goto done;
    }

    // block is fully past-end
    rval.first = install_segment(sd, lsn_begin);
    goto done;
  }

  /* This block ends before the end of the current segment, so no
     new segment required. Now to find the segment we belong to.
  */
  for (int i = 0; lsn_begin < sd->start_offset; i++) {
    /* There is only one valid reason why an offset might precede
       the oldest valid segment: a Rip van Winkle event could
       leave a thread holding an offset in the dead zone before
       the oldest valid segment. In that case, however, there is
       no clean-up required and the thread can simply start over.
    */
    sd = all_segments[sd->segment_number - 1];
    if (not sd or i == NUM_LOG_SEGMENTS) {
      // a Rip van Winkle that landed in the dead zone
      rval.first = NULL;
      goto done;
    }
  }

  if (lsn_begin < sd->end_offset) {
    // normal or straddle
    rval.first = sd;
    rval.second = (sd->end_offset < lsn_end);
  } else {
    // dead zone!
    rval.first = NULL;
  }

done:
  if (rval.first) {
    ASSERT(rval.first->start_offset <= lsn_begin);
    ASSERT(rval.second == (rval.first->end_offset < lsn_end));
  }
  return rval;
}

/* Attempt to install a new segment---successor to sd---that starts at
   lsn_offset. Threads race to call this function if they obtain a log
   block that spills past-end for the current active segment.

   Return the new segment if the given lsn_offset ended up inside it
   (either because we won the race, or because the winner had an
   earlier offset). Otherwise, return NULL.
 */
log_segment_desc *log_segment_mgr::install_segment(log_segment_desc *sd,
                                                   uint64_t lsn_offset) {
  /* We *could* develop some fancy latch-free protocol to install
     new segments, but there's a catch: a truly epic, multi-part,
     Rip van Winkle event could leave multiple threads all trying to
     install different segments to the same slot, unaware that the
     real world has passed them all by. Worse, this leaves open the
     possibility that threads reading from the segment array might
     see one of these too-old values, which would *really* mess
     things up. Rather than risk any of that, we use a mutex and
     rely on the fact that segment changes are literally one in a
     thousand (or million) events, in the worst case: A typical
     update transaction's log footprint is measured in kB, while a
     log segment can hold MB or even GB of data.

     To minimize time spent holding the mutex, allocate space for
     the new segment first (we can always free it we lose the race).
  */
  log_segment_desc *nsd = rcu_alloc();
  DEFER_UNLESS(nsd_installed, rcu_free(nsd));

  /* WARNING: segment_size can change at any time, so we can't
     assume sd was created with same segment_size we have now. We
     also can's safely read it twice in a row here.
   */
  nsd->segment_number = sd->segment_number + 1;
  nsd->start_offset = lsn_offset;
  nsd->end_offset = nsd->start_offset + volatile_read(segment_size);
  nsd->byte_offset = sd->byte_offset + (sd->end_offset - sd->start_offset) +
                     segment_close_size;

  pthread_mutex_lock(&segment_change_mutex);
  DEFER(pthread_mutex_unlock(&segment_change_mutex));

  auto *asd = volatile_read(active_segment);
  if (asd == sd) {
    /* we won the race, install the new segment.

       Write the current segment to its slot so that other threads
       can find it after we install the new one. This has to be
       done before we change the value of active_segment.
    */
    ASSERT(not all_segments[sd->segment_number]);
    all_segments[sd->segment_number] = sd;

    /* Right now, this is our only (meager) protection against a
       wedged log. Once a proper log full protocol is in place,
       this could probably be demoted to a normal assertion.
    */
    THROW_IF(all_segments[nsd->segment_number], log_is_full);
    active_segment = nsd;
    nsd_installed = true;
    return nsd;
  }

  // lost the race, might have landed in dead zone
  return (asd->start_offset < lsn_offset) ? asd : 0;
}
