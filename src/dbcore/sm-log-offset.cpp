#include "sm-log-offset.h"

namespace ermia {

/* Convert an LSN into the fat_ptr that can be used to access the
   log record at that LSN. Normally the LSN should point to the
   payload section of a record.
*/
fat_ptr sm_log_offset_mgr::lsn2ptr(LSN lsn, bool is_ext) {
#ifndef NDEBUG
  if (!config::is_backup_srv()) {
    // On backup servers the replay threads could run in parallel
    // with the flusher, and the segments are set up by the daemon
    // concurrently.
    auto *sid = get_segment(lsn.segment());
    ASSERT(sid->contains(lsn));
  }
#endif
  uint64_t offset = lsn.offset();

  int flags = is_ext ? fat_ptr::ASI_EXT_FLAG : fat_ptr::ASI_LOG_FLAG;
  flags |= lsn.flags();
  fat_ptr rval = fat_ptr::make(offset, lsn.size_code(), flags);
  ASSERT(rval.asi_type() == (is_ext ? fat_ptr::ASI_EXT : fat_ptr::ASI_LOG));
  return rval;
}

/* Convert a fat_ptr into the LSN it corresponds to.

   Throw illegal_argument if the pointer does not correspond to
   any LSN.
*/
LSN sm_log_offset_mgr::ptr2lsn(fat_ptr ptr) {
  auto atype = ptr.asi_type();
  THROW_IF(atype != fat_ptr::ASI_LOG and atype != fat_ptr::ASI_EXT,
           illegal_argument, "This fat_ptr does not reference the log");

  int segnum = ptr.log_segment();
  THROW_IF(segnum < 0, illegal_argument,
           "This fat_ptr does not reference the log");

  auto *sid = get_segment(segnum);
  uintptr_t offset = sid->start_offset;
  return LSN::make(offset, segnum, ptr.size_code());
}

/* Look up the segment_id that corresponds to [mod_segnum].

   This takes at most three loads: first, check the segment array. If
   the entry is NULL, check the active segment. If a mismatch, then
   the desired segment is guaranteed to now be present in the segment
   array, if it exists at all.

   The active segment leads to expected O(1) cost in assigning an LSN
   offset to a segment, while guaranteeing correct results even during
   segment change races.

   At a segment change, all threads racing to open a new segment will
   copy the old active segment to its proper slot before joining the
   race. That way, fallbacks from failed active segment checks are
   guaranteed to succeed.

   The winner of the race places the new segment's pointer in
   [active_segment], which is exactly where we look if we find a NULL
   entry in all_segments (being careful to verify in those cases that
   the active_segment found has a compatible segment_number; if not,
   retry with all_segments[]).
*/
segment_id *sm_log_offset_mgr::get_segment(uint32_t mod_segnum) {
  ASSERT(mod_segnum < NUM_LOG_SEGMENTS);
  auto &slot = segments[mod_segnum];
  segment_id *sid = volatile_read(slot);
  if (not sid) {
    /* No luck, try the active segment.

       If we a segment change in just the wrong way, the
       active_segment might not match either (because gets updated
       just before we read it. However, the array is guaranteed to
       contain the correct entry at that point, because the
       active_segment cannot change a second time until the first
       has propagated fully.

       It's conceivable that an epic Rip van Winkle event could
       cause a requesting thread to acquire the segment
       x+NUM_LOG_SEGMENTS rather than segment x as
       expected. However, the caller can detect that problem
       easily because the segment will have wrong offsets. It can
       also be avoided entirely by requiring an RCU quiescent
       point to pass between making the oldest log segment
       unreachable and destroying it (because then we know no
       threads are trying to access it).
    */
    sid = _newest_segment();
    if (mod_segnum != sid->segnum % NUM_LOG_SEGMENTS) {
      sid = volatile_read(slot);
      ASSERT(sid);
      // fall out
    }

    // fall out
  }

  ASSERT(mod_segnum == sid->segnum % NUM_LOG_SEGMENTS);
  return sid;
}

segment_id *sm_log_offset_mgr::get_offset_segment(uint64_t lsn_offset) {
  for (auto *sid = _newest_segment(); sid;
       sid = volatile_read(segments[sid->segnum - 1])) {
    if (sid->contains(lsn_offset)) return sid;
  }

  return 0;
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
   record) so that recovery continues into the new segment rather than
   truncating the log. Block C lost the race to install a new segment,
   and ended up in the "dead zone" between the two segments; that
   block does not map to any physical location in the log and must be
   discarded. Block D won the race to install the new segment, and
   thus becomes the first block of the new segment. Block E lost the
   segment-change race, but was lucky to have a predecessor win. It
   becomes a valid block in the new segment once the dust settles.

*/
sm_log_offset_mgr::segment_assignment sm_log_offset_mgr::assign_segment(
    uint64_t lsn_begin, uint64_t lsn_end) {
  ASSERT(lsn_begin < lsn_end);
  segment_id *right = NULL;
  segment_id *left = _newest_segment();
  LSN next_lsn = INVALID_LSN;
  bool full_size = true;
  for (int i = 0; lsn_end <= left->start_offset; i++) {
    /* Fully before this segment. Back up.

       NOTE: a truly epic Rip van Winkle event could have left us
       holding a range in the dead zone before what has since
       become the oldest segment; the range can't be earlier than
       that because our node in the log list prevents the log from
       becoming durable beyond [lsn_end] (and therefore definitely
       not reclaimable).

       If this occurs, no further action is required at this
       level. Caller should discard its log node and retry.
     */
    right = left;
    left = segments[right->segnum - 1];
    if (not left or i == NUM_LOG_SEGMENTS) {
      left = NULL;
      goto done;
    }
  }

  /* We know [left] starts before we end. We enforce the rule that
     the start of every segment is also the start of a valid log
     record, so any range that ends in a segment also starts there.

     However, we *could* run off the end of [left].

     WARNING: if we start *exactly* MIN_LOG_BLOCK_SIZE bytes before
     the end of the segment, our predecessor did *not* close the
     segment and we will have to do so.
   */
  ASSERT(left->start_offset < lsn_end);
  ASSERT(left->start_offset <= lsn_begin);
  if (lsn_begin + MIN_LOG_BLOCK_SIZE <= left->end_offset) {
    /* Range starts in [left] */

    if (lsn_end + MIN_LOG_BLOCK_SIZE <= left->end_offset) {
      /* Fully contained by [left], with room to spare */
      next_lsn = left->make_lsn(lsn_end);
      goto done;
    }

    /* Need [right] to compute [next_lsn], if we didn't have it
       already; try to install [lsn_end] as the new segment start
       so we don't create a dead zone.

       Meanwhile, the range may or may not fit, but we can
       determine that by examining [left].
    */
    if (not right) {
      /*
        NOTE: a truly malicious Rip van Winkle event could
        conspire to delay an entire segment worth of requests,
        such that we end up past-end for a segment that was only
        installed after we looked for it. It's a very narrow
        window: if we arrived any sooner, they would all end up
        in a dead zone, and if we arrived later, we would see
        the new segment.

        If that happens, no harm done; just start over.
      */
      right = _install_segment(left, lsn_end);
      if (right->end_offset < lsn_end + MIN_LOG_BLOCK_SIZE)
        return assign_segment(lsn_begin, lsn_end);
    }

    next_lsn = right->make_lsn(right->start_offset);
    full_size = (lsn_end <= left->end_offset);
    goto done;
  }

  /* We do not overlap [left] at all. We're either in the dead zone
     between [left] and [right], or fully contained in [right]. The
     latter is only possible if [left] is the active segment (or was
     when we last checked), and the race to install a new segment
     has a favorable outcome. Either way, we need to install a new
     segment if [right] is NULL; try to install [lsn_start] as the
     new segment so we don't land in the dead zone.
   */
  ASSERT(left->end_offset <= lsn_begin + MIN_LOG_BLOCK_SIZE);
  if (not right) {
    /* Might be fully inside [right]. Dead zone, otherwise.

       Watch out for Rip van Winkle! (see comments above)
     */
    right = _install_segment(left, lsn_begin);
    if (right->start_offset <= lsn_begin) {
      if (right->end_offset < lsn_end + MIN_LOG_BLOCK_SIZE)
        return assign_segment(lsn_begin, lsn_end);

      /* Fully contained in [right], with room to spare */
      left = right;
      next_lsn = left->make_lsn(lsn_end);
      goto done;
    }

    // fall out
  }

  /* Dead zone */
  ASSERT(left->end_offset <= lsn_end + MIN_LOG_BLOCK_SIZE);
  ASSERT(lsn_end <= right->start_offset);
  left = NULL;

done:
  if (left) {
    ASSERT(left->start_offset <= lsn_begin);
    ASSERT(lsn_begin + MIN_LOG_BLOCK_SIZE <= left->end_offset);
  }

  return segment_assignment{left, next_lsn, full_size};
}

/* Attempt to install a new segment---successor to sd---that starts at
   lsn_offset. Threads race to call this function if they obtain a log
   block that spills past-end for the current active segment.

   Return the new segment if the given lsn_offset ended up inside it
   (either because we won the race, or because the winner had an
   earlier offset). Otherwise, return NULL.
 */
segment_id *sm_log_offset_mgr::_install_segment(segment_id *sid,
                                                uint64_t lsn_offset) {
  segment_id *nsid = prepare_new_segment(lsn_offset);
  if (nsid and create_segment(nsid)) return nsid;

  nsid = get_segment((sid->segnum + 1) % NUM_LOG_SEGMENTS);
  ASSERT(nsid and nsid->segnum == sid->segnum + 1);
  return nsid;
}
}  // namespace ermia
