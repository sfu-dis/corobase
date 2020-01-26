#pragma once

#include "sm-common.h"

#include "dynarray.h"

#include <x86intrin.h>
#include <cstdint>
#include <utility>

// used for sparse bitmaps
typedef __v8hi v8hi;

namespace ermia {

struct sparse_bitset {
  static size_t const GAMUT = size_t(1) + UINT16_MAX;
  static size_t const CAPACITY_BYTES = 2 * sizeof(v8hi);
  static size_t const CAPACITY = CAPACITY_BYTES / sizeof(uint16_t);
  static_assert(not(CAPACITY & (CAPACITY - 1)),
                "CAPACITY must be a power of two");

  typedef uint16_t(Array)[CAPACITY];

  v8hi entries[2];
  static_assert(sizeof(Array) == sizeof(entries), "Fix CAPACITY");

  Array &as_array() { return **(Array **)entries; }

  struct iterator {
    uint16_t *entries;
    size_t i;
    uint16_t &operator*() { return entries[i]; }
    bool operator!=(iterator const &other) {
      return entries != other.entries or i != other.i;
    }
    void operator++() {
      if (not entries[++i]) i = CAPACITY;
    }
  };

  iterator begin() { return iterator{as_array(), 0}; }
  iterator end() { return iterator{as_array(), CAPACITY}; }

  /* Initialize the bitset. We have decreed that bitsets may not be
     empty, so initialize the first value.

     NOTE: we actually use uint16_t, but interpret the value as
     signed to keep the compiler happy.
   */
  void init1(int16_t i) {
    entries[0] = v8hi{i};
    entries[1] = v8hi{0};
  }

  /* Insert a value into the map. Return 0 if the insert succeeded
     normally, 1 if the map became full as a result of this insert,
     and -1 if the map was already full.

     WARNING: does *NOT* check for duplicates!
  */
  int insert(uint16_t i) {
#ifndef __clang__
    v8hi rotr_shufmask = {7, 0, 1, 2, 3, 4, 5, 6};
#endif
    if (not entries[0][7]) {
      /* Still working on the first half. We don't care whether
         [i] is zero, the [where] dance does the right thing
         either way.
      */
      int where = entries[0][0] ? 0 : 1;
#ifdef __clang__
      entries[0] = __builtin_shufflevector(entries[0], entries[0], 7, 0, 1, 2, 3, 4, 5, 6);
#else
      entries[0] = __builtin_shuffle(entries[0], rotr_shufmask);
#endif
      entries[0][where] = i;
      return 0;
    }

    // already full?
    if (entries[1][7]) return -1;

    if (not i) {
      // better not already be there!
      ASSERT(entries[0][0]);
      i = entries[0][0];
      entries[0][0] = 0;
    }

    // working on the second half now
#ifdef __clang__
    entries[1] = __builtin_shufflevector(entries[1], entries[1], 7, 0, 1, 2, 3, 4, 5, 6);
#else
    entries[1] = __builtin_shuffle(entries[1], rotr_shufmask);
#endif
    entries[1][0] = i;
    return entries[1][7] ? 1 : 0;
  }

#ifndef __clang__
  bool contains(uint16_t i) {
    // bit 0 is always first, if present.
    if (not i) return not entries[0][0];

    // otherwise, look for element matches
    int rval = __builtin_ia32_pmovmskb128(entries[0] == i);
    rval |= __builtin_ia32_pmovmskb128(entries[1] == i);
    return rval;
  }
  uint16_t size() {
    int a = __builtin_ia32_pmovmskb128(entries[0] != 0);
    a &= 0x5555;
    int b = __builtin_ia32_pmovmskb128(entries[1] != 0);
    b &= 0xaaaa;
    int c = 1;  // first entry is always present
    return __builtin_popcount(a | b | c);
  }
#endif
  void print(char const *name);
};

/* An allocator has (essentially) two jobs, one trivial, and one
   difficult. The easy job is allocating never-before-used objects,
   which can be done using a simple bump allocator. The hard job is
   recycling previously-freed objects, made difficult because we have
   to *find* those previously-freed objects in order to reuse them
   (which implies a need to efficiently track which ones have been
   freed, also a non-trivial task).

   We have here a multi-level allocator that attempts to strike a
   reasonable balance of time and space overheads versus precision
   (ability to recycle previously-freed objects).

   L0 is the bump allocator. It comprises a simple high water mark
   (identifying the highest OID ever allocated) and a capacity mark,
   that indicates how far bump allocation is allowed to proceed before
   the system attempts to reclaim freed OIDs (more on that later).

   L1 captures recently-freed OIDs. It is an array-based stack with
   several thousand entries, where OIDs are pushed on free and popped
   on allocate; the system will always drain L1 before bumping L0, and
   drains half of L1 to L2 each time L1 overflows.

   L2 serves to collect clusters of recently freed OIDs as they
   trickle in from L1. It uses sparse bitsets that can hold up to 16
   OIDs from a 64k-element swaths of the OID space. Swaths are
   assigned bitsets only if they contain at least one OID, and "full"
   bitsets (containing 16 OIDs) are retired to a list that will be
   drained after L1 is empty (and before bumping L0). Depending on the
   workload, L2 capacity ranges between 64k and 1M OIDs, usually with
   a small fraction of those OIDs available for easy re-allocation. L2
   is sized such there is one physical bitset for each swath of the
   OID space, so overflow can only occur if there are at least two
   full bitsets. By definition, L1 must also be full when L2
   overflows, so inconvenient bitsets drain to L3, one at a time, on
   overflow (see policy below). If L1 and L2 are both empty, the
   implementation prefers to scavenge non-full bitsets from L2
   (placing their OIDs into L1) if doing so would fill a significant
   fraction of L1. Otherwise, it prefers to bump L0.

   L3 is the lowest level of the allocator and captures all worst-case
   scenarios that defeat the higher levels. It is a dense bitmap, with
   one bit for every OID. Because the full bitmap is enormous (512MB),
   L3 starts out as small as possible and grows only if it needs to
   set a bit past the current end-of-storage. To minimize L3 growth,
   we prefer to spill non-full L2 entries that would fit in the
   existing L3 storage space. Failing that, we prefer to spill full L2
   entries that fit in the existing L3. Failing that, we spill
   remaining full L2 entries (at which point L2 is guaranteed to no
   longer be full and we stop).

   Although L1 and L2 should usually be effective at recycling most
   OIDs that are freed, a bad case could result in L1 and L2 both
   underflowing (even after scavenging L2) *and* the bump allocator
   being at capacity. At this point, we have two choices (assuming the
   allocator is not yet full-sized): scavenge L3 or increase the
   allocator's size (thus allowing bump allocation again). To prevent
   the allocator growing endlessly, we impose the policy that the
   allocator is only allowed to grow if doing so would make available
   fewer OIDs than could be reclaimed by scavenging L3. Since growing
   the allocator too much at a time would increase the risk of further
   overflows *and* L3 growth, we prefer to increase allocator size by
   small increments (e.g. 25%), which in turn would require us to
   scavenge L3 if more than 25% of all OIDs reside there. Note that,
   where sizes traditionally grow by doubling (increments of 100%),
   growing by increments of 25% is still geometric growth, and so
   allocation cost remains limited to amortized constant time.

   WARNING: this implementation does *not* deal with concurrency
   control at all, nor does it attempt to resize itself as the
   allocator grows. The user of this class is expected to deal with
   those issues, though the allocator provides helper functions to
   simplify resizing (CC should be as simple as grabbing a mutex).
*/

struct sm_allocator {
  /* We store all the "small" bits of allocator state in a separate
     header to simplify offset calculations and object construction.
   */
  struct header_data {
    /* Initialize this header, taking ownership of its own
       storage, which currently is owned by [self].
     */
    header_data(dynarray &&owner);

    // not copyable, not movable
    header_data() = delete;
    header_data(header_data const &) = delete;
    void operator=(header_data) = delete;

    /* The dynarray whose memory this allocator resides inside.
     */
    dynarray backing_store;

    /* This mark tells how high we are allowed to bump the high
       water mark before we have to consider scavenging L3.
    */
    OID capacity_mark;

    /* This mark is higher than any OID that has yet been
       allocated
     */
    OID hiwater_mark;

    /* How many free entries are in [l1] list?
     */
    uint16_t l1_size;

    /* How many full bitmaps are in [l2], ready to use?
     */
    uint32_t l2_size;

    /* What is the index, in [l2], where the first unused bitset
       resides? A value of zero means L2 does not yet exist.
    */
    uint32_t l2_first_unused;

    /* If we are reduced to scavenging L2 for loose OIDs, run an
       incremental sweep all the way across to avoid re-scanning
       the same areas with each allocation. A hand at position 0
       means no scan in progress (since any scan would examine at
       least one entry).
    */
    uint32_t l2_scan_hand;

    /* How many OIDs reside in non-full L2 bitmaps? We normally
       ignore these as "low grade ore", but it may become
       necessary to scavenge them if the allocator reaches full
       size and we're desperate to find usable OIDs.
    */
    uint32_t l2_loose_oids;

    /* How many words of OID bitmaps can L3 currently track? A
       size of zero means L3 does not exist yet (or any more).

       A full-sized L3 is huge (512MB), so we really prefer not to
       instantiate more than necessary. L1 and L2 already help a
       lot (by delaying the creation of L3 in the first place),
       and we can also give priority in L2 to OIDs that would
       cause L3 to grow.
    */
    uint32_t l3_capacity;

    /* How many OIDs reside in L3? We normally ignore this
       low-grade ore, but if the total reaches 25% of the capacity
       mark we have to scavenge L3 rather than increase capacity
       further.
    */
    uint32_t l3_loose_oids;

    /* Similar to the L2 case, where is the current scan hand, in
       case we are reduced to scavenging L3 for loose OIDs?
    */
    uint32_t l3_scan_hand;
  };

  /* Threads don't directly allocate OIDs from the central
     allocator. Instead, they grab a chunk of OIDs from it and then
     consume that chunk at their leisure, thus amortizing the cost
     of hitting the allocator over 32-64 allocations.

     Similarly, threads don't directly deallocate OIDs; instead,
     they queue them up and release them whenever their cache
     overflows.

     The cache is carefully sized to occupy an integer number of 64B
     cache lines.
   */
  struct thread_cache {
    /* When we ask to fill the cache from the central allocator,
       don't try to fill completely. We want some breathing room
       in case a bunch of OIDS suddenly go free, and also to give
       the allocator some flexibility in doling out OIDs.
     */
    enum : size_t { FILL_TARGET = 64 };
    enum : size_t { N = 126 };

    FID f;
    uint32_t nentries;
    OID entries[N];

    thread_cache(FID fid) : f(fid), nentries(0) {}

    uint32_t space_remaining() { return N - nentries; }
  };
  static_assert(is_aligned(sizeof(thread_cache), 64), "Go fix thread_cache::N");

  /* We will allocate things in chunks of 64kB, which should be
     page-aligned on almost all systems. We ignore huge pages here
     because they're not actually well-suited for our uses.
   */
  static size_t const ASSUMED_PAGE_SIZE = 1 << 16;

  /* How many bytes does an L1 allocator occupy? init() expects at
     least this many to be valid.
   */
  static size_t l1_alloc_size() {
    return OFFSETOF(sm_allocator, l1[L1_CAPACITY]);
  }

  /* How many bytes does an L2 allocator occupy? init2() expects at
     least this many to be valid.
   */
  static size_t l2_alloc_size() {
    return OFFSETOF(sm_allocator, l2_maps[L2_CAPACITY]);
  }

  /* How many bytes does an L3 allocator occupy if [end_word] marks
     the larger of its old and new end of storage? init3() expects
     at least this many to be valid.
   */
  static size_t l3_alloc_size(uint32_t end_word = L3_MAX_WORDS) {
    return OFFSETOF(sm_allocator, l3_words[end_word]);
  }

  static size_t max_alloc_size() { return l3_alloc_size(); }

  /* Used by template functions below */
  struct _nop_filter {
    bool operator()(OID) const { return true; }
  };

  /* Create a new, empty allocator. It contains only L0/L1, and
     attempts to access L2 or L3 are likely to fault.
  */
  static sm_allocator *make();

  static void destroy(sm_allocator *alloc);

 private:
  sm_allocator(dynarray &&self);

 public:
  /* Initialize a newly-available L2. L0 and L1 are left as-is, and
     L2 is empty.
   */
  void init_l2();
  bool l2_valid() const { return head.l2_first_unused; }

  /* Initialize L3 or change its size. L0-L2, and any existing parts
     of L3 not removed by this call, are left as-is. Because L3
     grows (and potentially shrinks) over time, this function can be
     called multiple times. The single arg specifies the number of
   */
  void init_l3(OID end_word);
  bool l3_valid() const { return head.l3_capacity; }
  OID l3_end() const { return head.l3_capacity * L3_BITS_PER_WORD; }

  /* How many bytes does the allocator currently use?
   */
  size_t alloc_size();

  /* If we were to grow the allocator, how large should it become?
     The caller would like the delta to be a multiple of [align] for
     space management reasons.

     Return zero if the allocator is full.

     NOTE: increasing capacity does not necessarily increase the
     number of byte the allocator occupies. The latter depends on
     the number of free OIDS and how inconvenient they are to
     record.
   */
  OID propose_capacity(size_t align);

  /* Place up to 64 OIDs in [tc], returning false if fewer than 64
     OIDs were available. Failure may indicate a need to grow or
     scavenge the allocator, or it may be that the allocator is
     truly full.
  */
  bool fill_cache(thread_cache *tc);

  /* Remove up to [target] entries from [tc] (all of them if
     [target]=0), returning false if the allocator was unable to
     absorb all entries.
   */
  bool drain_cache(thread_cache *tc, uint32_t target = 0);

  /* Run some consistency checks to see if there are any obvious
     problems with the current state of this allocator.
   */
  void sanity_check();

  /* Attempt to move [n] OIDs from L1 to [tc], returning the number
     that could not be placed.
   */
  int32_t _fill_from_l1(thread_cache *tc, uint32_t target);

  /* Attempt to drain [n] entries from L1 to L2. Return the number
     of entries that were not drained (positive implies full L2).
   */
  int32_t _drain_l1(uint32_t n);

  /* Scan L2 attempting to reclaim [n] OIDs from full
     bitsets. Whenever a full bitset is found, pass its base OID to
     filter(). If the latter returns false, skip that bitset;
     otherwise pass each OID it contains to sink() and deallocate
     the bitset.

     Return the number of OIDs (out of [n]) that were *not*
     reclaimed.

     WARNING: this function works with bitsets, not OIDS, and so the
     target might be passed by part of a bitset's capacity; the
     amount of overrun will be reported as a negative return value.
   */
  template <typename Filter, typename Sink>
  int32_t _scan_l2(uint32_t n, Filter const &filter, Sink const &sink);

  template <typename Sink>
  int32_t _scan_l2(uint32_t n, Sink const &sink) {
    return _scan_l2(n, _nop_filter(), sink);
  }

  /* Attempt to move at least [n] (and less than
     [n+sparse_bitset::CAPACITY] OIDs from L2 to [tc], returning the
     number that could not be placed.

     WARNING: This function deals only in full bitsets, so [tc] must
     have enough room to absorb a full bitset (minus one) worth of
     OIDs beyond those asked for.
   */
  int32_t _fill_from_l2(thread_cache *tc, uint32_t n);

  /* Scavenge L2 attempting to reclaim [n] loose OIDs. Whenever a
     non-empty bitset is found, pass its base OID to filter(). If
     the latter returns false, skip the bitset; otherwise pass each
     OID it contains to sink() and deallocate the bitset.

     Return the number of OIDs (out of [n]) that were *not*
     reclaimed.

     WARNING: this function works with bitsets, not OIDS, and so the
     target might be passed by part of a bitset's capacity; the
     amount of overrun will be reported as a negative return value.
   */
  template <typename Filter, typename Sink>
  int32_t _scavenge_l2(uint32_t n, Filter const &filter, Sink const &sink);

  template <typename Sink>
  int32_t _scavenge_l2(uint32_t n, Sink const &sink) {
    return _scavenge_l2(n, _nop_filter(), sink);
  }

  /* Scavenge L2 for [n] loose OIDs that can be placed in L1. If at
     least [n] were available, reclaim them and return
     [true]. Otherwise, do nothing and return [false].

     WARNING: All OIDs in a bitset are reclaimed at once, so [n] may
     be passed by part of a bitset's capacity.
  */
  int32_t _scavenge_l2(uint32_t n);

  /* Attempt to drain [n] entries from L2 to L3. Return the number
     of entries that were not drained (so non-zero means L3 could
     not accept them all, probably because L3 needs to grow).

     Return a negative number if the target was overrun (by
     something less than a bitset's capacity).
   */
  int32_t _drain_l2(uint32_t n);

  /* Add an OID to L3
   */
  void _insert_l3(OID x);

  /* Scavenge L3 attempting to reclaim [n] loose OIDs. Whenever a
     non-empty bitset is found, pass its base OID to filter(). If
     the latter returns false, skip the bitset; otherwise pass each
     OID it contains to sink() and deallocate the bitset.

     Return the number of OIDs (out of [n]) that were *not*
     reclaimed.

     WARNING: this function works with dense bitmaps, not OIDS, and
     so the target might be passed by up to L3_BITS_PER_WORD; the
     amount of overrun will be reported as a negative return value.
   */
  template <typename Filter, typename Sink>
  int32_t _scavenge_l3(uint32_t n, Filter const &filter, Sink const &sink);

  template <typename Sink>
  int32_t _scavenge_l3(uint32_t n, Sink const &sink) {
    return _scavenge_l3(n, _nop_filter(), sink);
  }

  /* Scavenge L3 for up to [n] loose OIDs that can be placed in L1
     and L2. Return the number of OIDs that could not be reclaimed.
  */
  int32_t _scavenge_l3(uint32_t n);

  /**********************************************
   * * * * Header (always safe to access) * * * *
   **********************************************/
  static size_t const MAX_CAPACITY_MARK = OID(~OID(0)) - 1;
  header_data head;

  /******************************************
   * * * * L1 (always safe to access) * * * *
   ******************************************/

  static size_t const L1_CAPACITY_BYTES = ASSUMED_PAGE_SIZE - sizeof(head);
  static size_t const L1_CAPACITY = L1_CAPACITY_BYTES / sizeof(OID);

  OID l1[L1_CAPACITY];

  /********************************************************
   * * * * L2 (only safe if header.l2_capacity > 0) * * * *
   ********************************************************/

  /* In theory, we could grow L2 gradually, the way we do for L3,
     but the complexity is just not worth it: L2 only occupies a bit
     more than 2MB anyway.
   */
  static size_t const L2_CAPACITY = size_t(1) + UINT16_MAX;

  /* This array holds full sparse maps that are ready to use. The
     low 16 bits identify which sparse map is involved. Masking off
     the low 16 bits gives the first OID of the span.

     NOTE: we "borrow" the unused space in this array to hold unused
     bitsets. Unused bitsets are stored at the right side of the
     array, with the leftmost (first) residing at []. The fixed
     number of bitsets means there is always room for both (probably
     with room to spare, since most bitsets will be in-use but
     non-full.
   */
  uint32_t l2[L2_CAPACITY];

  /* Each span of 64k OIDs in the OID space can be covered by a
     sparse bitmap. Record here which bitmap a given span is
     assigned (0 if none). Note that this means bitmap 0 is unused.
   */
  uint16_t l2_assignments[L2_CAPACITY];

  /* We break the 32-bit OID space into 64k spans of 64k OIDs
     each. We can thus save space by liberal use of 16-bit
     integers. Note: we allocate a potentially smaller number of
     bitsets to spans on an as-needed basis (see [l2_capacity]).
   */
  sparse_bitset l2_maps[L2_CAPACITY];

  /********************************************************
   * * * * L3 (only safe if header.l3_capacity > 0) * * * *
   ********************************************************/

  static size_t const L3_BITS_PER_WORD = 8 * sizeof(uint64_t);
  static size_t const L3_MAX_WORDS = UINT32_MAX / L3_BITS_PER_WORD + 1;

  /* The bitmap data that comprises L3. Hopefully we never need it,
     but if L2 fails, L3 can capture *anything* an adversary throws
     at it.
   */
  uint64_t l3_words[];
};

//static_assert(sm_allocator::l1_alloc_size() == sm_allocator::ASSUMED_PAGE_SIZE,
//              "Go fix allocator::L1_CAPACITY");
//static_assert(is_aligned(sm_allocator::l2_alloc_size(),
//                         sm_allocator::ASSUMED_PAGE_SIZE),
//              "Something went wrong with L2 sizing");
}  // namespace ermia
