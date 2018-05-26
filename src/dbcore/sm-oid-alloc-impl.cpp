#include <vector>
#include <algorithm>

#include "sm-oid-alloc-impl.h"

namespace ermia {

sm_allocator *sm_allocator::make() {
  dynarray d(l3_alloc_size(), l1_alloc_size());
  void *ptr = d.data();
  return new (ptr) sm_allocator(std::move(d));
}

void sm_allocator::destroy(sm_allocator *alloc) { alloc->~sm_allocator(); }

sm_allocator::header_data::header_data(dynarray &&self)
    : backing_store(std::move(self)),
      capacity_mark(1 * 1024 * 1024),
      hiwater_mark(0),
      l1_size(0),
      l2_size(0xdead),
      l2_first_unused(0),
      l2_scan_hand(0xdead),
      l2_loose_oids(0xdeadbeef),
      l3_capacity(0),
      l3_loose_oids(0xdeadbeef),
      l3_scan_hand(0xdeadbeef) {
  /* ^^^

     Put canary values 0xdead and 0xdeadbeef in variables that
     should not be accessed yet (have to init L2 and L3 first).

     As for the capacity mark, there's no point trying to scavenge
     loose OIDs until we have a reasonable chance of filling L1 and
     L2 as a result. Those two have a combined capacity that ranges
     from a worst case of 128k OIDs up to a best case of more than
     1M OIDs. Let's go out on a limb and decree that 512k OIDs is a
     normal capacity for L1/L2. We don't want to scavenge and then
     overflow right after, so only try to reclaim enough OIDs to
     fill L1/L2 halfway. That drops us down to 256k. Since we like
     to grow by 25% at a time, that means we need to multiply that
     by 4, resulting in an initial cap of 1M OIDs.

     NOTE: this does *not* mean we will allocate all 2M OIDs before
     attempting to recycle any of them. We actually recycle OIDs
     quite aggressively at all times. Rather, this number means we
     won't even try to "mine" low-grade ore until we have a
     reasonable chance of recovering a significant number of OIDs
     (lest we find ourselves doing it again too soon afterward). We
     expect scavenging to be very rare in all but the most
     pathological cases, so even as conservative as we're being, the
     capacity will probably still have to increase many times before
     we actually scavenge L3.
   */

  // make sure this is *our* dynarray and not some other
  ASSERT(this == (void *)backing_store.data());
}

sm_allocator::sm_allocator(dynarray &&self) : head(std::move(self)) {}

void sm_allocator::init_l2() {
  ASSERT(head.l2_size == 0xdead);
  ASSERT(head.l2_scan_hand == 0xdead);
  ASSERT(head.l2_loose_oids == 0xdeadbeef);
  head.backing_store.ensure_size(l2_alloc_size());

  // no assignments at first
  objzero(l2_assignments);

  /* Initialize the bitmaps to garbage so we notice if they get used
     improperly. Also, gives "fail-fast" behavior if the bytes
     aren't actually accessible.
   */
  for (size_t i = 0; i < L2_CAPACITY; i++) {
    // unused bitmap list
    l2[i] = i;

// initialize to garbage
#ifndef NDEBUG
    l2_maps[i] =
        sparse_bitset{{{9191, 8282, 7373, 6464, 4646, 3737, 2828, 1919}}};
#endif
  }

  // seed the unused bitmap list
  head.l2_size = 0;
  head.l2_first_unused = 1;
  head.l2_scan_hand = 0;
  head.l2_loose_oids = 0;
}

void sm_allocator::init_l3(uint32_t end_word) {
  /* Something is wrong if we allocate L3 space for OIDs
     significantly beyond the high water mark.
   */
  ASSERT(end_word * L3_BITS_PER_WORD <=
         align_up(head.hiwater_mark, L3_BITS_PER_WORD));
  if (head.l3_capacity < end_word) {
    head.backing_store.ensure_size(l3_alloc_size(end_word));
    if (not head.l3_capacity) {
      // allocating a new L3
      ASSERT(head.l3_loose_oids == 0xdeadbeef);
      ASSERT(head.l3_scan_hand == 0xdeadbeef);
      head.l3_loose_oids = 0;
      head.l3_scan_hand = 0;
    } else {
      // growing L3
    }

    /* Clear out the new space. It is likely all zeros, but we'd
       face disaster if that assumption turned out to be wrong.
     */
    objzero(&l3_words[head.l3_capacity], &l3_words[end_word]);
  } else if (head.l3_capacity > end_word) {
    /* Make sure the space to be discarded is empty, lest we allow
       a buggy caller to permanently leak OIDs.
     */
    for (size_t i = end_word; i < head.l3_capacity; i++)
      ASSERT(not l3_words[i]);

    head.backing_store.truncate(l3_alloc_size(end_word));
    if (not end_word) {
      // deallocating L3
      head.l3_loose_oids = 0xdeadbeef;
      head.l3_scan_hand = 0xdeadbeef;
    } else {
      // shrinking L3
      if (head.l3_scan_hand >= head.l3_capacity) head.l3_scan_hand = 0;
    }
  }

  head.l3_capacity = end_word;
}

size_t sm_allocator::alloc_size() {
  if (l3_valid()) return l3_alloc_size(head.l3_capacity);

  return l2_valid() ? l2_alloc_size() : l1_alloc_size();
}

OID sm_allocator::propose_capacity(size_t align) {
  auto pc = min(5 * head.capacity_mark / 4, MAX_CAPACITY_MARK);
  auto delta = align_down(pc - head.capacity_mark, align);
  return delta ? head.capacity_mark + delta : 0;
}

bool sm_allocator::fill_cache(thread_cache *tc) {
  // careful of overrun
  auto target = min(thread_cache::FILL_TARGET,
                    tc->space_remaining() - sparse_bitset::CAPACITY);

  // start with L1
  if (head.l1_size) {
  drain_l1:
    auto left = _fill_from_l1(tc, target);
    if (left <= 0) return true;

    target = left;
  }

  // safe to access L2? also worthwhile?
  if (l2_valid() and head.l2_size) {
    auto left = _fill_from_l2(tc, target);
    if (left <= 0) return true;

    target = left;
  }

  // fall back to the bump allocator
  while (target and head.hiwater_mark < head.capacity_mark) {
    tc->entries[tc->nentries++] = head.hiwater_mark++;
    target--;
  }

  // scavenge L2/L3?
  auto capacity_bump = propose_capacity(1) - head.capacity_mark;
  if (head.l2_loose_oids + head.l3_loose_oids >= capacity_bump) {
    if (head.l2_loose_oids) {
      auto left = _scavenge_l2(L1_CAPACITY / 2);
      if (left > 0 and head.l3_loose_oids) left = _scavenge_l3(left);

      if (head.l1_size) goto drain_l1;
    }
  }

  /* It would be very easy to bump capacity locally. However,
     increasing capacity usually implies resizing other resources
     (like the OID array this allocator manages), so we never do it
     automatically.
   */

  // if it didn't work by now, give up
  return not target;
}

bool sm_allocator::drain_cache(thread_cache *tc, uint32_t target) {
  if (not target) target = tc->nentries;

  // do we need to drain L1? is there an L2 to drain to?
  if (head.l1_size + tc->nentries > L1_CAPACITY) _drain_l1(L1_CAPACITY / 2);

  // drain to L1 (all, unless L1 and L2 were both full)
  auto l1_target = min(target, L1_CAPACITY - head.l1_size);
#ifndef NDEBUG
  for (size_t i = 0; i < tc->nentries; i++)
    ASSERT(tc->entries[i] < head.hiwater_mark);
#endif
  tc->nentries -= l1_target;
  objcopy(&l1[head.l1_size], &tc->entries[tc->nentries], l1_target);
  head.l1_size += l1_target;
  target -= l1_target;
  return not target;
}

int32_t sm_allocator::_fill_from_l1(thread_cache *tc, uint32_t target) {
  ASSERT(target <= tc->space_remaining());
  auto l1_target = min(target, head.l1_size);
  if (l1_target) {
    head.l1_size -= l1_target;
    target -= l1_target;
    objcopy(&tc->entries[tc->nentries], &l1[head.l1_size], l1_target);
    tc->nentries += l1_target;
  }
  return target;
}

int32_t sm_allocator::_drain_l1(uint32_t target) {
  ASSERT(target <= head.l1_size);
  if (not l2_valid()) init_l2();

  // adjust size and remember it
  auto base = (head.l1_size -= target);
  for (size_t i = 0; i < target; i++) {
    auto x = l1[base + i];
    auto &bidx = l2_assignments[x >> 16];
    if (bidx) {
      // add an entry to the swath
      head.l2_loose_oids++;
      ASSERT(not l2_maps[bidx].contains(x));
      if (int rval = l2_maps[bidx].insert(x)) {
        /* Became full! Move the map to the ready list. We can
           never overflow that list, because it has room for
           all bitmaps we can possibly allocate. In fact, it
           can never even fill, because bitmap 0 is unused.
        */
        ASSERT(rval == 1);
        l2[head.l2_size++] = (x & ~0xffff) | bidx;
        ASSERT(head.l2_size < L2_CAPACITY);
        bidx = 0;
        head.l2_loose_oids -= sparse_bitset::CAPACITY;
      }
    } else {
      // swath has no bitmap
      if (head.l2_first_unused == L2_CAPACITY) {
        /* No more bitmaps available, skip this OID and hope
           we can drain enough other OIDs to meet target.
        */
        l1[head.l1_size++] = x;
        continue;
      }

      // assign an unused bitmap to the span and initialize it
      bidx = (uint16_t)l2[head.l2_first_unused++];
      l2_maps[bidx].init1(x);
      head.l2_loose_oids++;
    }
  }

  return head.l1_size - base;
}

template <typename Filter, typename Sink>
int32_t sm_allocator::_scan_l2(uint32_t n, Filter const &f, Sink const &s) {
  /* This scan maintains two cursors: one, starting from the front,
     below which we store bitsets that were rejected by the
     filter. The other, starting from the end, is the current size
     of L2. The scan ends when the target is met, or the cursors
     meet, whichever comes first.
   */
  int32_t target = n;
  size_t end_rejects = 0;
  while (target > 0 and end_rejects < head.l2_size) {
    uint32_t &x = l2[head.l2_size - 1];
    OID base = x & ~0xffff;
    if (f(base)) {
      // accepted -- sink OIDs and deallocate bitmap
      uint16_t bdx = x;
      for (auto offset : l2_maps[bdx].as_array()) s(base + offset);

      head.l2_size--;
      target -= sparse_bitset::CAPACITY;
      l2[--head.l2_first_unused] = bdx;
    } else {
      // rejected -- swap out with alternate and retry
      std::swap(x, l2[end_rejects]);
      end_rejects++;
    }
  }

  return target;
}

int32_t sm_allocator::_fill_from_l2(thread_cache *tc, uint32_t target) {
  // safe to access L2? also worthwhile?
  if (not l2_valid() or not head.l2_size) return target;

  auto sink = [&](OID x) { tc->entries[tc->nentries++] = x; };

  return _scan_l2(target, sink);
}

template <typename Filter, typename Sink>
int32_t sm_allocator::_scavenge_l2(uint32_t n, Filter const &f, Sink const &s) {
  // reclaim OIDs until we reach our target or complete a full scan.
  size_t i = 0;
  int32_t target = n;
  for (; target > 0 and i < L2_CAPACITY; i++) {
    // use wraparound in idx to stay in array bounds
    uint16_t idx = head.l2_scan_hand + i;
    auto &bdx = l2_assignments[idx];
    OID base = OID(idx) << 16;
    if (not bdx or not f(base)) continue;

    for (auto x : l2_maps[bdx]) {
      s(base + x);
      head.l2_loose_oids--;
      target--;
    }

    // unassign and deallocate the bitmap
    l2[--head.l2_first_unused] = bdx;
    bdx = 0;
  }

  head.l2_scan_hand += i;
  return target;
}

int32_t sm_allocator::_scavenge_l2(uint32_t target) {
  ASSERT(target + sparse_bitset::CAPACITY <= L1_CAPACITY - head.l1_size);

  // does L2 even exist? with enough OIDS to be worth reclaiming?
  if (not l2_valid() or head.l2_loose_oids < target) return target;

  auto sink = [&](OID x) { l1[head.l1_size++] = x; };

  auto left = _scavenge_l2(target, sink);
  ASSERT(left <= 0);
  return left;
}

int32_t sm_allocator::_drain_l2(uint32_t n) {
  // only try if L3 actually exists
  if (not l3_valid()) {
    /* start out modest, 16M OIDs; that's 16x more OIDS than L2
       can possibly hold, and the additional 2MB doesn't quite
       double the size of our allocator. Besides, it can always
       grow later if need be.
     */
    auto count = min(16 * 1024 * 1024, head.capacity_mark);
    init_l3(count / L3_BITS_PER_WORD);
  }

  // the policy is always the same
  OID biggest = 0;
  auto filter = [&](OID x) {
    x += sparse_bitset::GAMUT;
    if (biggest < x) biggest = x;
    return x <= l3_end();
  };
  auto sink = [&](OID x) { _insert_l3(x); };

  // First preference: non-full bitmaps that fit in L3
  {
    auto left = _scavenge_l2(n, filter, sink);
    if (left <= 0) return 0;
    n = left;
  }

  // Second preference: full bitmaps that fit in L3
  {
    biggest = 0;
    auto left = _scan_l2(n, filter, sink);
    if (left <= 0) return 0;
    n = left;
  }

  // Third preference: grow L3 and take any full bitmaps.
  if (biggest) {
    init_l3(biggest / L3_BITS_PER_WORD);
    auto left = _scan_l2(n, sink);
    if (left <= 0) return 0;
    n = left;
  }

  return n;
}

void sm_allocator::_insert_l3(OID x) {
  ASSERT(x < l3_end());
  auto &word = l3_words[x / L3_BITS_PER_WORD];
  auto bit = x % L3_BITS_PER_WORD;
  word |= uint64_t(1) << bit;
  head.l3_loose_oids++;
}

template <typename Filter, typename Sink>
int32_t sm_allocator::_scavenge_l3(uint32_t n, Filter const &f, Sink const &s) {
  size_t sentinel = head.l3_scan_hand;
  int32_t target = n;
  while (target > 0) {
    OID base = head.l3_scan_hand * L3_BITS_PER_WORD;
    auto &word = l3_words[head.l3_scan_hand++];
    if (word and f(base)) {
      while (1) {
        auto bit = __builtin_ctzll(word);
        s(base + bit);
        target--;
        head.l3_loose_oids--;
        word &= (word - 1);
        if (not word) break;
      }
    }

    // wraparound?
    if (head.l3_scan_hand >= head.l3_capacity) head.l3_scan_hand = 0;

    // scan complete?
    if (head.l3_scan_hand == sentinel) break;
  }

  return target;
}

int32_t sm_allocator::_scavenge_l3(uint32_t n) {
  if (not l3_valid()) return 0;

  auto l1_has_space =
      [&]() { return head.l1_size + thread_cache::N <= L1_CAPACITY; };

  /* Actually, we couldn't care less what OID it is... but we need
     to stop if we reach our target, or if we can't guarantee room
     for another batch in L1.
   */
  thread_cache tc = {0};
  auto filter = [&](OID) {
    // can we absorb a full bitmap word?
    if (tc.space_remaining() < L3_BITS_PER_WORD) {
      auto rval = drain_cache(&tc);
      ASSERT(rval and not tc.nentries);

      // can L1 absorb another full thread cache?
      if (not l1_has_space()) {
        // try to drain L1?
        _drain_l1(L1_CAPACITY / 2);
        if (not l1_has_space()) return false;
      }
    }
    return true;
  };

  auto sink = [&](OID x) {
    ASSERT(tc.space_remaining());
    tc.entries[tc.nentries++] = x;
  };

  return _scavenge_l3(n, filter, sink);
}

void sm_allocator::sanity_check() {
  /* First, are there duplicates in L1?
   */
  std::vector<OID> values, values2;
  values.insert(values.end(), l1, l1 + head.l1_size);
  std::sort(values.begin(), values.end());
  {
    auto dup = std::adjacent_find(values.begin(), values.end());
    DIE_IF(dup != values.end(), "Duplicate value found in L1: %u", *dup);
  }

  // all the real fun is in L2, but only if L2 exists
  if (not l2_valid()) return;

  /* Is the same bitset used more than once? */
  std::vector<uint16_t> bitmaps, bitmaps2;
  for (auto it : enumerate(l2_assignments)) {
    OID base = OID(it.first) << 16;
    if (it.second) {
      for (auto offset : l2_maps[it.second]) values2.push_back(base + offset);

      bitmaps.push_back(it.second);
    }
  }
  std::sort(bitmaps.begin(), bitmaps.end());
  {
    auto dup = std::adjacent_find(bitmaps.begin(), bitmaps.end());
    DIE_IF(dup != bitmaps.end(), "One bitmap assigned to 2+ swaths: %u",
           uint32_t(*dup));
  }

  for (size_t i = 0; i < head.l2_size; i++) {
    auto x = l2[i];
    OID base = x & ~0xffff;
    uint16_t bidx = x;
    DIE_IF(not bidx, "Bitmap 0 marked as full");
    bitmaps2.push_back(bidx);
    for (auto offset : l2_maps[bidx]) values2.push_back(base + offset);
  }
  std::sort(bitmaps2.begin(), bitmaps2.end());
  {
    auto dup = std::adjacent_find(bitmaps2.begin(), bitmaps2.end());
    DIE_IF(dup != bitmaps2.end(), "One full bitmap found twice in L2: %u",
           uint32_t(*dup));
  }

  bitmaps.insert(bitmaps.end(), bitmaps2.begin(), bitmaps2.end());
  std::sort(bitmaps.begin(), bitmaps.end());
  {
    auto dup = std::adjacent_find(bitmaps.begin(), bitmaps.end());
    DIE_IF(dup != bitmaps.end(), "Full bitmap still assigned to a swath: %u",
           uint32_t(*dup));
  }

  /* Check the unused bitmaps, also */
  bitmaps2.clear();
  for (size_t i = head.l2_first_unused; i < L2_CAPACITY; i++) {
    uint16_t bidx = l2[i];
    DIE_IF(not bidx, "Bitmap 0 marked as idle");
    bitmaps2.push_back(bidx);
  }
  std::sort(bitmaps2.begin(), bitmaps2.end());
  {
    auto dup = std::adjacent_find(bitmaps2.begin(), bitmaps2.end());
    DIE_IF(dup != bitmaps2.end(), "Same bitmap marked as idle 2+ times: %u",
           uint32_t(*dup));
  }

  bitmaps.insert(bitmaps.end(), bitmaps2.begin(), bitmaps2.end());
  std::sort(bitmaps.begin(), bitmaps.end());
  {
    auto dup = std::adjacent_find(bitmaps.begin(), bitmaps.end());
    DIE_IF(dup != bitmaps.end(), "Same bitmap marked as idle and in use: %u",
           uint32_t(*dup));
  }

  /* Now that we've ruled out the worst abuses of bitmap
     assignments, do those bitmaps contain duplicates in L2 or L1/L2
     combined?
  */
  std::sort(values2.begin(), values2.end());
  {
    auto dup = std::adjacent_find(values2.begin(), values2.end());
    DIE_IF(dup != values2.end(), "Duplicate value found in L2: %u", *dup);
  }
  values.insert(values.end(), values2.begin(), values2.end());
  std::sort(values.begin(), values.end());
  {
    auto dup = std::adjacent_find(values.begin(), values.end());
    DIE_IF(dup != values.end(), "Duplicate value found in L1/L2: %u", *dup);
  }

  /* Last of all, is the highest value actually smaller than the
     high water mark?
   */
  DIE_IF(values.size() and not(values.back() < head.hiwater_mark),
         "Supposedly free OID is past the high water mark");
}
}  // namespace ermia
