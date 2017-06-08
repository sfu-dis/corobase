#include "sm-oid-alloc-impl.h"

#include "dynarray.h"

#include <cstdio>

void sparse_bitset::print(char const *name) {
  printf("%s = {%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d}\n", name,
         entries[0][0], entries[0][1], entries[0][2], entries[0][3],
         entries[0][4], entries[0][5], entries[0][6], entries[0][7],
         entries[1][0], entries[1][1], entries[1][2], entries[1][3],
         entries[1][4], entries[1][5], entries[1][6], entries[1][7]);
}

#define LOG(msg, ...) fprintf(stderr, msg "\n", ##__VA_ARGS__)

void test_bitset() {
  LOG("****************************************\n"
      "* * * Testing sparse bitsets * * *\n"
      "****************************************");

  sparse_bitset s1 = {{{0, 1, 2, 3, 4, 5, 6, 7}, {8, 9, 10, 11, 12, 13, 0, 0}}};
  sparse_bitset s2 = {
      {{10, 11, 12, 13, 14, 15, 16, 17}, {18, 19, 20, 21, 22, 23, 0, 0}}};
  sparse_bitset s3 = {0};
  int x[] = {0, 1, 5, 35};
  ASSERT(s1.size() == 14);
  ASSERT(s2.size() == 14);
  ASSERT(s3.size() == 1);
  LOG("s1 has %d entries", s1.size());
  LOG("s2 has %d entries", s2.size());
  LOG("s3 has %d entries", s3.size());
  for (auto i : x) {
    LOG("s1 %s %d", s1.contains(i) ? "contains" : "does NOT contain", i);
    LOG("s2 %s %d", s2.contains(i) ? "contains" : "does NOT contain", i);
  }

  // test overflow
  int y[] = {1, 2, 3, 4, 5, 6, 7};
  for (auto i : y) {
    int rval = s3.insert(i);
    ASSERT(not rval and s3.contains(i));
  }

  int z[] = {8, 9, 10, 11, 12, 13, 14};
  for (auto i : z) {
    int rval = s3.insert(i);
    ASSERT(not rval and s3.contains(i));
  }
  int rval;
  rval = s3.insert(15);
  ASSERT(rval == 1 and s3.contains(15));
  rval = s3.insert(16);
  ASSERT(rval == -1 and not s3.contains(16));

  // just to be sure we didn't lose anything
  for (auto i : y) ASSERT(s3.contains(i));
  for (auto i : z) ASSERT(s3.contains(i));
  ASSERT(s3.contains(15));

  s1.print("s1");
  s2.print("s2");
  s3.print("s3");
}

int main() {
  test_bitset();

  LOG("****************************************\n"
      "* * * Testing allocator routines * * *\n"
      "****************************************");

  bool rval;
  auto *alloc = sm_allocator::make();
  DEFER(sm_allocator::destroy(alloc));

  auto const FILL_TARGET = sm_allocator::thread_cache::FILL_TARGET;
  sm_allocator::thread_cache tc = {0};

  LOG("allocating the first batch of values");
  rval = alloc->fill_cache(&tc);
  ASSERT(rval and tc.nentries == FILL_TARGET);
  for (size_t i = 0; i < tc.nentries; i++) ASSERT(tc.entries[i] == i);
  alloc->sanity_check();

  LOG("freeing the first batch");
  rval = alloc->drain_cache(&tc);
  ASSERT(rval and not tc.nentries and alloc->head.l1_size == FILL_TARGET);
  alloc->sanity_check();

  LOG("reallocating that batch");
  rval = alloc->fill_cache(&tc);
  ASSERT(rval and tc.nentries == FILL_TARGET);
  for (size_t i = 0; i < tc.nentries; i++) ASSERT(tc.entries[i] == i);
  alloc->sanity_check();

  LOG("using part of the batch and then asking for more");
  size_t delta = 20;
  auto old_nentries = tc.nentries -= delta;
  rval = alloc->fill_cache(&tc);
  ASSERT(rval and tc.nentries == old_nentries + FILL_TARGET);
  for (size_t i = old_nentries; i < tc.nentries; i++)
    ASSERT(tc.entries[i] == i + delta);
  alloc->sanity_check();

  LOG("freeing a small part of the cache");
  tc.nentries = delta;
  rval = alloc->drain_cache(&tc);
  ASSERT(rval and not tc.nentries and alloc->head.l1_size == delta);

  // move the high water mark so we can explore corner cases
  LOG("empty L0 and alloc a batch that underflows L1");
  alloc->head.hiwater_mark = alloc->head.capacity_mark;
  tc.nentries = 0;
  rval = alloc->fill_cache(&tc);
  ASSERT(not rval and tc.nentries == delta);
  for (size_t i = 0; i < delta; i++) ASSERT(tc.entries[i] == i);

  LOG("free enough batches to overflow L1");
  OID i = 0;
  tc.nentries = 0;
  while (1) {
    if (tc.nentries == FILL_TARGET) {
      alloc->drain_cache(&tc);
      if (alloc->l2_valid()) break;

      ASSERT(not tc.nentries);
    }
    tc.entries[tc.nentries++] = i++;
  }
  ASSERT(not tc.nentries);
  ASSERT(i ==
         (alloc->head.l1_size + alloc->head.l2_size * sparse_bitset::CAPACITY +
          alloc->head.l2_loose_oids));

  LOG("alloc enough OIDs to consume all full bitsets in L2");
  while (1) {
    rval = alloc->fill_cache(&tc);
    tc.nentries = 0;  // "use" them
    if (not rval) break;
  }
  ASSERT(not alloc->head.l1_size);
  ASSERT(not alloc->head.l2_size);
  alloc->sanity_check();
  auto tmp = alloc->head.l2_loose_oids;
  LOG("-> %d loose OIDs left in L2", tmp);

  LOG("scavenge loose OIDs in L2");
  auto left = alloc->_scavenge_l2(tmp);
  ASSERT(left <= 0);
  ASSERT(not alloc->head.l2_loose_oids);
  alloc->sanity_check();

  LOG("now allocate those previously-loose OIDs");
  rval = alloc->fill_cache(&tc);
  ASSERT(rval or tmp < FILL_TARGET);
  ASSERT(tc.nentries == tmp);
  alloc->sanity_check();

  // make a clean slate
  tc.nentries = 0;
  sm_allocator::destroy(alloc);
  alloc = sm_allocator::make();
  alloc->head.hiwater_mark = alloc->head.capacity_mark;

  alloc->sanity_check();

  LOG("free some OIDs into L2 and then drain to L3");
  // first, put several entries in each valid swath
  auto nswath = alloc->head.hiwater_mark / sparse_bitset::GAMUT;
  auto ndumps = 0;
  for (int i = 0; ndumps < 1024; i++) {
    auto base = sparse_bitset::GAMUT * (i % nswath);
    tc.entries[tc.nentries++] = base + i / nswath;
    if (not tc.space_remaining()) {
      rval = alloc->drain_cache(&tc);
      ASSERT(rval and not tc.nentries);
      ndumps++;
    }
  }
  ASSERT(not tc.nentries);
  alloc->sanity_check();
  left = alloc->_drain_l1(alloc->head.l1_size);
  ASSERT(left <= 0);
  left = alloc->_drain_l2(1 << 16);
  ASSERT(left + alloc->head.l3_loose_oids == (1 << 16));
  alloc->sanity_check();

  LOG("reclaim the OIDs from L3");
  left = alloc->_scavenge_l3(alloc->head.l3_loose_oids);
  ASSERT(not left and not alloc->head.l3_loose_oids);
  alloc->sanity_check();

  LOG("Allocate OIDs until we need to bump");
  size_t count = 0;
  while (1) {
    auto rval = alloc->fill_cache(&tc);
    count += tc.nentries;
    tc.nentries = 0;
    if (not rval) break;
  }
  LOG("-> %zd OIDs allocated (%u loose remain)", count,
      alloc->head.l2_loose_oids + alloc->head.l3_loose_oids);

  LOG("Bump the allocator and allocate some more");
  alloc->head.capacity_mark = alloc->propose_capacity(1);
  count = 0;
  while (1) {
    auto rval = alloc->fill_cache(&tc);
    count += tc.nentries;
    tc.nentries = 0;
    if (not rval) break;
  }
  LOG("-> %zd OIDs allocated", count);
  ASSERT(alloc->head.hiwater_mark == alloc->head.capacity_mark);
  ASSERT(not alloc->head.l1_size);
  ASSERT(not alloc->head.l2_size);
}
