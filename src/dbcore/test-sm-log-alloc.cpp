#include "sm-log-alloc.h"

#include "w_rand.h"
#include "stopwatch.h"

#include <unistd.h>

using namespace RCU;

void doit(w_rand &rng) {
  auto no_recover = [](void *, sm_log_scan_mgr *, LSN, LSN)
                        -> void { SPAM("Log recovery is a no-op here\n"); };

  tmp_dir dname;
  sm_log_alloc_mgr lm(dname, 1024, no_recover, NULL, 1 * 1024 * 1024);

  SPAM("Fill up the log...");
  std::vector<log_allocation *> allocations;
  uint64_t start = rng.randn(1111, 9999);
  uint64_t end = start;
  while (lm.cur_lsn_offset() < 1024 * 15) {
    int nrec = rng.randn(1, 10);
    std::vector<int> counts;
    int n = 0;
    for (int i = 0; i < nrec; i++) {
      int m = 2 * rng.randn(1, 4);
      counts.push_back(m);
      n += m;
    }
    auto *x = lm.allocate(nrec, sizeof(end) * n);
    if (not rng.randn(20)) {
      lm.discard(x);
      continue;
    }

    auto *b = x->block;
    uint32_t pend = 0;
    for (auto count : enumerate(counts)) {
      pend += sizeof(end) * count.second;
      b->records[count.first].payload_end = pend;
      b->records[count.first].type = LOG_COMMENT;

      uint64_t *p = (uint64_t *)b->payload(count.first);
      for (int i = 0; i < count.second; i++) p[i] = end++;
    }
    b->checksum = b->full_checksum();

    if (allocations.size() == 16) {
      int victim = rng.randn(allocations.size());
      std::swap(allocations[victim], x);
      lm.release(x);
    } else {
      allocations.push_back(x);
    }
  }

  for (auto *x : allocations) lm.release(x);

  SPAM("Listing contents of  now-full log at %s:", *dname);
  for (char const *fname : dirent_iterator(dname)) SPAM("\t%s", fname);
  SPAM("Listing complete");

  lm.update_durable_mark(lm.cur_lsn_offset());
  SPAM("Listing contents of  now-durable log at %s:", *dname);
  for (char const *fname : dirent_iterator(dname)) SPAM("\t%s", fname);
  SPAM("Listing complete");

  /* Now for the hard part: reading it back...

     First, scan all blocks and verify their checksums. Nrmal
     recovery only checks checksum for blocks after the durable
     mark.

     Then, go back and scan all records and verify that they have
     the correct payload values.
   */
  LSN lsn = lm._lm.get_chkpt_start();
  {
    SPAM("Verify block checksums:");
    sm_log_recover_mgr::block_scanner bscan(&lm._lm, lsn, true, true);
    for (; bscan.valid(); ++bscan) {
      SPAM("\t%zx", bscan->lsn.offset());
      THROW_IF(bscan->checksum != bscan->full_checksum(), log_file_error,
               "Bad checksum found");
    }
  }
  {
    SPAM("Verify log record contents...");
    auto i = start;
    sm_log_recover_mgr::log_scanner scan(&lm._lm, lsn, true);
    for (; scan.valid(); ++scan) {
      uint64_t *elem = scan.payload();
      size_t nelem = scan.payload_size() / sizeof(*elem);
      for (uint32_t j = 0; j < nelem; j++) {
        THROW_IF(i != elem[j], log_file_error,
                 "Unexpected value : %zd (expected %zd)", elem[j], i);
        i++;
      }
    }
    THROW_IF(i != end, log_file_error,
             "Log ended prematurely at %zd (expected %zd)", i, end);
    SPAM("... contents valid");
  }
}

int main() {
  rcu_register();
  rcu_enter();
  DEFER(rcu_exit());

  uint64_t now = stopwatch_t::now();
  uint32_t seed[] = {uint32_t(getpid()), uint32_t(now), uint32_t(now >> 32)};
  // uint32_t seed[] = {0x00002220, 0x518ac930, 0x13818fa1}  ;
  w_rand rng(seed);
  fprintf(stderr, "RNG seed: {0x%08x, 0x%08x, 0x%08x}\n", seed[0], seed[1],
          seed[2]);

  try {
    doit(rng);
  } catch (os_error &err) {
    DIE("Yikes! Caught OS error %d: %s", err.err, err.msg);
  } catch (log_file_error &err) {
    DIE("Yikes! Log file error: %s", err.msg);
  } catch (illegal_argument &err) {
    DIE("Yikes! Illegal argument: %s", err.msg);
  } catch (log_is_full &err) {
    DIE("Yikes! Log is full");
  }
}
