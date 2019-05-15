#include "sm-log-offset.h"
#include "sm-log-defs.h"

#include "w_rand.h"
#include "stopwatch.h"

#include <vector>
#include <algorithm>
#include <utility>
#include <map>
#include <unistd.h>

using namespace RCU;

static uint64_t const SKIP_LOG_SIZE = MIN_LOG_BLOCK_SIZE;

/* WARNING: nothing in this test removes old segments, so running for
   too long will overflow the last emtpy segment and cause an
   assertion failure. This is not a bug, but rather an omission in the
   test driver.
 */
void doit(size_t nmax, size_t segment_size, bool verbose) {
#define P(msg, ...)                                        \
  do {                                                     \
    if (verbose) fprintf(stderr, msg "\n", ##__VA_ARGS__); \
  } while (0)

  tmp_dir dname;
  sm_log_offset_mgr lm(dname, segment_size);
  uint64_t n = 0;
  uint64_t now = stopwatch_t::now();
  uint32_t seed[] = {uint32_t(now), uint32_t(now >> 32)};
  // uint32_t seed[] = {0x1832ebec, 0x13812bd6};//{0x497aaefc, 0x1380f746};
  w_rand rng(seed);
  w_rand_urng urng = {rng};

  fprintf(stderr, "RNG seed: {0x%08x, 0x%08x}\n", seed[0], seed[1]);

  // lsn_start -> (segment_num, lsn_end)
  typedef std::pair<uint64_t, uint64_t> span;
  typedef sm_log_offset_mgr::segment_id segment_id;
  std::map<uint64_t, segment_id> segments;
  struct keyless {
    bool operator()(span const &a, span const &b) { return a.first < b.first; }
  };
  std::map<span, std::tuple<char const *, size_t, size_t>, keyless> results;

  while (n < nmax) {
    std::vector<span> pending;
    int x = rng.randn(1, 128);
    for (int i = 0; i < x; i++) {
      int sz = align_up(rng.randn(SKIP_LOG_SIZE, 128));
      P("Generate [%zd %zd)", n, n + sz);
      pending.push_back(std::make_pair(n, n + sz));
      n += sz;
    }
    std::shuffle(pending.begin(), pending.end(), urng);
    for (auto x : pending) {
      P("About to assign [%zd %zd)", x.first, x.second);
      auto rval = lm.assign_segment(x.first, x.second);
      if (not rval.sid) {
        // dead zone
        P("=> dead");
        if (verbose) {
          results[x] = std::make_tuple("dead", -1, -1);
        }
        auto it = segments.upper_bound(x.first);
        ASSERT(it == segments.end() or x.second <= it->second.end_offset);
        if (it != segments.begin()) {
          --it;
          ASSERT(it == segments.end() or it->first <= x.first);
        }
      } else {
        auto it = segments.find(rval.sid->start_offset);
        if (it == segments.end())
          it = segments.insert(std::make_pair(rval.sid->start_offset,
                                              *rval.sid)).first;
        else
          ASSERT(rval.sid->segnum == it->second.segnum and
                 rval.sid->end_offset == it->second.end_offset);

        ASSERT(it->second.start_offset <= x.first);
        size_t buf_offset = it->second.buf_offset(x.first);
        if (not rval.full_size) {
          // straddle
          P("=> straddle @%zd nxt=%zd", buf_offset, rval.next_lsn.offset());
          if (verbose) {
            results[x] =
                std::make_tuple("straddle", buf_offset, rval.next_lsn.offset());
          }
          ASSERT(it->second.end_offset < x.second + SKIP_LOG_SIZE);
        } else {
          // normal
          P("=> normal @%zd nxt=%zd", buf_offset, rval.next_lsn.offset());
          if (verbose) {
            results[x] =
                std::make_tuple("normal", buf_offset, rval.next_lsn.offset());
          }
          ASSERT(x.second <= it->second.end_offset);
        }
      }
    }
  }

  for (auto &it : segments) {
    P("Segment %u: [%zd %zd) @%zd", it.second.segnum, it.second.start_offset,
      it.second.end_offset, it.second.byte_offset);
  }

  for (auto it : results) {
    char const *nm;
    size_t here, next;
    std::tie(nm, here, next) = it.second;
    if ((int64_t)here < 0)
      P("[%zd, %zd) -> ---- %s ----", it.first.first, it.first.second, nm);
    else
      P("[%zd, %zd) -> %zd %s %zd", it.first.first, it.first.second, here, nm,
        next);
  }
}
int main() {
  rcu_register();
  rcu_enter();

  try {
    fprintf(stderr, "Begin single-threaded tests\n");
    doit(10 * 1024, 1000, true);
    doit(1024 * 1024, 65536, false);
  } catch (os_error &err) {
    fprintf(stderr, "Yikes! Caught OS error %d: %s", err.err, err.msg);
    exit(-1);
  } catch (log_file_error &err) {
    fprintf(stderr, "Yikes! Log file error: %s", err.msg);
    exit(-1);
  } catch (illegal_argument &err) {
    fprintf(stderr, "Yikes! Illegal argument: %s", err.msg);
    exit(-1);
  } catch (log_is_full &err) {
    fprintf(stderr, "Yikes! Log is full");
    exit(-1);
  }

  rcu_deregister();
}
