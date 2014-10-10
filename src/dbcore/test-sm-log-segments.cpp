#include "sm-log-segments.h"

#include "w_rand.h"
#include "stopwatch.h"

#include <vector>
#include <algorithm>
#include <utility>
#include <map>

static uint64_t const SKIP_LOG_SIZE = 8;

/* WARNING: nothing in this test removes old segments, so running for
   too long will overflow the last emtpy segment and cause an
   assertion failure. This is not a bug, but rather an omission in the
   test driver.
 */
void doit(size_t nmax, size_t segment_size, bool verbose) {
#define P(msg, ...) do { if (verbose) fprintf(stderr, msg "\n", ##__VA_ARGS__); } while (0)

    log_segment_mgr lm(segment_size-SKIP_LOG_SIZE, SKIP_LOG_SIZE);
    uint64_t n = 0;
    uint64_t now = stopwatch_t::now();
    uint32_t seed[] = {uint32_t(now), uint32_t(now>>32)};
    //uint32_t seed[] = {0x705b8d18, 0x137251cb};
    w_rand rng(seed);
    w_rand_urng urng = {rng};

    fprintf(stderr, "RNG seed: {0x%08x, 0x%08x}\n", seed[0], seed[1]);
    
    // lsn_start -> (segment_num, lsn_end)
    typedef std::pair<uint64_t, uint64_t> span;
    struct keysel {
        uint64_t operator()(log_segment_desc const &sd) { return sd.start_offset; }
    };
    std::_Rb_tree<uint64_t, log_segment_desc, keysel,
                  std::less<uint64_t>, std::allocator<log_segment_desc> > segments;
    struct keyless {
        bool operator()(span const &a, span const &b) {
            return a.first < b.first;
        }
    };
    std::map<span, std::pair<char const *, size_t>, keyless> results;

    while (n < nmax) {
        std::vector<span> pending;
        int x = rng.randn(1, 128);
        for (int i=0; i < x; i++) {
            int sz = rng.randn(SKIP_LOG_SIZE, 128);
            P("Generate [%zd %zd)", n, n+sz);
            pending.push_back(std::make_pair(n, n+sz));
            n += sz;
        }
        std::shuffle(pending.begin(), pending.end(), urng);
        for (auto x : pending) {
            P("About to assign [%zd %zd)", x.first, x.second);
            auto rval = lm.assign_segment(x.first, x.second);
            if (not rval.first) {
                // dead zone
                P("=> dead");
                if (verbose) { results[x] = std::make_pair("dead", -1); }
                auto it = segments.upper_bound(x.first);
                ASSERT(it == segments.end() or x.second <= it->end_offset);
                if (it != segments.begin()) {
                    --it;
                    ASSERT(it == segments.end() or it->start_offset <= x.first);
                }
            }
            else {
                auto it = segments.find(rval.first->start_offset);
                if (it == segments.end())
                    it = segments._M_insert_unique(*rval.first).first;
                else
                    ASSERT(rval.first->segment_number == it->segment_number
                           and rval.first->end_offset == it->end_offset);

                ASSERT(it->start_offset <= x.first);
                size_t buf_offset = it->buf_offset(x.first);
                if (rval.second) {
                    // straddle
                    P("=> straddle @%zd", buf_offset);
                    if (verbose) { results[x] = std::make_pair("straddle", buf_offset); }
                    ASSERT(it->end_offset < x.second);
                }
                else {
                    // normal
                    P("=> normal @%zd", buf_offset);
                    if (verbose) { results[x] = std::make_pair("normal", buf_offset); }
                    ASSERT(x.second <= it->end_offset);
                }
            }
        }
    }
    
    for (auto it : segments) {
        P("Segment %zd: [%zd %zd) @%zd", it.segment_number, it.start_offset,
          it.end_offset, it.byte_offset);
    }
    
    for (auto it : results) {
        if ((int64_t) it.second.second < 0)
            P("[%zd, %zd) -> ---- %s", it.first.first, it.first.second, it.second.first);
        else
            P("[%zd, %zd) -> %zd %s", it.first.first, it.first.second, it.second.second, it.second.first);
    }
}
int main() {
    doit(10*1024, 1000, true);
    doit(1024*1024, 65536, false);
}
