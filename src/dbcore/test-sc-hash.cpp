#include "sc-hash.h"

#include "sm-common.h"
#include "burt-hash.h"
#include "stopwatch.h"
#include "w_rand.h"

#include <unistd.h>
#include <cxxabi.h>
#include <cmath>

/* A way to track mean and variance of a large sample accurately with O(1)
   space.

   Hat tip to the second solution at
   http://math.stackexchange.com/questions/20593/calculate-variance-from-a-stream-of-sample-values
 */
struct mvar_tracker {
  double m;
  double v;
  size_t k;
  mvar_tracker() : m(0), v(0), k(0) {}

  void record(double x) {
    ++k;
    auto delta = x - m;
    m += delta / k;
    v = ((k - 1) * v + delta * (x - m)) / k;
  }
};

static size_t eqcnt = 0;
struct obj {
  int x;
  bool operator==(obj const &other) const {
    eqcnt++;
    return x == other.x;
  }
  bool operator==(int ox) const {
    eqcnt++;
    return x == ox;
  }
};

struct hasher {
  burt_hash h;
  size_t count = 0;

  uint32_t operator()(int x) {
    count++;
    return h(x);
  }
  uint32_t operator()(obj const &o) { return (*this)(o.x); }
};

struct hinstance {
  sc_hash_set<256, obj, hasher, void, 2, 4, true, true> hset;
  bool full;
  size_t size;
  mvar_tracker stats;

  hinstance() : full(false), size(0) {}
  void clear() {
    hset.clear();
    full = false;
    size = 0;
  }

  void put(int x) {
    if (full) return;

    auto it = hset.insert(x);
    if (it.second < 0)
      full = true;
    else if (it.second == 0)
      size++;
  }

  bool find(int x) { return hset.find(x) != hset.end(); }
};

int main() {
  uint64_t now = stopwatch_t::now();
  uint32_t seed[] = {uint32_t(getpid()), uint32_t(now), uint32_t(now >> 32)};
  // uint32_t seed[] = {0x00004258, 0x7b02ad04, 0x1388562a};

  w_rand rng(seed);
  fprintf(stderr, "RNG seed: {0x%08x, 0x%08x, 0x%08x}\n", seed[0], seed[1],
          seed[2]);

  sc_hash_set<256, obj, hasher> h{};

  {
    auto rval = h.insert(10);
    ASSERT(not rval.second and *rval.first == 10);
  }
  {
    auto it = h.find(10);
    ASSERT(it != h.end() and *it == 10);
  }
  {
    auto it = h.begin();
    ASSERT(it != h.end() and *it == 10);
  }
  ASSERT(h.find(15) == h.end());
  {
    auto rval = h.erase(15);
    ASSERT(not rval);
  }
  {
    auto rval = h.erase(10);
    ASSERT(rval);
    ASSERT(h.find(10) == h.end());
    ASSERT(h.begin() == h.end());
  }
  {
    auto rval = h.insert(10);
    ASSERT(not rval.second and *rval.first == 10);
    h.erase(rval.first);
    ASSERT(h.find(10) == h.end());
    ASSERT(h.begin() == h.end());
  }

  hinstance hset[1];

  auto all_full = [&]() -> bool {
    for (auto &h : hset) {
      if (not h.full) return false;
    }
    return true;
  };

  for (int i = 0; not all_full(); i++) {
    auto x = rng.rand();
    for (auto &h : hset) h.put(x);
  }
  size_t count = 0;
  for (auto __attribute__((unused)) x : hset->hset) count++;

  ASSERT(count == hset->size);
  ASSERT(hset->hset.size() == hset->size);

  eqcnt = 0;
  for (auto &h : hset) h.clear();

  size_t niter = 1000;
  for (size_t j = 0; j < niter; j++) {
    for (auto &h : hset) h.clear();

    for (int i = 0; not all_full(); i++) {
      auto x = rng.rand();
      for (auto &h : hset) h.put(x);
    }
    for (auto &h : hset) h.stats.record(h.size);

    if (j < 10) {
      printf("Tables overflowed at: ");
      for (auto &h : hset) printf(" %4zd", h.size);
      printf("\n");
    }
  }
  printf("Mean and stddev for each table type:\n");
  for (auto &h : hset) {
    int status;
    char *name = abi::__cxa_demangle(typeid(h.hset).name(), 0, 0, &status);
    auto cost = std::make_pair(h.hset._hash.count, eqcnt);  // h.hptr->cost();
    double total_sz = h.stats.k * h.stats.m;
    DEFER(free(name));
    ASSERT(not status);
    printf(
        "\t%40s: %5.1f +/- %4.1f at cost %.1f/%.1f hash/comparisons per "
        "insert\n",
        name, h.stats.m, sqrt(h.stats.v), cost.first / total_sz,
        cost.second / total_sz);
  }

  eqcnt = 0;
  for (auto &h : hset) h.hset._hash.count = 0;

  niter = 10000;
  for (size_t j = 0; j < niter; j++) {
    auto x = rng.rand();
    for (auto &h : hset) h.find(x);
  }
  printf("Mean and stddev for each table type:\n");
  for (auto &h : hset) {
    int status;
    char *name = abi::__cxa_demangle(typeid(h.hset).name(), 0, 0, &status);
    auto cost = std::make_pair(h.hset._hash.count, eqcnt);  // h.hptr->cost();
    double total_sz = niter;
    DEFER(free(name));
    ASSERT(not status);
    printf("\t%40s: cost %.1f/%.1f hash/comparisons per search\n", name,
           cost.first / total_sz, cost.second / total_sz);
  }
}
