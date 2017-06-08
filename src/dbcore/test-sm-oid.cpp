#include "sm-oid.h"

#include "stopwatch.h"
#include "w_rand.h"

#include <set>
#include <vector>

#include <cmath>
#include <pthread.h>
#include <unistd.h>

#define LOG(msg, ...) fprintf(stderr, msg "\n", ##__VA_ARGS__)

sm_oid_mgr *om;
bool done = false;

static int const NTABLES = 100;
FID tables[NTABLES];

extern "C" void *thread_main(void *arg) {
  int tnum = (uintptr_t)arg;

  uint64_t now = stopwatch_t::now();
  uint32_t seed[] = {
      (uint32_t)(now >> 32), (uint32_t)now, (uint32_t)getpid(), (uint32_t)tnum,
  };
  LOG("t%d seed: {0x%08x 0x%08x 0x%08x 0x%08x}", tnum, seed[0], seed[1],
      seed[2], seed[3]);

  w_rand rng(seed);

  /* Use a quadratic distribution of sorts so that some tables are
     heavy hitters
   */
  int ntables = rng.randn(min(NTABLES, 10), NTABLES);
  int nsel = (ntables + 1) * (ntables + 1) - 1;
  std::vector<OID> oids[ntables];

  while (not volatile_read(done)) {
    int idx = std::sqrt(rng.randn(1, nsel)) - 1;
    ASSERT(0 <= idx and idx < NTABLES);
    FID f = tables[idx];

    // want to allocate slightly more often than we delete
    auto &olist = oids[idx];
    int should_delete = (rng.randn(99) < 49) and not olist.empty();
    if (should_delete) {
      int victim = rng.randn(olist.size());
      OID v = olist[victim];
      olist[victim] = olist.back();
      olist.pop_back();
      om->free_oid(f, v);
    } else {
      OID o = om->alloc_oid(f);
      om->oid_put(f, o, NULL_PTR);
      olist.push_back(o);
    }

    /* About 1% of the time, do a spot check for duplicates */
    if (not rng.randn(100)) {
      std::set<OID> nodups(olist.begin(), olist.end());
      ASSERT(nodups.size() == olist.size());
    }
  }

  return 0;
}

int main() {
  /* No attempt at recovery for now (it's ont implemented yet). Just
     test out file creation/deletion and OID alloc/free
   */
  om = sm_oid_mgr::create(NULL, NULL);

  {
    /* quick sanity test run */
    LOG("creating a table");
    auto f = om->create_file(true);
    LOG("Allocating OID from table %d", f);
    auto o = om->alloc_oid(f);
    LOG("Freeing OID %d", o);
    om->free_oid(f, o);
    LOG("Deleting table %d", f);
    om->destroy_file(f);
  }
#if 0
        static int const NOIDS = 100;
        OID oids[NOIDS];
        for (auto &o : oids)
            o = om->alloc_oid(f);
#endif

  LOG("creating %d tables", NTABLES);
  for (auto &f : tables) f = om->create_file(true);

  static int const N = 4;
  pthread_t tids[N];
  LOG("creating %d worker threads", N);
  for (int i = 0; i < N; i++)
    pthread_create(&tids[i], NULL, thread_main, (void *)(uintptr_t)i);

  LOG("sleeping for 10 seconds...");
  sleep(10);
  done = true;

  LOG("joining worker threads");
  for (auto tid : tids) pthread_join(tid, NULL);

  LOG("destroying tables");
  for (auto f : tables) om->destroy_file(f);

  LOG("done!");
}
