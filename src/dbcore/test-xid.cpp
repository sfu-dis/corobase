#include "xid.h"

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

bool done = false;

extern "C" void* slow_worker(void* t) {
  size_t tid = (size_t)t;
  while (not volatile_read(done)) {
    XID xid = xid_alloc();
    printf("%.10u %.4u\n", xid.epoch(), xid.local());
    usleep(1000);
    xid_get_context(xid);
    xid_free(xid);
  }
  return NULL;
}

extern "C" void* fast_worker(void* t) {
  size_t tid = (size_t)t;
  while (not volatile_read(done)) {
    XID xid = xid_alloc();
    printf("%.10u %.4u\n", xid.epoch(), xid.local());
    usleep(10);
    xid_get_context(xid);
    xid_free(xid);
  }
  return NULL;
}

int main() {
  pthread_t fast_thd[4], slow_thd;

  fprintf(stderr, "Creating worker threads\n");
  pthread_create(&slow_thd, NULL, slow_worker, (void*)-1);
  for (auto it : enumerate(fast_thd))
    pthread_create(&it.second, NULL, fast_worker, (void*)it.first);

  fprintf(stderr, "Sleeping for 10 seconds...\n");
  sleep(10);
  fprintf(stderr, "... back, joining with workers\n");
  done = true;

  for (auto t : fast_thd) pthread_join(t, NULL);
  pthread_join(slow_thd, NULL);
  fprintf(stderr, "Done!\n");
}
