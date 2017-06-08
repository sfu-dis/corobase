#include "rcu-slist.h"

#include "w_rand.h"
#include "stopwatch.h"

#include <stdint.h>
#include <pthread.h>
#include <vector>
#include <cstdio>
#include <unistd.h>
#include <cstdlib>
#include <map>
#include <set>

void *RCU::rcu_alloc(size_t nbytes) { return std::malloc(nbytes); }

void __attribute__((noinline)) rcu_free(void const *ptr);

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
typedef std::vector<void *> ptr_list;

typedef std::map<pthread_t, ptr_list **> rcu_active_list;
rcu_active_list rcu_active;
std::vector<ptr_list *> rcu_cooling;
std::set<pthread_t> everyone, in_flight;

void RCU::rcu_unwind(char const *msg) {
  fprintf(stderr, "%s: <unwinding not implemented>\n", msg);
}

static __thread ptr_list *tls_rcu_active;

void __attribute__((noinline)) RCU::rcu_register() {
  tls_rcu_active = new ptr_list;
  pthread_t self = pthread_self();
  pthread_mutex_lock(&global_mutex);
  everyone.insert(self);
  in_flight.insert(self);
  rcu_active[self] = &tls_rcu_active;
  pthread_mutex_unlock(&global_mutex);
}

bool RCU::rcu_is_active() { return tls_rcu_active != NULL; }

void __attribute__((noinline)) RCU::rcu_quiesce() {
  pthread_t self = pthread_self();
  std::vector<ptr_list *> old_cooling;
  pthread_mutex_lock(&global_mutex);
  if (in_flight.erase(self) and in_flight.empty()) {
    // collection cycle!
    old_cooling.swap(rcu_cooling);
    for (rcu_active_list::iterator it = rcu_active.begin();
         it != rcu_active.end(); ++it) {
      ptr_list *x = new ptr_list;
      rcu_cooling.push_back(*it->second);
      *it->second = x;
    }
    in_flight = everyone;
  }
  pthread_mutex_unlock(&global_mutex);

  // now clean up the mess...
  int freed = 0;
  for (size_t i = 0; i < old_cooling.size(); i++) {
    ptr_list *x = old_cooling[i];
    freed += x->size();
    for (ptr_list::iterator it = x->begin(); it != x->end(); ++it)
      std::free(*it);
    delete x;
  }

  if (old_cooling.size())
    fprintf(stderr, "Thread %ld freed %d dead objects\n", (long)(uintptr_t)self,
            freed);
}

void __attribute__((noinline)) RCU::rcu_free(void const *p) {
  tls_rcu_active->push_back((void *)p);
}

struct my_node {
  _rcu_slist::node _node;
  uintptr_t value;
};

typedef rcu_slist<my_node> my_list;
my_list list;
bool done = false;

typedef std::vector<my_node *> node_list;

static void check_list(node_list &present) {
  std::set<void *> unseen(present.begin(), present.end());
  for (my_list::iterator it = list.begin(); it != list.end(); ++it)
    unseen.erase(&*it);

  ASSERT(unseen.empty());
}

extern "C" void *thread_run(void *) {
  uint64_t now = stopwatch_t::now();
  uint32_t seed[] = {(uint32_t)(now >> 32), (uint32_t)now, (uint32_t)getpid(),
                     (uint32_t)(uintptr_t)pthread_self()};
  w_rand rng(seed);

  node_list inserted;
  rcu_register();
  int count = 1;
  for (int n = 1; not done; n++) {
    int sz = rng.randn(10, 100);
    for (int i = 0; i < sz; i++) {
      my_node *n = rcu_alloc();
      int x = ++count;  // rng.randn(1000);
      n->value = x;
      bool success = list.push(n);
      ASSERT(success);
      inserted.push_back(n);
      check_list(inserted);
    }

    while (not inserted.empty()) {
      int i = rng.randn(inserted.size());
      bool is_empty = list.remove(inserted[i]);
      inserted[i] = inserted.back();
      inserted.pop_back();
      check_list(inserted);
      ASSERT(inserted.empty() or not is_empty);
      if (is_empty) printf("List empty!\n");
    }

    if (not(n % 10)) rcu_quiesce();

    if (not rng.randn(1000)) sleep(1);
  }

  rcu_quiesce();
  return 0;
}

struct list_visitor {
  FILE *_out;
  list_visitor(FILE *out) : _out(out) { fprintf(_out, "list: "); }

  bool operator()(my_node *n, bool is_dead) const {
    fprintf(_out, "[%ld%s] ", n->value, is_dead ? "*" : "");
    return 0;
  }
  ~list_visitor() { fprintf(_out, "\n"); }
};

int main() {
  // global init
  rcu_register();
  list.visit(list_visitor(stdout));

  std::vector<pthread_t> tids;
  int nthreads = 4;
  for (int i = 0; i < nthreads; i++) {
    pthread_t tid;
    int err = pthread_create(&tid, 0, &thread_run, 0);
    ASSERT(not err);
    tids.push_back(tid);
  }

  for (int i = 0; i < 3; i++) {
    sleep(3);
    list.visit(list_visitor(stdout));
  }
  done = true;

  for (int i = 0; i < nthreads; i++) pthread_join(tids[i], 0);

  list.visit(list_visitor(stdout));
}
