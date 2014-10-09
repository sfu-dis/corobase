#include "rcu.h"
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

using namespace RCU;

bool done = false;

struct my_node {
    _rcu_slist::node _node;
    uintptr_t value;
};

typedef rcu_slist<my_node> my_list;
my_list list;

typedef std::vector<my_node*> node_list;

static
void check_list(node_list &present)
{
    std::set<void*> unseen(present.begin(), present.end());
    for (my_list::iterator it=list.begin(); it != list.end(); ++it)
        unseen.erase(&*it);
    
    ASSERT (unseen.empty());
}

extern "C" void *thread_run(void*) {
    uint64_t now = stopwatch_t::now();
    uint32_t seed[] = {
        (uint32_t) (now >> 32),
        (uint32_t) now,
        (uint32_t) getpid(),
        (uint32_t) (uintptr_t) pthread_self()
    };
    w_rand rng(seed);
    
    node_list inserted;
    rcu_register();
    rcu_enter();
    int count = 1;
    for (int n=1; not done; n++) {
        int sz = rng.randn(10,100);
        for (int i=0; i < sz; i++) {
            my_node *n = rcu_alloc();
            int x = ++count;//rng.randn(1000);
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
            ASSERT (inserted.empty() or not is_empty);
            if (0 and is_empty)
                printf("List empty!\n");
        }

        if (not (n % 10))
            rcu_quiesce();
        
        if (0 and not rng.randn(1000))
            sleep(1);
    }    

    return 0;
}

struct list_visitor {
    FILE *_out;
    list_visitor(FILE *out)
        : _out(out)
    {
        fprintf(_out, "list: ");
    }

    bool operator()(my_node *n, bool is_dead) const {
        fprintf(_out, "[%ld%s] ", n->value, is_dead? "*" : "");
        return 0;
    }
    ~list_visitor() {
        fprintf(_out, "\n");
    }
};
        
int main(int argc, char const *argv[]) {

    rcu_set_gc_threshold(10*1000, 100*1000);
    rcu_register();
    
    std::vector<pthread_t> tids;
    int nthreads = 0;
    if (argc > 1) {
        nthreads = atoi(argv[1]);
    }

    if (nthreads < 1 or nthreads > 100) {
        nthreads = 3;
    }

    fprintf(stderr, "Running with %d threads\n", nthreads);

    // global init
    rcu_enter();
    list.visit(list_visitor(stdout));
    rcu_exit();

    // Starts the hr timer
    stopwatch_t timer;

    // Kick offs all the threads
    for (int i=0; i < nthreads; i++) {
        pthread_t tid;
        int err = pthread_create(&tid, 0, &thread_run, 0);
        ASSERT (not err);
        tids.push_back(tid);
    }

    // Three-times, sleep for 3 secs and print list (total 9 secs)
    for (int i=0; i < 3; i++) {
        sleep(3);
        rcu_enter();
        list.visit(list_visitor(stdout));
        rcu_exit();
    }
    done = true;

    for (int i=0; i < nthreads; i++) {
        pthread_join(tids[i], 0);
    }

    // Calculates the time
    fprintf(stderr, "Took %.3f seconds\n", timer.time());
   
    rcu_enter();
    list.visit(list_visitor(stdout));
    rcu_exit();

    // normally not necessary, but useful for testing
    rcu_deregister();
    rcu_gc_info info = rcu_get_gc_info();
    fprintf(stderr, "RCU subsystem freed %zd objects in %zd passes\n",
            info.objects_freed, info.gc_passes);
}

