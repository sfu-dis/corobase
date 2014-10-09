#include "epoch.h"

#include <pthread.h>
#include <tuple>
#include <utility>

#define LOG(msg, ...) fprintf(stderr, msg "\n", ##__VA_ARGS__)

size_t x = 1;

void
global_init(void* arg)
{
    LOG("Initializing");
}
epoch_mgr::tls_storage *
get_tls(void*)
{
    static __thread epoch_mgr::tls_storage s;
    return &s;
}
void *
thread_registered(void*)
{
    LOG("Thread %zd registered", (size_t) pthread_self());
    return 0;
}
void
thread_deregistered(void* cookie, void *thread_cookie)
{
    LOG("Thread %zd deregistered", (size_t) pthread_self());
}
void *
epoch_ended(void*, epoch_mgr::epoch_num x)
{
    LOG("Epoch %zd ended", x);
    return (void*)x;
}

void *
epoch_ended_thread(void *cookie, void *epoch_cookie, void *thread_cookie)
{
    return epoch_cookie;
}
void
epoch_reclaimed(void *cookie, void *epoch_cookie)
{
    LOG("Epoch %zd reclaimed", (size_t)epoch_cookie);
}

struct state {
};

static state s;

static epoch_mgr em{{
        &s, &global_init, &get_tls,
        &thread_registered, &thread_deregistered,
        &epoch_ended, &epoch_ended_thread,
        &epoch_reclaimed
}};

int main() {
    em.thread_init();
    LOG("thread_enter");
    em.thread_enter();
    em.new_epoch();
    em.new_epoch();
    em.new_epoch();
    LOG("thread_quiesce");
    em.thread_quiesce();
    em.new_epoch();
    em.new_epoch();
    em.new_epoch();
    LOG("thread_exit");
    em.thread_exit();
    em.new_epoch();
    em.new_epoch();
    em.new_epoch();
    DEFER(em.thread_fini());
}
