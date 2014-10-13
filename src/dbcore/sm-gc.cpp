#include "sm-gc.h"

percore<size_t, false, false> GC::allocated_memory;
sm_log *GC::logger;
LSN GC::reclaim_lsn;
__thread struct GC::thread_data GC::tls;
std::condition_variable GC::cleaner_cv;
std::mutex GC::cleaner_mutex;

/***************************************
 * * * Callbacks for the epoch_mgr * * *
 ***************************************/
void
GC::global_init(void*)
{
    for (size_t i = 0; i < coreid::NMaxCores; i++)
        allocated_memory[i] = 0;
}

epoch_mgr::tls_storage *
GC::get_tls(void*)
{
    static __thread epoch_mgr::tls_storage s;
    return &s;
}

void *
GC::thread_registered(void*)
{
    // cookie initialization
    GC::tls.initialized = true;
    return &GC::tls;
}

void
GC::thread_deregistered(void*, void *thread_cookie)
{
    auto *t = (GC::thread_data*) thread_cookie;
    ASSERT(t == &GC::tls);
    t->initialized = false;
}

void *
GC::epoch_ended(void* cookie, epoch_mgr::epoch_num e)
{
    ASSERT(GC::logger);
    std::cout << "e " << e << "ended" << std::endl;
    // So we need this rcu_is_active here because
    // epoch_eneded is called not only when an epoch is eneded,
    // but also when threads exit (see epoch.cpp:274-283 in function
    // epoch_mgr::thread_init(). So we need to avoid the latter case
    // as when thread exits it will no longer be in the rcu region
    // created by the scoped_rcu_region in the transaction class.
    if (RCU::rcu_is_active()) {
        // FIXME: change to some clever alloc
        LSN *lsn = (LSN *)malloc(sizeof(LSN));
        *lsn = GC::logger->cur_lsn();
        // record cur_lsn
        return lsn; // return cookie to get epoch_reclaimed called
    }
    return NULL;
}

// FIXME: tzwang: need this?
void *
GC::epoch_ended_thread(void *, void *epoch_cookie, void *)
{
    return epoch_cookie;
}

void
GC::epoch_reclaimed(void *cookie, void *epoch_cookie)
{
    reclaim_lsn = *(LSN *)epoch_cookie;
    free(epoch_cookie);
    // signal the GC cleaner daemon to do real work
    cleaner_cv.notify_all();
}

void 
GC::epoch_enter()
{
    if (!GC::tls.initialized)
        epochs.thread_init();
    epochs.thread_enter();
}

void
GC::epoch_exit()
{
    epochs.thread_exit();
}

void
GC::epoch_quiesce()
{
    epochs.thread_quiesce();
}

void
GC::report_malloc(size_t nbytes)
{
    allocated_memory.my() += nbytes;
    size_t total_mem = 0;

    for (size_t i = 0; i < coreid::NMaxCores; i++)
        total_mem += allocated_memory[i];
    if( total_mem > WATERMARK )
    {
        // FIXME: tzwang: the threaad should already be active
        // if this is ever called? (as we called epoch_enter in tx ctor)
        std::cout << "cur epoch :" << epochs.get_cur_epoch() << std::endl;
        std::cout<< "memory consumed:" << total_mem << std::endl;
        for (size_t i = 0; i < coreid::NMaxCores; i++)
            allocated_memory[i] = 0;
        // trigger epoch advance
        epoch_exit();   // FIXME: tzwang: this correct?
        while (not epochs.new_epoch())
            usleep(1000);
        epoch_enter();
        std::cout << "new epoch :" << epochs.get_cur_epoch() << std::endl;
    }
}

void
GC::cleaner_daemon()
{
    std::cout << "GC cleaner thread start" << std::endl;
    std::unique_lock<std::mutex> lock(cleaner_mutex);
    while (1) {
        cleaner_cv.wait(lock);
        // GC work
        std::cout << "[GC cleaner] to sweep versions < "
                  << reclaim_lsn._val << std:: endl;
        // NOTE: also need to make sure there's at least only one version
        // left for each tuple, even if the tuple's only version is less
        // than reclaim_lsn; touch clean versions only too.
    }
}

GC::GC(sm_log *l)
{
    GC::logger = l;
    std::thread t(cleaner_daemon);  // # of threads should be based on system speed, GC speed.
    t.detach();
}

