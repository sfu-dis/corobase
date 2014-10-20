#include "sm-gc.h"

percore<size_t, false, false> GC::allocated_memory;
sm_log *GC::logger;
LSN GC::reclaim_lsn;
__thread struct GC::thread_data GC::tls;
std::condition_variable GC::cleaner_cv;
std::mutex GC::cleaner_mutex;
std::vector<concurrent_btree*> GC::tables;

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
    // So we need this rcu_is_active here because
    // epoch_ended is called not only when an epoch is eneded,
    // but also when threads exit (see epoch.cpp:274-283 in function
    // epoch_mgr::thread_init(). So we need to avoid the latter case
    // as when thread exits it will no longer be in the rcu region
    // created by the scoped_rcu_region in the transaction class.
    if (RCU::rcu_is_active()) {
        // record cur_lsn
        LSN *lsn = (LSN *)malloc(sizeof(LSN));
        *lsn = GC::logger->cur_lsn();
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
//    size_t total_mem = 0;

//    for (size_t i = 0; i < coreid::NMaxCores; i++)
  //      total_mem += allocated_memory[i];
    if( allocated_memory.my() > WATERMARK )
    {
		if(epochs.new_epoch_possible())
		{
			epochs.new_epoch();	
			allocated_memory.my() = 0;
		}
    }
}

void
GC::cleaner_daemon()
{
	std::cout << "GC Daemon Started" << std::endl;
	std::unique_lock<std::mutex> lock(cleaner_mutex);
    while (1) 
	{
		// proceed on epoch_reclaimed
        cleaner_cv.wait(lock);

		// TODO. Multiple thread on partitioned object tables?
//		std::cout << "GC begin! Reclaim LSN: " << reclaim_lsn._val <<  std::endl;

		// GC
		for( unsigned int i = 0; i < tables.size(); i++ )
			tables[i]->cleanup_versions( reclaim_lsn );

		// Reset memory usage stats
//		for (size_t i = 0; i < coreid::NMaxCores; i++)
//			allocated_memory[i] = 0;				// FIXME. couldn't be zero

//		std::cout << "GC finished! Reclaim LSN: " << reclaim_lsn._val <<  std::endl;
    }
}

void 
GC::register_table(concurrent_btree* table)
{
	GC::tables.push_back(table);
}

GC::GC(sm_log *l)
{
    GC::logger = l;
    std::thread t(cleaner_daemon);  // # of threads should be based on system speed, GC speed.
    t.detach();
}

