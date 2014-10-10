#include "sm-gc.h"

namespace GC {
    gc_thread GC_thread;
    percore<size_t, false, false> allocated_memory;

    struct thread_data {
        bool initialized;
    };

    static __thread thread_data tls;
};

/***************************************
 * * * Callbacks for the epoch_mgr * * *
 ***************************************/
void
global_init(void*)
{
    for (size_t i = 0; i < coreid::NMaxCores; i++)
        GC::allocated_memory[i] = 0;
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
	// cookie initialization
    GC::tls.initialized = true;
    return &GC::tls;
}

void
thread_deregistered(void*, void *thread_cookie)
{
    auto *t = (GC::thread_data*) thread_cookie;
    ASSERT(t == &GC::tls);
    t->initialized = false;
}

void *
epoch_ended(void* cookie, epoch_mgr::epoch_num e)
{
    std::cout << "e " << e << "ended" << std::endl;
	// TODO. record cur_lsn
    return cookie;  // return cookie to get epoch_reclaimed called
}

// FIXME: tzwang: need this?
void *
epoch_ended_thread(void *, void *epoch_cookie, void *)
{
    return epoch_cookie;
}

void
epoch_reclaimed(void *, void *)
{
	// TODO. signal to GC daemon
}

epoch_mgr gc_epochs{{
        nullptr, &global_init, &get_tls,
            &thread_registered, &thread_deregistered,
            &epoch_ended, &epoch_ended_thread,
            &epoch_reclaimed}};

void 
GC::epoch_enter()
{
    if (!GC::tls.initialized)
        gc_epochs.thread_init();
    gc_epochs.thread_enter();
}

void
GC::epoch_exit()
{
	gc_epochs.thread_exit();
}

void
GC::epoch_quiesce()
{
    gc_epochs.thread_quiesce();
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
		std::cout << "cur epoch :" << gc_epochs.get_cur_epoch() << std::endl;
		std::cout<< "memory consumed:" << total_mem << std::endl;
		for (size_t i = 0; i < coreid::NMaxCores; i++)
			allocated_memory[i] = 0;
		// trigger epoch advance
        epoch_exit();   // FIXME: tzwang: this correct?
        while (not gc_epochs.new_epoch())
            usleep(1000);
		epoch_enter();
        std::cout << "new epoch :" << gc_epochs.get_cur_epoch() << std::endl;
	}
}

void
GC::gc_thread::gc_daemon()
{
  while (1)
  {
	  ;
	  // wait for notification
	  // GC work
  }
}

GC::gc_thread::gc_thread()
{
  std::cout << "GC thread start" << std::endl;
  std::thread t(gc_daemon);			// # of threads should be based on system speed, GC speed. 
  t.detach();
}

