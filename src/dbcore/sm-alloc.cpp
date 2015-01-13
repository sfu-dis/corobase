#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"
//#include "../masstree_btree.h"

namespace RA {
    char **mem;
    uint64_t *alloc_offset;
    int nr_sockets;
    //std::vector<concurrent_btree*> tables;
    //std::vector<std::string> table_names;
    LSN trim_lsn;
	//std::condition_variable gc_trigger;
	//std::mutex gc_lock;
	void gc_daemon( int id );

    // epochs related
    static __thread struct thread_data epoch_tls;
    epoch_mgr ra_epochs {{nullptr, &global_init, &get_tls,
                        &thread_registered, &thread_deregistered,
                        &epoch_ended, &epoch_ended_thread, &epoch_reclaimed}};

    // epoch mgr callbacks
    epoch_mgr::tls_storage *
    get_tls(void*)
    {
        static __thread epoch_mgr::tls_storage s;
        return &s;
    }

	void ra_register()
	{
		ra_epochs.thread_init();
	}

	void ra_deregister()
	{
		ra_epochs.thread_fini();
	}
	bool ra_is_registered() {
		return ra_epochs.thread_initialized();
	}
    void global_init(void*)
    {
        /*
#define NR_GC_DAEMONS 1
		for( int i = 0; i < NR_GC_DAEMONS; i++ )
		{
			std::thread t(gc_daemon, i);
			t.detach();
		}
        */
    }

    void*
    thread_registered(void*)
    {
        epoch_tls.initialized = true;
		epoch_tls.nbytes = 0;
		epoch_tls.counts = 0;
        return &epoch_tls;
    }

    void
    thread_deregistered(void *cookie, void *thread_cookie)
    {
        auto *t = (thread_data*) thread_cookie;
        ASSERT(t == &epoch_tls);
        t->initialized = false;
		t->nbytes = 0;
		t->counts = 0;
    }

    void*
    epoch_ended(void *cookie, epoch_num e)
	{
		// So we need this rcu_is_active here because
		// epoch_ended is called not only when an epoch is eneded,
		// but also when threads exit (see epoch.cpp:274-283 in function
		// epoch_mgr::thread_init(). So we need to avoid the latter case
		// as when thread exits it will no longer be in the rcu region
		// created by the scoped_rcu_region in the transaction class.
		LSN *lsn = (LSN *)malloc(sizeof(LSN));
		//ALWAYS_ASSERT(lsn);
		//RCU::rcu_enter();
		*lsn = transaction_base::logger->cur_lsn();
		//RCU::rcu_exit();
		return lsn;
	}

    void*
    epoch_ended_thread(void *cookie, void *epoch_cookie, void *thread_cookie)
    {
        return epoch_cookie;
    }

    void
    epoch_reclaimed(void *cookie, void *epoch_cookie)
    {
        LSN lsn = *(LSN *)epoch_cookie;
        if (lsn != INVALID_LSN)
		{
			volatile_write( trim_lsn._val, lsn._val);
			free(epoch_cookie);
		}
    }

    void
    epoch_enter(void)
    {
        ra_epochs.thread_enter();
    }

    void
    epoch_exit(void)
    {
        ra_epochs.thread_exit();
    }

    void
    epoch_thread_quiesce(void)
    {
        ra_epochs.thread_quiesce();
    }

    void init() {
        nr_sockets = numa_max_node() + 1;
        alloc_offset = (uint64_t *)malloc(sizeof(uint64_t) * nr_sockets);
        memset(alloc_offset, '\0', sizeof(uint64_t) * nr_sockets);
        mem = (char **)malloc(sizeof(char *) * nr_sockets);
        for (int i = 0; i < nr_sockets; i++) {
            numa_run_on_node(i);
            uint64_t mega = 1024 * 1024;
            printf("allocating for socket %d\n", i);
            mem[i] = (char *)numa_alloc_local(8192 * mega);
            printf("faulting for socket %d\n", i);
            memset(mem[i], '\0', 8192 * mega);
        }
    }

    void *allocate(uint64_t size) {
        //int skt = sched_getcpu() % nr_sockets;
		//char *p = &mem[skt][__sync_fetch_and_add(&alloc_offset[skt], align_up(size, 1 << DEFAULT_ALIGNMENT_BITS))];
        void* p =  malloc(size);
		ALWAYS_ASSERT(p);
		epoch_tls.nbytes += size;
		epoch_tls.counts += 1;

		if( epoch_tls.nbytes >= (1<<28) or epoch_tls.counts >= (20000) )
		{
			// New Epoch
			if( ra_epochs.new_epoch_possible() )
			{
				if(ra_epochs.new_epoch() )
				{
					epoch_tls.nbytes = 0;
					epoch_tls.counts = 0;
				}
			}
		}
		return p;
    }

	void deallocate( void* p )
	{
		ASSERT(p);
		free(p);
	}

    /*
    void register_table(concurrent_btree *t, std::string name) {
        tables.push_back(t);
        table_names.push_back(name);
    }

	void gc_daemon( int id )
	{
		std::unique_lock<std::mutex> lock(gc_lock);
		while(1)
		{
			gc_trigger.wait(lock);
			LSN tlsn = volatile_read(trim_lsn);
			uint64_t reclaimed_nbytes = 0;
			std::cout << "GC(" << id  << ")  started: trim LSN(" << tlsn._val << ")" << std::endl;
			for (uint i = 0; i < RA::tables.size(); i++) {
				concurrent_btree *t = RA::tables[i];
				concurrent_btree::tuple_vector_type *v = t->get_tuple_vector();
				INVARIANT(v);
				for( uint64_t oid = 1; oid < v->size(); oid++ )
				{
start_over:
					fat_ptr *prev_next = v->begin_ptr(oid);
					fat_ptr head = volatile_read( *prev_next );
					fat_ptr cur = head;
					bool found = false;

					while (cur.offset() ) {
						object *cur_obj = (object *)cur.offset();
						dbtuple *version = reinterpret_cast<dbtuple *>(cur_obj->payload());
						auto clsn = volatile_read(version->clsn);
						if( clsn.asi_type() == fat_ptr::ASI_LOG and cur != head and LSN::from_ptr(clsn) < tlsn )
						{
							// unlink
							fat_ptr null_ptr = NULL_PTR; 
							if( not __sync_bool_compare_and_swap( &prev_next->_ptr, cur._ptr, null_ptr._ptr ) )
								goto start_over;
							found = true;
							break;
						}
						prev_next = &cur_obj->_next;
						cur = volatile_read( *prev_next );
					}
					if( found )
					{
						while( cur.offset() )
						{
							object *cur_obj = (object *)cur.offset();
							fat_ptr next = cur_obj->_next;
							reclaimed_nbytes += cur_obj->_size;

							// free memory
							deallocate((void*)cur_obj );
							cur = next;
						}
					}

				}
			}
			std::cout << "GC(" << id  << ") finished: trim LSN(" << tlsn._val << ") " << "reclaimed bytes(" << reclaimed_nbytes << ")" << std::endl;
		}
	}
    */
};


