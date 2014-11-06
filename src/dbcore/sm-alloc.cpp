#include <sys/mman.h>
#include "sm-alloc.h"
#include "sm-common.h"
#include "../txn.h"
#include "../masstree_btree.h"

namespace RA {
    std::vector<concurrent_btree*> tables;
    std::vector<std::string> table_names;
    LSN trim_lsn;

    void register_table(concurrent_btree *t, std::string name) {
        tables.push_back(t);
        table_names.push_back(name);
    }

    void *allocate(uint64_t size) {
		void* p =  malloc(size);
		ALWAYS_ASSERT(p);
		return p;
    }

    // epochs related
    __thread struct thread_data epoch_tls;
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

    void global_init(void*)
    {
    }

    void*
    thread_registered(void*)
    {
        epoch_tls.initialized = true;
        return &epoch_tls;
    }

    void
    thread_deregistered(void *cookie, void *thread_cookie)
    {
        auto *t = (thread_data*) thread_cookie;
        ASSERT(t == &epoch_tls);
        t->initialized = false;
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
		ALWAYS_ASSERT(lsn);
		*lsn = transaction_base::logger->cur_lsn();
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
            trim_lsn = *(LSN *)epoch_cookie;
        free(epoch_cookie);
    }

    void
    epoch_enter(void)
    {
        if (!epoch_tls.initialized) {
            ra_epochs.thread_init();
        }
        ra_epochs.thread_enter();
    }

    void
    epoch_exit(void)
    {
        ra_epochs.thread_quiesce();
        ra_epochs.thread_exit();
    }

    void
    epoch_thread_quiesce(void)
    {
        ra_epochs.thread_quiesce();
    }
};

