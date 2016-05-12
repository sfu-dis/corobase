#pragma once
#include <iostream>
#include <string>
#include <numa.h>
#include "sm-defs.h"
#include "../macros.h"

class sm_log_recover_impl;

class sysconf {
public:
    static uint32_t _active_threads;
    static uint32_t worker_threads;
    static int numa_nodes;
    static const uint32_t MAX_THREADS = 256;
    static const uint64_t MB = 1024 * 1024;
    static const uint64_t GB = MB * 1024;
    static int enable_gc;
    static std::string tmpfs_dir;
    static int htt_is_on;
    static uint32_t max_threads_per_node;
    static bool loading;

    static int log_buffer_mb;
    static int log_segment_mb;
    static std::string log_dir;
    static int null_log_device;
    static int nvram_log_buffer;
    static int group_commit;
    static int group_commit_queue_length;  // how much to reserve
    static sm_log_recover_impl *recover_functor;
    static uint64_t node_memory_gb;

    // Warm-up policy when recovering from a chkpt or the log.
    // Set by --recovery-warm-up=[lazy/eager/whatever].
    //
    // lazy: spawn a thread to access every OID entry after recovery; log/chkpt
    //       recovery will only oid_put objects that contain the records' log location.
    //       Tx's might encounter some storage-resident versions, if the tx tried to
    //       access them before the warm-up thread fetched those versions.
    //
    // eager: dig out versions from the log when scanning the chkpt and log; all OID
    //        entries will point to some memory location after recovery finishes.
    //        Txs will only see memory-residents, no need to dig them out during execution.
    //
    // --recovery-warm-up ommitted or = anything else: don't do warm-up at all; it
    //        is the tx's burden to dig out versions when accessing them.
    enum WU_POLICY { WARM_UP_NONE, WARM_UP_LAZY, WARM_UP_EAGER };
    static int recovery_warm_up_policy;  // no/lazy/eager warm-up at recovery

    /* CC-related options */
    static int enable_ssi_read_only_opt;
    static uint64_t ssn_read_opt_threshold;
    static const uint64_t SSN_READ_OPT_DISABLED = 0xffffffffffffffff;

    // XXX(tzwang): enabling safesnap for tpcc basically halves the performance.
    // perf says 30%+ of cycles are on oid_get_version, which makes me suspect
    // it's because enabling safesnap makes the reader has to go deeper in the
    // version chains to find the desired version. So perhaps don't enable this
    // for update-intensive workloads, like tpcc. TPC-E to test and verify.
    static int enable_safesnap;

    /* Log shipping related options */
    static int wait_for_backups;
    static int num_backups;
    static int num_active_backups;
    static std::string primary_srv;
    static int log_ship_warm_up_policy;
    static int log_ship_by_rdma;

    inline static bool is_backup_srv() {
        return primary_srv.size();
    }

    inline static uint32_t my_thread_id() {
        static __thread uint32_t __id = 0;
        static __thread bool initialized;
        if (not initialized) {
            __id = __sync_fetch_and_add(&_active_threads, 1);
            initialized = true;
        }
        return __id;
    }

    inline static bool eager_warm_up() {
        return loading ?
            recovery_warm_up_policy == WARM_UP_EAGER :
            log_ship_warm_up_policy == WARM_UP_EAGER;
    }

    inline static bool lazy_warm_up() {
        return loading ?
            recovery_warm_up_policy == WARM_UP_LAZY :
            log_ship_warm_up_policy == WARM_UP_LAZY;
    }

    static void init();
    static void sanity_check();
    static inline bool ssn_read_opt_enabled() {
        return ssn_read_opt_threshold < SSN_READ_OPT_DISABLED;
    }
};
