#ifndef _NDB_TXN_PROTO2_IMPL_H_
#define _NDB_TXN_PROTO2_IMPL_H_

#include <iostream>
#include <atomic>
#include <vector>
#include <set>

#include <lz4.h>

#include "txn.h"
#include "txn_impl.h"
#include "txn_btree.h"
#include "macros.h"
#include "pxqueue.h"
#include "circbuf.h"
#include "spinbarrier.h"
#include "record/serializer.h"

// forward decl
template <typename Traits> class transaction_proto2;
template <template <typename> class Transaction>
  class txn_epoch_sync;

class transaction_proto2_static {
public:
  static inline ALWAYS_INLINE
  uint64_t CoreId(uint64_t v)
  {
    return v & CoreMask;
  }

  static inline ALWAYS_INLINE
  uint64_t NumId(uint64_t v)
  {
    return (v & NumIdMask) >> NumIdShift;
  }

  static const uint64_t NBitsNumber = 24;

  // XXX(stephentu): need to implement core ID recycling
  static const size_t CoreBits = NMAXCOREBITS; // allow 2^CoreShift distinct threads
  static const size_t NMaxCores = NMAXCORES;

  static const uint64_t CoreMask = (NMaxCores - 1);

  static const uint64_t NumIdShift = CoreBits;
  static const uint64_t NumIdMask = ((((uint64_t)1) << NBitsNumber) - 1) << NumIdShift;

  static const uint64_t EpochShift = CoreBits + NBitsNumber;
  // since the reserve bit is always zero, we don't need a special mask
  static const uint64_t EpochMask = ((uint64_t)-1) << EpochShift;

  static inline ALWAYS_INLINE
  uint64_t MakeTid(uint64_t core_id, uint64_t num_id, uint64_t epoch_id)
  {
    // some sanity checking
    static_assert((CoreMask | NumIdMask | EpochMask) == ((uint64_t)-1), "xx");
    static_assert((CoreMask & NumIdMask) == 0, "xx");
    static_assert((NumIdMask & EpochMask) == 0, "xx");
    return (core_id) | (num_id << NumIdShift) | (epoch_id << EpochShift);
  }

  static inline void
  set_hack_status(bool hack_status)
  {
    g_hack->status_ = hack_status;
  }

  static inline bool
  get_hack_status()
  {
    return g_hack->status_;
  }

  // thread-safe, can be called many times
  static void InitGC();

  static void PurgeThreadOutstandingGCTasks();

#ifdef PROTO2_CAN_DISABLE_GC
  static inline bool
  IsGCEnabled()
  {
    return g_flags->g_gc_init.load(std::memory_order_acquire);
  }
#endif

#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
  static void
  DisableSnapshots()
  {
    g_flags->g_disable_snapshots.store(true, std::memory_order_release);
  }
  static inline bool
  IsSnapshotsEnabled()
  {
    return !g_flags->g_disable_snapshots.load(std::memory_order_acquire);
  }
#endif

protected:
  // FIXME: tzwang: this is different from the one in RCU namespace!
  // (used by clean_up_memory().)
  // FIXME: tzwang: todo: see how to use trigger_tid (actually tuple's version field).
  struct delete_entry {
#ifdef CHECK_INVARIANTS
    dbtuple *tuple_ahead_;
#endif

    dbtuple *tuple_;
    marked_ptr<std::string> key_;
    concurrent_btree *btr_;

    delete_entry()
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(nullptr),
#endif
        tuple_(),
        key_(),
        btr_(nullptr) {}

    delete_entry(dbtuple *tuple_ahead,
                 dbtuple *tuple,
                 const marked_ptr<std::string> &key,
                 concurrent_btree *btr)
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(tuple_ahead),
#endif
        tuple_(tuple),
        key_(key),
        btr_(btr) {}

    inline dbtuple *
    tuple()
    {
      return tuple_;
    }
  };

  typedef basic_px_queue<delete_entry, 4096> px_queue;

  // FIXME: tzwang: removed all tid/epoch stuff and only keep the basic
  // tuple tracking queues here.
  struct threadctx {
    px_queue queue_;
    px_queue scratch_;
    std::deque<std::string *> pool_;
    threadctx() {
      ALWAYS_ASSERT(((uintptr_t)this % CACHELINE_SIZE) == 0);
      queue_.alloc_freelist(RCU::NQueueGroups);
      scratch_.alloc_freelist(RCU::NQueueGroups);
    }
  };

  // FIXME: tzwang: this was clean_up_to_including. We use it to just
  // 1. free malloc'd memory (basically tuples) and
  // 2. rcu_free rcu_alloc'd memory
  // (while silo used this one to do the cleaning of tuples allocated
  // up to a specified tick (epoch)). We don't have the epochs as in
  // silo, so we just need threadctx, which records all the memory
  // we allocated along the tx execution.
  //
  // P.S., Q: why do need this? (shouldn't rcu handle it already?)
  //       A: tx calls alloc and alloc_rcu on tuples, and it needs a
  //          way to reclaim them when they become garbage. In silo
  //          this was done by 1. recording all allocs in threadctx's
  //          queues, and 2. clean them up in this function. To my
  //          understanding, for rcu allocated ones, silo checks the
  //          epoch and if it's safe to reclaim, call dealloc_rcu.
  //          If it's not rcu allocated, i.e., it's just from the slab
  //          allocator, then just call dealloc. Similarly, we don't
  //          have the epoch stuff, and we aren't really using a slab
  //          allocator, either, so what do based on silo's approach
  //          is just to track those memory in the queues. Then for
  //          rcu-allocated memory, we just do an rcu_free; for others
  //          we can only do delete or free now because we don't have
  //          a fancy slab allocator. (so back to the question, actually
  //          before we can do rcu_free or free, we need to keep track
  //          of those memory allocated first.)
  static void clean_up_memory(threadctx &ctx);

  struct hackstruct {
    std::atomic<bool> status_;
    std::atomic<uint64_t> global_tid_;
    constexpr hackstruct() : status_(false), global_tid_(0) {}
  };

  // use to simulate global TID for comparsion
  static util::aligned_padded_elem<hackstruct>
    g_hack CACHE_ALIGNED;

  struct flags {
    std::atomic<bool> g_gc_init;
    std::atomic<bool> g_disable_snapshots;
    constexpr flags() : g_gc_init(false), g_disable_snapshots(false) {}
  };
  static util::aligned_padded_elem<flags> g_flags;

  // FIXME: tzwang: needed by clean_up_mmeory
  static percore_lazy<threadctx> g_threadctxs;

  static event_counter g_evt_worker_thread_wait_log_buffer;
  static event_counter g_evt_dbtuple_no_space_for_delkey;
  static event_counter g_evt_proto_gc_delete_requeue;
  // static event_avg_counter g_evt_avg_log_entry_size;
  static event_avg_counter g_evt_avg_proto_gc_queue_len;
};

// protocol 2 - no global consistent TIDs
template <typename Traits>
class transaction_proto2 : public transaction<transaction_proto2, Traits>,
                           private transaction_proto2_static {

  friend class transaction<transaction_proto2, Traits>;
  typedef transaction<transaction_proto2, Traits> super_type;

public:

  typedef Traits traits_type;
  typedef transaction_base::string_type string_type;
  typedef typename super_type::read_set_map read_set_map;
  typedef typename super_type::absent_set_map absent_set_map;
  typedef typename super_type::write_set_map write_set_map;
  typedef typename super_type::write_set_u32_vec write_set_u32_vec;

  transaction_proto2(uint64_t flags,
                     typename Traits::StringAllocator &sa)
    : transaction<transaction_proto2, Traits>(flags, sa)
  {
    INVARIANT(RCU::rcu_is_active());
  }

  ~transaction_proto2()
  {
    INVARIANT(RCU::rcu_is_active());
  }

public:
  // FIXME: tzwang: so after removed all tid/epoch stuff, the only
  // thing left is clean up allocated memory when the tx finishes.
  void
  on_post_rcu_region_completion()
  {
    threadctx &ctx = g_threadctxs.my();
    clean_up_memory(ctx);
  }
};

// txn_btree_handler specialization
template <>
struct base_txn_btree_handler<transaction_proto2> {
  static inline void
  on_construct()
  {
#ifndef PROTO2_CAN_DISABLE_GC
    transaction_proto2_static::InitGC();
#endif
  }
  static const bool has_background_task = true;
};
#endif /* _NDB_TXN_PROTO2_IMPL_H_ */
