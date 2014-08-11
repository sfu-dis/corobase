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
#include "circbuf.h"
#include "spinbarrier.h"
#include "record/serializer.h"

// forward decl
template <typename Traits> class transaction_proto2;
template <template <typename> class Transaction>
  class txn_epoch_sync;

class transaction_proto2_static {
public:

  // NOTE:
  // each epoch is tied (1:1) to the ticker subsystem's tick. this is the
  // speed of the persistence layer.
  //
  // however, read only txns and GC are tied to multiples of the ticker
  // subsystem's tick

  // tzwang: remove silo's epoch
//#ifdef CHECK_INVARIANTS
//  static const uint64_t ReadOnlyEpochMultiplier = 10; /* 10 * 1 ms */
//#else
//  static const uint64_t ReadOnlyEpochMultiplier = 25; /* 25 * 40 ms */
//  static_assert(ticker::tick_us * ReadOnlyEpochMultiplier == 1000000, "");
//#endif
/*
  static_assert(ReadOnlyEpochMultiplier >= 1, "XX");

  static const uint64_t ReadOnlyEpochUsec =
    ticker::tick_us * ReadOnlyEpochMultiplier;

  static inline uint64_t constexpr
  to_read_only_tick(uint64_t epoch_tick)
  {
    return epoch_tick / ReadOnlyEpochMultiplier;
  }
*/
  // in this protocol, the version number is:
  // (note that for tid_t's, the top bit is reserved and
  // *must* be set to zero
  //
  // [ core  | number |  epoch | reserved ]
  // [ 0..9  | 9..33  | 33..63 |  63..64  ]

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

/*
  static inline ALWAYS_INLINE
  uint64_t EpochId(uint64_t v)
  {
    return (v & EpochMask) >> EpochShift;
  }

  // XXX(stephentu): HACK
  static void
  wait_an_epoch()
  {
    INVARIANT(!rcu::s_instance.in_rcu_region());
    const uint64_t e = to_read_only_tick(
        ticker::s_instance.global_last_tick_exclusive());
    if (!e) {
      std::cerr << "wait_an_epoch(): consistent reads happening in e-1, but e=0 so special case"
                << std::endl;
    } else {
      std::cerr << "wait_an_epoch(): consistent reads happening in e-1: "
                << (e-1) << std::endl;
    }
    while (to_read_only_tick(ticker::s_instance.global_last_tick_exclusive()) == e)
      nop_pause();
    COMPILER_MEMORY_FENCE;
  }
*/
  static uint64_t
  ComputeReadOnlyTid(uint64_t global_tick_ex)
  {
    // FIXME: tzwang: return dummy now for removing silo's epoch, need our tidmgr
    /*
    const uint64_t a = (global_tick_ex / ReadOnlyEpochMultiplier);
    const uint64_t b = a * ReadOnlyEpochMultiplier;

    // want to read entries <= b-1, special casing for b=0
    if (!b)
      return MakeTid(0, 0, 0);
    else
      return MakeTid(CoreMask, NumIdMask >> NumIdShift, b - 1);
    */
    return MakeTid(0, 0, 0);
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
// FIXME: tzwang: removed with our new free_with_fn
/*
  struct delete_entry {
#ifdef CHECK_INVARIANTS
    dbtuple *tuple_ahead_;
    uint64_t trigger_tid_;
#endif

    dbtuple *tuple_;
    marked_ptr<std::string> key_;
    concurrent_btree *btr_;

    delete_entry()
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(nullptr),
        trigger_tid_(0),
#endif
        tuple_(),
        key_(),
        btr_(nullptr) {}

    delete_entry(dbtuple *tuple_ahead,
                 uint64_t trigger_tid,
                 dbtuple *tuple,
                 const marked_ptr<std::string> &key,
                 concurrent_btree *btr)
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(tuple_ahead),
        trigger_tid_(trigger_tid),
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

  struct threadctx {
    uint64_t last_commit_tid_;
    unsigned last_reaped_epoch_;
#ifdef ENABLE_EVENT_COUNTERS
    uint64_t last_reaped_timestamp_us_;
#endif
    px_queue queue_;
    px_queue scratch_;
    std::deque<std::string *> pool_;
    threadctx() :
        last_commit_tid_(0)
      , last_reaped_epoch_(0)
#ifdef ENABLE_EVENT_COUNTERS
      , last_reaped_timestamp_us_(0)
#endif
    {
      ALWAYS_ASSERT(((uintptr_t)this % CACHELINE_SIZE) == 0);
      queue_.alloc_freelist(RCU::NQueueGroups);
      scratch_.alloc_freelist(RCU::NQueueGroups);
    }
  };
*/

  // FIXME: tzwang: removed with silo's epoch
  //static void
  //clean_up_to_including(threadctx &ctx, uint64_t ro_tick_geq);

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

  // FIXME: tzwang: removed with silo's epoch
  //static percore_lazy<threadctx> g_threadctxs;

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
  typedef transaction_base::tid_t tid_t;
  typedef transaction_base::string_type string_type;
  typedef typename super_type::dbtuple_write_info dbtuple_write_info;
  typedef typename super_type::dbtuple_write_info_vec dbtuple_write_info_vec;
  typedef typename super_type::read_set_map read_set_map;
  typedef typename super_type::absent_set_map absent_set_map;
  typedef typename super_type::write_set_map write_set_map;
  typedef typename super_type::write_set_u32_vec write_set_u32_vec;

  transaction_proto2(uint64_t flags,
                     typename Traits::StringAllocator &sa)
    : transaction<transaction_proto2, Traits>(flags, sa)
  {
    /* FIXME: tzwang: nothing todo due to epoch removal.
    if (this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY) {
      const uint64_t global_tick_ex =
        this->rcu_guard_->guard()->impl().global_last_tick_exclusive();
      u_.last_consistent_tid = ComputeReadOnlyTid(global_tick_ex);
    }
    */
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    dbtuple::TupleLockRegionBegin();
#endif
    INVARIANT(RCU::rcu_is_active());
  }

  ~transaction_proto2()
  {
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    dbtuple::AssertAllTupleLocksReleased();
#endif
    INVARIANT(RCU::rcu_is_active());
  }

// FIXME: tzwang: broken b/c epoch removal
  inline bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    /*
    INVARIANT(prev <= cur);

#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
    if (!IsSnapshotsEnabled())
      return true;
#endif

    // XXX(stephentu): the !prev check is a *bit* of a hack-
    // we're assuming that !prev (MIN_TID) corresponds to an
    // absent (removed) record, so it is safe to overwrite it,
    //
    // This is an OK assumption with *no TID wrap around*.
    return (to_read_only_tick(EpochId(prev)) ==
            to_read_only_tick(EpochId(cur))) ||
           !prev;
           */
    return true;
  }

  // can only read elements in this epoch or previous epochs
  inline bool
  can_read_tid(tid_t t) const
  {
    return true;
  }

  inline void
  on_tid_finish(tid_t commit_tid)
  {
    // FIXME: tzwang: this was the entrance of logging, now disabled to get our log
  }

public:

  inline ALWAYS_INLINE bool is_snapshot() const {
    return this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY;
  }

  inline transaction_base::tid_t
  snapshot_tid() const
  {
#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
    if (!IsSnapshotsEnabled())
      // when snapshots are disabled, but we have a RO txn, we simply allow
      // it to read all the latest values and treat them as consistent
      //
      // it's not correct, but its for the factor analysis
      return dbtuple::MAX_TID;
#endif
    return u_.last_consistent_tid;
  }

  void
  dump_debug_info() const
  {
    transaction<transaction_proto2, Traits>::dump_debug_info();
    if (this->is_snapshot())
      std::cerr << "  last_consistent_tid: "
        << g_proto_version_str(u_.last_consistent_tid) << std::endl;
  }

// FIXME: tzwang: need our tidmgr. Returns dummy due to epoch removal.
  transaction_base::tid_t
  gen_commit_tid(const dbtuple_write_info_vec &write_tuples)
  {
    return MakeTid(0, 0, 0);
    /*
    const size_t my_core_id = this->rcu_guard_->guard()->core();
    threadctx &ctx = g_threadctxs.get(my_core_id);
    INVARIANT(!this->is_snapshot());

    COMPILER_MEMORY_FENCE;
    u_.commit_epoch = ticker::s_instance.global_current_tick();
    COMPILER_MEMORY_FENCE;

    tid_t ret = ctx.last_commit_tid_;
    INVARIANT(ret == dbtuple::MIN_TID || CoreId(ret) == my_core_id);
    if (u_.commit_epoch != EpochId(ret))
      ret = MakeTid(0, 0, u_.commit_epoch);

    // What is this? Is txn_proto1_impl used?
    if (g_hack->status_.load(std::memory_order_acquire))
      g_hack->global_tid_.fetch_add(1, std::memory_order_acq_rel);

    // XXX(stephentu): I believe this is correct, but not 100% sure
    //const size_t my_core_id = 0;
    //tid_t ret = 0;
    {
      typename read_set_map::const_iterator it     = this->read_set.begin();
      typename read_set_map::const_iterator it_end = this->read_set.end();
      for (; it != it_end; ++it) {
        if (it->get_tid() > ret)
          ret = it->get_tid();
      }
    }

    {
      typename dbtuple_write_info_vec::const_iterator it     = write_tuples.begin();
      typename dbtuple_write_info_vec::const_iterator it_end = write_tuples.end();
      for (; it != it_end; ++it) {
        INVARIANT(it->tuple->is_locked());
        INVARIANT(it->tuple->is_lock_owner());
        INVARIANT(it->tuple->is_write_intent());
        INVARIANT(!it->tuple->is_modifying());
        INVARIANT(it->tuple->is_latest());
        if (it->is_insert())
          // we inserted this node, so we don't want to do the checks below
          continue;
        const tid_t t = it->tuple->version;

        // XXX(stephentu): we are overly conservative for now- technically this
        // abort isn't necessary (we really should just write the value in the correct
        // position)
        //if (EpochId(t) > u_.commit_epoch) {
        //  std::cerr << "t: " << g_proto_version_str(t) << std::endl;
        //  std::cerr << "epoch: " << u_.commit_epoch << std::endl;
        //  this->dump_debug_info();
        //}

        // t == dbtuple::MAX_TID when a txn does an insert of a new tuple
        // followed by 1+ writes to the same tuple.
        INVARIANT(EpochId(t) <= u_.commit_epoch || t == dbtuple::MAX_TID);
        if (t != dbtuple::MAX_TID && t > ret)
          ret = t;
      }

      INVARIANT(EpochId(ret) == u_.commit_epoch);
      ret = MakeTid(my_core_id, NumId(ret) + 1, u_.commit_epoch);
    }

    // XXX(stephentu): this txn hasn't actually been commited yet,
    // and could potentially be aborted - but it's ok to increase this #, since
    // subsequent txns on this core will read this # anyways
    return (ctx.last_commit_tid_ = ret);
    */
  }

  inline ALWAYS_INLINE void
  on_dbtuple_spill(dbtuple *tuple_ahead, dbtuple *tuple)
  {
#ifdef PROTO2_CAN_DISABLE_GC
    if (!IsGCEnabled())
      return;
#endif

    INVARIANT(RCU::rcu_is_active());
    INVARIANT(!tuple->is_latest());

    // >= not > only b/c of the special case of inserting a new tuple +
    // overwriting the newly inserted record with a longer sequence of bytes in
    // the *same* txn
    INVARIANT(tuple_ahead->version >= tuple->version);

    if (tuple->is_deleting()) {
      INVARIANT(tuple->is_locked());
      INVARIANT(tuple->is_lock_owner());
      // already on queue
      return;
    }

// FIXME: tzwang: epoch removal
/*
    const uint64_t ro_tick = to_read_only_tick(this->u_.commit_epoch);
    INVARIANT(to_read_only_tick(EpochId(tuple->version)) <= ro_tick);

#ifdef CHECK_INVARIANTS
    uint64_t exp = 0;
    INVARIANT(tuple->opaque.compare_exchange_strong(exp, 1, std::memory_order_acq_rel));
#endif

    // when all snapshots are happening >= the current epoch,
    // then we can safely remove tuple
    threadctx &ctx = g_threadctxs.my();
    ctx.queue_.enqueue(
        delete_entry(tuple_ahead, tuple_ahead->version,
          tuple, marked_ptr<std::string>(), nullptr),
        ro_tick);
    */
  }

  inline ALWAYS_INLINE void
  on_logical_delete(dbtuple *tuple, const std::string &key, concurrent_btree *btr)
  {
#ifdef PROTO2_CAN_DISABLE_GC
    if (!IsGCEnabled())
      return;
#endif

    INVARIANT(tuple->is_locked());
    INVARIANT(tuple->is_lock_owner());
    INVARIANT(tuple->is_write_intent());
    INVARIANT(tuple->is_latest());
    INVARIANT(tuple->is_deleting());
    INVARIANT(!tuple->size);
    INVARIANT(RCU::rcu_is_active());
/* FIXME: tzwang: need our own tuple handling. No-op due to epoch removal.
    const uint64_t ro_tick = to_read_only_tick(this->u_.commit_epoch);
    threadctx &ctx = g_threadctxs.my();

#ifdef CHECK_INVARIANTS
    uint64_t exp = 0;
    INVARIANT(tuple->opaque.compare_exchange_strong(exp, 1, std::memory_order_acq_rel));
#endif

    if (likely(key.size() <= tuple->alloc_size)) {
      NDB_MEMCPY(tuple->get_value_start(), key.data(), key.size());
      tuple->size = key.size();

      // eligible for deletion when all snapshots >= the current epoch
      marked_ptr<std::string> mpx;
      mpx.set_flags(0x1);

      ctx.queue_.enqueue(
          delete_entry(nullptr, tuple->version, tuple, mpx, btr),
          ro_tick);
    } else {
      // this is a rare event
      ++g_evt_dbtuple_no_space_for_delkey;
      std::string *spx = nullptr;
      if (ctx.pool_.empty()) {
        spx = new std::string(key.data(), key.size()); // XXX: use numa memory?
      } else {
        spx = ctx.pool_.front();
        ctx.pool_.pop_front();
        spx->assign(key.data(), key.size());
      }
      INVARIANT(spx);

      marked_ptr<std::string> mpx(spx);
      mpx.set_flags(0x1);

      ctx.queue_.enqueue(
          delete_entry(nullptr, tuple->version, tuple, mpx, btr),
          ro_tick);
    }
    */
  }

  // tzwang: so i guess this one is actually rcu_queisce
  // Silo uses it in txn's dtor via the scoped_rcu_guard
  // We do rcu-queisce in scoped_rcu_guard instead
  // So i'll make it a no-op for now
  void
  on_post_rcu_region_completion()
  {
    /*
#ifdef PROTO2_CAN_DISABLE_GC
    if (!IsGCEnabled())
      return;
#endif
    const uint64_t last_tick_ex = ticker::s_instance.global_last_tick_exclusive();
    if (unlikely(!last_tick_ex))
      return;
    // we subtract one from the global last tick, because of the way
    // consistent TIDs are computed, the global_last_tick_exclusive() can
    // increase by at most one tick during a transaction.
    const uint64_t ro_tick_ex = to_read_only_tick(last_tick_ex - 1);
    if (unlikely(!ro_tick_ex))
      // won't have anything to clean
      return;
    // all reads happening at >= ro_tick_geq
    const uint64_t ro_tick_geq = ro_tick_ex - 1;
    threadctx &ctx = g_threadctxs.my();
    clean_up_to_including(ctx, ro_tick_geq);
    */
  }

private:

  union {
    // the global epoch this txn is running in (this # is read when it starts)
    // -- snapshot txns only
    uint64_t last_consistent_tid;
    // the epoch for this txn -- committing non-snapshot txns only
    uint64_t commit_epoch;
  } u_;
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

template <>
struct txn_epoch_sync<transaction_proto2> : public transaction_proto2_static {
  static void
  sync()
  {
    // FIXME: tzwang: no-op due to epoch removal
    // wait_an_epoch();
  }
  static void
  finish()
  {
  }
  static void
  thread_init(bool loader)
  {
  }
  static void
  thread_end()
  {
  }
  /* FIXME: tzwang: remove silo's log
  static std::tuple<uint64_t, uint64_t, double>
  compute_ntxn_persisted()
  {
      return std::make_tuple(0, 0, 0.0);
  }
  static void
  reset_ntxn_persisted()
  {
  }
  */
};

#endif /* _NDB_TXN_PROTO2_IMPL_H_ */
