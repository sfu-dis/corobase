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

protected:

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

  static event_counter g_evt_worker_thread_wait_log_buffer;
  static event_counter g_evt_dbtuple_no_space_for_delkey;
  static event_counter g_evt_proto_gc_delete_requeue;
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
