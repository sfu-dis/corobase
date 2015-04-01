#ifndef _NDB_TXN_H_
#define _NDB_TXN_H_

#include <malloc.h>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

#include <map>
#include <iostream>
#include <vector>
#include <string>
#include <utility>
#include <stdexcept>
#include <limits>
#include <type_traits>
#include <tuple>

#include <unordered_map>
#include "dbcore/xid.h"
#include "dbcore/serial.h"
#include "dbcore/sm-log.h"
#include "dbcore/sm-trace.h"
#include "dbcore/sm-rc.h"
#include "amd64.h"
#include "btree_choice.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "static_unordered_map.h"
#include "static_vector.h"
#include "prefetch.h"
#include "tuple.h"
#include "scopedperf.hh"
#include "marked_ptr.h"
#include "ndb_type_traits.h"
#include "object.h"

#include <sparsehash/dense_hash_map>
using google::dense_hash_map;

using namespace TXN;

// forward decl
class base_txn_btree;

// XXX: hacky
extern std::string (*g_proto_version_str)(uint64_t v);

// base class with very simple definitions- nothing too exciting yet
class transaction_base {
  friend class base_txn_btree;
public:
  static sm_log *logger;

  typedef dbtuple::size_type size_type;
  typedef TXN::txn_state txn_state;

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, a transaction_read_only_exception is thrown and the
    // txn is aborted
    TXN_FLAG_READ_ONLY = 0x2,

    // XXX: more flags in the future, things like consistency levels
  };

  transaction_base(uint64_t flags)
    : flags(flags) {}

  transaction_base(const transaction_base &) = delete;
  transaction_base(transaction_base &&) = delete;
  transaction_base &operator=(const transaction_base &) = delete;

public:

  inline uint64_t
  get_flags() const
  {
    return flags;
  }

protected:
  static event_counter g_evt_read_logical_deleted_node_search;
  static event_counter g_evt_read_logical_deleted_node_scan;
  static event_counter g_evt_dbtuple_write_search_failed;
  static event_counter g_evt_dbtuple_write_insert_failed;

  static event_counter evt_local_search_lookups;
  static event_counter evt_dbtuple_latest_replacement;

  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe0, g_txn_commit_probe0_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe1, g_txn_commit_probe1_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe2, g_txn_commit_probe2_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe3, g_txn_commit_probe3_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe4, g_txn_commit_probe4_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe5, g_txn_commit_probe5_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe6, g_txn_commit_probe6_cg);

  const uint64_t flags;
};

class transaction : public transaction_base {
  // XXX: weaker than necessary
  friend class base_txn_btree;

public:
  // KeyWriter is expected to implement:
  // [1-arg constructor]
  //   KeyWriter(const Key *)
  // [fully materialize]
  //   template <typename StringAllocator>
  //   const std::string * fully_materialize(bool, StringAllocator &)

  // ValueWriter is expected to implement:
  // [1-arg constructor]
  //   ValueWriter(const Value *, ValueInfo)
  // [compute new size from old value]
  //   size_t compute_needed(const uint8_t *, size_t)
  // [fully materialize]
  //   template <typename StringAllocator>
  //   const std::string * fully_materialize(bool, StringAllocator &)
  // [perform write]
  //   void operator()(uint8_t *, size_t)
  //
  // ValueWriter does not have to be move/copy constructable. The value passed
  // into the ValueWriter constructor is guaranteed to be valid throughout the
  // lifetime of a ValueWriter instance.

  // KeyReader Interface
  //
  // KeyReader is a simple transformation from (const std::string &) => const Key &.
  // The input is guaranteed to be stable, so it has a simple interface:
  //
  //   const Key &operator()(const std::string &)
  //
  // The KeyReader is expect to preserve the following property: After a call
  // to operator(), but before the next, the returned value is guaranteed to be
  // valid and remain stable.

  // ValueReader Interface
  //
  // ValueReader is a more complex transformation from (const uint8_t *, size_t) => Value &.
  // The input is not guaranteed to be stable, so it has a more complex interface:
  //
  //   template <typename StringAllocator>
  //   bool operator()(const uint8_t *, size_t, StringAllocator &)
  //
  // This interface returns false if there was not enough buffer space to
  // finish the read, true otherwise.  Note that this interface returning true
  // does NOT mean that a read was stable, but it just means there were enough
  // bytes in the buffer to perform the tentative read.
  //
  // Note that ValueReader also exposes a dup interface
  //
  //   template <typename StringAllocator>
  //   void dup(const Value &, StringAllocator &)
  //
  // ValueReader also exposes a means to fetch results:
  //
  //   Value &results()
  //
  // The ValueReader is expected to preserve the following property: After a
  // call to operator(), if it returns true, then the value returned from
  // results() should remain valid and stable until the next call to
  // operator().

protected:
  inline txn_state state() const
  {
    return xc->state;
  }

  // only fires during invariant checking
  inline void
  ensure_active()
  {
    if (state() == TXN_EMBRYO)
      volatile_write(xc->state, TXN_ACTIVE);
    INVARIANT(state() == TXN_ACTIVE);
  }

  struct write_record_t {
    write_record_t(dbtuple *n, concurrent_btree *b, oid_type o) :
        new_tuple(n), btr(b), oid(o) {}
    write_record_t() : new_tuple(NULL), btr(NULL), oid(0) {}
    dbtuple *new_tuple;
    concurrent_btree *btr;
    oid_type oid;
  };

  typedef std::vector<write_record_t> write_set_map;

#ifdef PHANTOM_PROT_TABLE_LOCK
  typedef std::vector<table_lock_t*> table_lock_set_t;
  table_lock_set_t table_locks;
#elif defined(PHANTOM_PROT_NODE_SET)
  // the absent set is a mapping from (btree_node -> version_number).
  struct absent_record_t { uint64_t version; };
  typedef dense_hash_map<const concurrent_btree::node_opaque_t*, absent_record_t> absent_set_map;
  absent_set_map absent_set;
#endif

public:

  transaction(uint64_t flags, str_arena &sa);
  ~transaction();

  rc_t commit();
#ifdef USE_PARALLEL_SSN
  rc_t parallel_ssn_commit();
  rc_t ssn_read(dbtuple *tuple);
#elif defined USE_PARALLEL_SSI
  rc_t parallel_ssi_commit();
  rc_t ssi_read(dbtuple *tuple);
#else
  rc_t si_commit();
#endif

#ifdef PHANTOM_PROT_NODE_SET
  bool check_phantom();
#endif

  // if an abort has been signaled, perform the actual abort and clean
  // up. always succeeds, so caller should rethrow if needed.
  inline void
  abort()
  {
    abort_impl();
  }

  void dump_debug_info() const;

protected:
  void abort_impl();

  bool
  try_insert_new_tuple(
      concurrent_btree *btr,
      const varstr *key,
      const varstr *value,
      object* object);

  // reads the contents of tuple into v
  // within this transaction context
  rc_t
  do_tuple_read(dbtuple *tuple, value_reader &value_reader);

#ifdef PHANTOM_PROT_NODE_SET
  rc_t
  do_node_read(const typename concurrent_btree::node_opaque_t *n, uint64_t version);
#endif

public:
  // expected public overrides

  inline str_arena &
  string_allocator()
  {
    return *sa;
  }

protected:
  XID xid;
  xid_context *xc;
  sm_tx_log* log;
  str_arena *sa;
  write_set_map write_set;
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  typedef std::vector<dbtuple *> read_set_map;
  read_set_map read_set;
#endif
#ifdef ENABLE_GC
  object_pool *op;
  epoch_num epoch;
#endif
};
#endif /* _NDB_TXN_H_ */
