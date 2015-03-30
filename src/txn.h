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
template <template <typename> class Transaction, typename P>
  class base_txn_btree;

//class transaction_unusable_exception {};
//class transaction_read_only_exception {};

// XXX: hacky
extern std::string (*g_proto_version_str)(uint64_t v);

// base class with very simple definitions- nothing too exciting yet
class transaction_base {
  template <template <typename> class T, typename P>
    friend class base_txn_btree;
public:
  static sm_log *logger;

  typedef dbtuple::size_type size_type;
  typedef dbtuple::string_type string_type;
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

#define ABORT_REASONS(x) \
    x(ABORT_REASON_NONE) \
    x(ABORT_REASON_INTERNAL) \
    x(ABORT_REASON_USER) \
    x(ABORT_REASON_UNSTABLE_READ) \
    x(ABORT_REASON_FUTURE_TID_READ) \
    x(ABORT_REASON_NODE_SCAN_WRITE_VERSION_CHANGED) \
    x(ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED) \
    x(ABORT_REASON_WRITE_NODE_INTERFERENCE) \
    x(ABORT_REASON_INSERT_NODE_INTERFERENCE) \
    x(ABORT_REASON_READ_NODE_INTEREFERENCE) \
    x(ABORT_REASON_READ_ABSENCE_INTEREFERENCE) \
    x(ABORT_REASON_VERSION_INTERFERENCE) \
    x(ABORT_REASON_SSN_EXCLUSION_FAILURE)

  enum abort_reason {
#define ENUM_X(x) x,
    ABORT_REASONS(ENUM_X)
#undef ENUM_X
  };

  static const char *
  AbortReasonStr(abort_reason reason)
  {
    switch (reason) {
#define CASE_X(x) case x: return #x;
    ABORT_REASONS(CASE_X)
#undef CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  transaction_base(uint64_t flags)
    : /*reason(ABORT_REASON_NONE),*/
      flags(flags) {}

  transaction_base(const transaction_base &) = delete;
  transaction_base(transaction_base &&) = delete;
  transaction_base &operator=(const transaction_base &) = delete;

protected:
#define EVENT_COUNTER_DEF_X(x) \
  static event_counter g_ ## x ## _ctr;
  ABORT_REASONS(EVENT_COUNTER_DEF_X)
#undef EVENT_COUNTER_DEF_X

  static event_counter *
  AbortReasonCounter(abort_reason reason)
  {
    switch (reason) {
#define EVENT_COUNTER_CASE_X(x) case x: return &g_ ## x ## _ctr;
    ABORT_REASONS(EVENT_COUNTER_CASE_X)
#undef EVENT_COUNTER_CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

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

  //abort_reason reason;
  const uint64_t flags;
};

struct default_transaction_traits {
  static const bool stable_input_memory = false;
  static const bool hard_expected_sizes = false; // true if the expected sizes are hard maximums

  typedef util::default_string_allocator StringAllocator;
};

struct default_stable_transaction_traits : public default_transaction_traits {
  static const bool stable_input_memory = true;
};

template <template <typename> class Protocol, typename Traits>
class transaction : public transaction_base {
  // XXX: weaker than necessary
  template <template <typename> class, typename>
    friend class base_txn_btree;
  friend Protocol<Traits>;

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

  typedef Traits traits_type;
  typedef typename Traits::StringAllocator string_allocator_type;

protected:
  // data structures

  inline ALWAYS_INLINE Protocol<Traits> *
  cast()
  {
    return static_cast<Protocol<Traits> *>(this);
  }

  inline ALWAYS_INLINE const Protocol<Traits> *
  cast() const
  {
    return static_cast<const Protocol<Traits> *>(this);
  }

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
    write_record_t(dbtuple *n, dbtuple::tuple_writer_t w,
                   concurrent_btree *b, oid_type o) :
        new_tuple(n), writer(w), btr(b), oid(o) {}
    write_record_t() : new_tuple(NULL), writer(), btr(NULL), oid(0) {}
    dbtuple *new_tuple;
    dbtuple::tuple_writer_t writer;
    concurrent_btree *btr;
    oid_type oid;
  };

  typedef std::vector<write_record_t> write_set_map;
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  typedef std::vector<dbtuple *> read_set_map;
#endif

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

  inline transaction(uint64_t flags, string_allocator_type &sa);
  inline ~transaction();

  rc_t commit();
#ifdef USE_PARALLEL_SSN
  rc_t parallel_ssn_commit();
#elif defined USE_PARALLEL_SSI
  rc_t parallel_ssi_commit();
#else
  rc_t si_commit();
#endif

#ifdef PHANTOM_PROT_NODE_SET
  bool check_phantom();
#endif

  // signal the caller that an abort is necessary by throwing an abort
  // exception. 
  //void __attribute__((noreturn))
  //signal_abort(abort_reason r=ABORT_REASON_USER);

  // if an abort has been signaled, perform the actual abort and clean
  // up. always succeeds, so caller should rethrow if needed.
  inline void
  abort()
  {
    abort_impl();
  }

  void dump_debug_info() const;
/*
#ifdef DIE_ON_ABORT
  void
  abort_trap(abort_reason reason)
  {
    AbortReasonCounter(reason)->inc();
    this->reason = reason; // for dump_debug_info() to see
    dump_debug_info();
    ::abort();
  }
#else
  inline ALWAYS_INLINE void
  abort_trap(abort_reason reason)
  {
    AbortReasonCounter(reason)->inc();
  }
#endif
*/
protected:
  void abort_impl();

  bool
  try_insert_new_tuple(
      concurrent_btree *btr,
      const varstr *key,
      const varstr *value,
      object* object,
      dbtuple::tuple_writer_t writer);

  // reads the contents of tuple into v
  // within this transaction context
  template <typename ValueReader>
  rc_t
  do_tuple_read(dbtuple *tuple, ValueReader &value_reader);

#ifdef PHANTOM_PROT_NODE_SET
  rc_t
  do_node_read(const typename concurrent_btree::node_opaque_t *n, uint64_t version);
#endif

public:
  // expected public overrides

  inline string_allocator_type &
  string_allocator()
  {
    return *sa;
  }

protected:
  XID xid;
  xid_context *xc;
  sm_tx_log* log;
  string_allocator_type *sa;
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  read_set_map read_set;
#endif
  write_set_map write_set;
#ifdef ENABLE_GC
  object_pool *op;
  epoch_num epoch;
#endif
};

/*
class transaction_abort_exception : public std::exception {
public:
  transaction_abort_exception(transaction_base::abort_reason r)
    : r(r) {}
  inline transaction_base::abort_reason
  get_reason() const
  {
    return r;
  }
  virtual const char *
  what() const throw()
  {
    return transaction_base::AbortReasonStr(r);
  }
private:
  transaction_base::abort_reason r;
};
*/
#endif /* _NDB_TXN_H_ */
