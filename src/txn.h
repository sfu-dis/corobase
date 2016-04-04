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
#include "dbcore/sm-config.h"
#include "dbcore/sm-oid.h"
#include "dbcore/sm-log.h"
#include "dbcore/sm-rc.h"
#include "amd64.h"
#include "btree_choice.h"
#include "core.h"
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
#include "ndb_type_traits.h"
#include "object.h"

#include <sparsehash/dense_hash_map>
using google::dense_hash_map;

using namespace TXN;

// forward decl
class base_txn_btree;

class transaction {
  // XXX: weaker than necessary
  friend class base_txn_btree;
  friend class sm_oid_mgr;

public:
  typedef dbtuple::size_type size_type;
  typedef TXN::txn_state txn_state;

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, it is aborted. SSN uses it to implement to safesnap.
    // No bookeeping is done with SSN if this is enable for a tx.
    TXN_FLAG_READ_ONLY = 0x2,

    TXN_FLAG_READ_MOSTLY = 0x3,

    // XXX: more flags in the future, things like consistency levels
  };

  inline bool is_read_mostly() { return flags & TXN_FLAG_READ_MOSTLY; }
  inline bool is_read_only() { return flags & TXN_FLAG_READ_ONLY; }

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
    write_record_t(object *obj, oid_array *a, OID o) :
        new_object(obj), oa(a), oid(o) {}
    object *new_object;
    oid_array *oa;
    OID oid;
  };

  typedef std::vector<write_record_t> write_set_map;

#ifdef PHANTOM_PROT_NODE_SET
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

  void abort();

  void dump_debug_info() const;

protected:
  bool
  try_insert_new_tuple(
      concurrent_btree *btr,
      const varstr *key,
      const varstr *value,
      FID fid);

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

  inline uint64_t
  get_flags() const
  {
    return flags;
  }

protected:
  const uint64_t flags;
  XID xid;
  xid_context *xc;
  sm_tx_log* log;
  str_arena *sa;
  write_set_map write_set;
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  typedef std::vector<dbtuple *> read_set_map;
  read_set_map read_set;
#endif
#ifdef REUSE_OBJECTS
  object_pool *op;
#endif
  epoch_num epoch;
};
#endif /* _NDB_TXN_H_ */
