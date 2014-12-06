#ifndef _NDB_TUPLE_H_
#define _NDB_TUPLE_H_

#include <atomic>
#include <vector>
#include <string>
#include <utility>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <ostream>
#include <thread>

#include "amd64.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "allocator.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "prefetch.h"
#include "ownership_checker.h"

#include "object.h"
#include "dbcore/xid.h"

#include "dbcore/sm-alloc.h"

using namespace TXN;

template <template <typename> class Protocol, typename Traits>
  class transaction; // forward decl

// XXX: hack
extern std::string (*g_proto_version_str)(uint64_t v);

/**
 * A dbtuple is the type of value which we stick
 * into underlying (non-transactional) data structures- it
 * also contains the memory of the value
 */
struct dbtuple {
public:
  typedef size_t size_type;
  typedef std::string string_type;

  struct xid_hash {
    size_t operator()(const XID &x) const {
      return (uint64_t)x._val;
    }
  };

  oid_type oid;
  fat_ptr clsn;     // version creation stamp
  LSN xlsn;         // access (reader) stamp (\eta), updated when reader commits
  LSN slsn;         // successor (overwriter) stamp (\pi), updated when writer commits
  std::unordered_set<XID, xid_hash> readers;  // list of readers for overwriters of this tuple to examine (ssn)
  std::mutex readers_mutex;

public:
  size_type size; // actual size of record
  size_type alloc_size; // allocated (aligned) size

  // must be last field
  uint8_t value_start[0];

private:
  static inline ALWAYS_INLINE size_type
  CheckBounds(size_type s)
  {
    INVARIANT(s <= std::numeric_limits<size_type>::max());
    return s;
  }

  dbtuple(size_type size, size_type alloc_size)
    :
      clsn(NULL_PTR)
      , xlsn(INVALID_LSN)
      , slsn(INVALID_LSN)
      , size(CheckBounds(size))
      , alloc_size(CheckBounds(alloc_size))
  {
    INVARIANT(((char *)this) + sizeof(*this) == (char *) &value_start[0]);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  ~dbtuple();

  static event_avg_counter g_evt_avg_dbtuple_read_retries;

public:

  enum ReadStatus {
    READ_FAILED,
    READ_EMPTY,
    READ_RECORD,
  };

  inline void
  prefetch() const
  {
#ifdef TUPLE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + alloc_size);
#endif
  }

  inline ALWAYS_INLINE uint8_t *
  get_value_start()
  {
    return &value_start[0];
  }

  inline ALWAYS_INLINE const uint8_t *
  get_value_start() const
  {
    return &value_start[0];
  }

private:

#ifdef ENABLE_EVENT_COUNTERS
  struct scoped_recorder {
    scoped_recorder(unsigned long &n) : n(&n) {}
    ~scoped_recorder()
    {
      g_evt_avg_dbtuple_read_retries.offer(*n);
    }
  private:
    unsigned long *n;
  };
#endif

  static event_counter g_evt_dbtuple_creates;
  static event_counter g_evt_dbtuple_bytes_allocated;
  static event_counter g_evt_dbtuple_bytes_freed;

public:
  template <typename Reader, typename StringAllocator>
  inline ALWAYS_INLINE ReadStatus
  stable_read(Reader &reader, StringAllocator &sa) const
  {
    if (unlikely(size && !reader(get_value_start(), size, sa)))
      return READ_FAILED;
    return size ? READ_RECORD : READ_EMPTY;
  }

  struct write_record_ret {
    write_record_ret() : head_(), rest_() {}
    write_record_ret(dbtuple *head, dbtuple* rest)
      : head_(head), rest_(rest)
    {
      INVARIANT(head);
      INVARIANT(head != rest);
      INVARIANT(rest);
    }
    dbtuple *head_;
    dbtuple *rest_;
  };

  // XXX: kind of hacky, but we do this to avoid virtual
  // functions / passing multiple function pointers around
  enum TupleWriterMode {
    TUPLE_WRITER_NEEDS_OLD_VALUE, // all three args ignored
    TUPLE_WRITER_COMPUTE_NEEDED,
    TUPLE_WRITER_COMPUTE_DELTA_NEEDED, // last two args ignored
    TUPLE_WRITER_DO_WRITE,
    TUPLE_WRITER_DO_DELTA_WRITE,
  };
  typedef size_t (*tuple_writer_t)(TupleWriterMode, const void *, uint8_t *, size_t);

  static inline dbtuple *
  init( char*p ,size_type sz, size_type alloc_sz)
  {
    return new (p) dbtuple(sz, alloc_sz - sizeof(dbtuple));
  }
}
#if !defined(CHECK_INVARIANTS)
//PACKED
#endif
;

#endif /* _NDB_TUPLE_H_ */
