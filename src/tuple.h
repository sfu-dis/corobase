#ifndef _NDB_TUPLE_H_
#define _NDB_TUPLE_H_

#include <atomic>
#include <vector>
#include <string>
#include <utility>
#include <limits>
#include <unordered_map>
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
  friend std::ostream &
  operator<<(std::ostream &o, const dbtuple &tuple);

public:
  // trying to save space by putting constraints
  // on node maximums
  typedef uint32_t version_t;
  typedef uint16_t node_size_type;
  typedef uint8_t * record_type;
  typedef const uint8_t * const_record_type;
  typedef size_t size_type;
  typedef std::string string_type;

  fat_ptr clsn;
  oid_type oid;
public:

#ifdef TUPLE_MAGIC
  class magic_failed_exception: public std::exception {};
  static const uint8_t TUPLE_MAGIC = 0x29U;
  uint8_t magic;
  inline ALWAYS_INLINE void CheckMagic() const
  {
    if (unlikely(magic != TUPLE_MAGIC)) {
      print(1);
      // so we can catch it later and print out useful debugging output
      throw magic_failed_exception();
    }
  }
#else
  inline ALWAYS_INLINE void CheckMagic() const {}
#endif

  // small sizes on purpose
  node_size_type size; // actual size of record
                       // (only meaningful is the deleting bit is not set)
  node_size_type alloc_size; // max size record allowed. is the space
                             // available for the record buf

#ifdef TUPLE_CHECK_KEY
  // for debugging
  std::string key;
  void *tree;
#endif

#ifdef CHECK_INVARIANTS
  // for debugging
  std::atomic<uint64_t> opaque;
#endif

  // must be last field
  uint8_t value_start[0];

  void print(std::ostream &o, unsigned len) const;

private:
  // private ctor/dtor b/c we do some special memory stuff
  // ctors start node off as latest node

  static inline ALWAYS_INLINE node_size_type
  CheckBounds(size_type s)
  {
    INVARIANT(s <= std::numeric_limits<node_size_type>::max());
    return s;
  }

  dbtuple(const dbtuple &) = delete;
  dbtuple(dbtuple &&) = delete;
  dbtuple &operator=(const dbtuple &) = delete;

  // creates a (new) record with a tentative value at MAX_TID
  dbtuple(size_type size, size_type alloc_size)
    :
#ifdef TUPLE_MAGIC
      magic(TUPLE_MAGIC),
#endif
      size(CheckBounds(size))
      , alloc_size(CheckBounds(alloc_size))
#ifdef TUPLE_CHECK_KEY
      , key()
      , tree(nullptr)
#endif
#ifdef CHECK_INVARIANTS
      , opaque(0)
#endif
  {
    INVARIANT(((char *)this) + sizeof(*this) == (char *) &value_start[0]);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  // creates a record at version derived from base
  // (inheriting its value).
  dbtuple(struct dbtuple *base,
          size_type alloc_size)
    :
#ifdef TUPLE_MAGIC
      magic(TUPLE_MAGIC),
#endif
      size(base->size)
      , alloc_size(CheckBounds(alloc_size))
#ifdef TUPLE_CHECK_KEY
      , key()
      , tree(nullptr)
#endif
#ifdef CHECK_INVARIANTS
      , opaque(0)
#endif
  {
    INVARIANT(size <= alloc_size);
    NDB_MEMCPY(&value_start[0], base->get_value_start(), size);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  // creates a spill record, copying in the *old* value if necessary, but
  // setting the size to the *new* value
  dbtuple(const_record_type r,
          size_type old_size,
          size_type new_size,
          size_type alloc_size,
          bool needs_old_value)
    :
#ifdef TUPLE_MAGIC
      magic(TUPLE_MAGIC),
#endif
      size(CheckBounds(new_size))
      , alloc_size(CheckBounds(alloc_size))
#ifdef TUPLE_CHECK_KEY
      , key()
      , tree(nullptr)
#endif
#ifdef CHECK_INVARIANTS
      , opaque(0)
#endif
  {
    INVARIANT(!needs_old_value || old_size <= alloc_size);
    INVARIANT(new_size <= alloc_size);
    if (needs_old_value)
      NDB_MEMCPY(&value_start[0], r, old_size);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  ~dbtuple();

  static event_avg_counter g_evt_avg_dbtuple_stable_version_spins;
  static event_avg_counter g_evt_avg_dbtuple_lock_acquire_spins;
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
    CheckMagic();
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
  static event_counter g_evt_dbtuple_spills;
  static event_counter g_evt_dbtuple_inplace_buf_insufficient;
  static event_counter g_evt_dbtuple_inplace_buf_insufficient_on_spill;
  static event_avg_counter g_evt_avg_record_spill_len;

public:

  /**
   * Read the record at tid t. Returns true if such a record exists, false
   * otherwise (ie the record was GC-ed, or other reasons). On a successful
   * read, the value @ start_t will be stored in r
   *
   * NB(stephentu): calling stable_read() while holding the lock
   * is an error- this will cause deadlock
   */
  template <typename Reader, typename StringAllocator>
  inline ALWAYS_INLINE ReadStatus
  stable_read(Reader &reader, StringAllocator &sa) const
  {
    if (unlikely(size && !reader(get_value_start(), size, sa)))
      return READ_FAILED;
    return size ? READ_RECORD : READ_EMPTY;
  }

  struct write_record_ret {
    write_record_ret() : head_(), rest_(), forced_spill_() {}
    write_record_ret(dbtuple *head, dbtuple* rest, bool forced_spill)
      : head_(head), rest_(rest), forced_spill_(forced_spill)
    {
      INVARIANT(head);
      INVARIANT(head != rest);
      INVARIANT(!forced_spill || rest);
    }
    dbtuple *head_;
    dbtuple *rest_;
    bool forced_spill_;
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
  static std::string
  VersionInfoStr(version_t v);

}
#if !defined(TUPLE_CHECK_KEY) && \
    !defined(CHECK_INVARIANTS)
//PACKED
#endif
;

#endif /* _NDB_TUPLE_H_ */
