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
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "prefetch.h"
#include "ownership_checker.h"

#include "reader_writer.h"
#include "dbcore/xid.h"
#include "dbcore/sm-alloc.h"
#include "dbcore/serial.h"

using namespace TXN;

// differentiate with delete case (pvalue = null)
#define DEFUNCT_TUPLE_MARK ((varstr *)0x1)

// Indicate somebody has read this tuple and thought it was an old one
#define PERSISTENT_READER_MARK 0x1

/**
 * A dbtuple is the type of value which we stick
 * into underlying (non-transactional) data structures- it
 * also contains the memory of the value
 */
struct dbtuple {
public:
  typedef uint32_t size_type;
  typedef varstr string_type;

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  readers_list::bitmap_t   readers_bitmap;   // bitmap of in-flight readers
  fat_ptr sstamp;          // successor (overwriter) stamp (\pi in ssn), set to writer XID during
                           // normal write to indicate its existence; become writer cstamp at commit
  uint64_t xstamp;         // access (reader) stamp (\eta), updated when reader commits
  uint8_t preader;         // did I have some reader thinking I'm old?
#endif
#ifdef USE_PARALLEL_SSI
  uint64_t s2;  // smallest successor stamp of all reads performed by the tx
                // that clobbered this version
                // Consider a transaction T which clobbers this version, upon
                // commit, T writes its cstamp in sstamp, and the smallest
                // sstamp among all its reads in s2 of this version. This
                // basically means T has clobbered this version, and meantime,
                // some other transaction C clobbered T's read.
                // So [X] r:w T r:w C. If anyone reads this version again,
                // it will become the X in the dangerous structure above
                // and must abort.
#endif
  size_type size; // actual size of record
  varstr *pvalue;    // points to the value that will be put into value_start if committed
                     // so that read-my-own-update can copy from here.
  uint8_t value_start[0];   // must be last field

private:
  static inline ALWAYS_INLINE size_type
  CheckBounds(size_type s)
  {
    INVARIANT(s <= std::numeric_limits<size_type>::max());
    return s;
  }

  dbtuple(size_type size)
    :
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
      sstamp(NULL_PTR),
      xstamp(0),
      preader(0),
#endif
#ifdef USE_PARALLEL_SSI
      s2(0),
#endif
      size(CheckBounds(size)),
      pvalue(NULL)
  {
#ifdef USE_PARALLEL_SSN
    // FIXME: seems this assumes some 8-byte alignment, which isn't the
    // case when dbtuple is without those ssn-related fields.
    //INVARIANT(((char *)this) + sizeof(*this) == (char *) &value_start[0]);
#endif
  }

  ~dbtuple();

public:
  enum ReadStatus {
    READ_FAILED,
    READ_EMPTY,
    READ_RECORD,
  };

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
  /* return the tuple's age based on a safe_lsn provided by the calling tx.
   * safe_lsn usually = the calling tx's begin offset.
   *
   * and we need to check the type of clsn because a "committed" tx might
   * change the clsn to ASI_LOG type after changing state.
   */
  inline ALWAYS_INLINE uint64_t age(xid_context *visitor) {
    uint64_t end = 0;
    XID owner = volatile_read(visitor->owner);

  retry:
    fat_ptr cstamp = volatile_read(get_object()->_clsn);

    if (cstamp.asi_type() == fat_ptr::ASI_XID) {
      XID xid = XID::from_ptr(cstamp);
      if (xid == owner)   // my own update
        return 0;
      xid_context *xc = xid_get_context(xid);
      end = volatile_read(xc->end).offset();
      if (not xc or xc->owner != xid)
        goto retry;
    }
    else {
      end = cstamp.offset();
    }

    // the caller must be alive...
    return volatile_read(visitor->begin).offset() - end;
  }
  bool is_old(xid_context *visitor);    // FOR READERS ONLY!
  inline ALWAYS_INLINE bool set_persistent_reader() {
    uint64_t pr = 0;
    do {
        pr = volatile_read(preader);
        if (pr >> 7)   // some updater already locked it
            return false;
    }
    while (volatile_read(preader) != PERSISTENT_READER_MARK and
           not __sync_bool_compare_and_swap(&preader, pr, PERSISTENT_READER_MARK));
    return true;
  }
  inline ALWAYS_INLINE bool has_persistent_reader() {
    return volatile_read(preader) & PERSISTENT_READER_MARK;
  }

  // XXX: for the writer who's updating this tuple only
  inline ALWAYS_INLINE void lockout_readers() {
    if (not (volatile_read(preader) >> 7))
        __sync_fetch_and_xor(&preader, uint8_t{1} << 7);
    ASSERT(volatile_read(preader) >> 7);
  }

  // XXX: for the writer who's updating this tuple only
  inline ALWAYS_INLINE void welcome_readers() {
    if (volatile_read(preader) >> 7)
        __sync_fetch_and_xor(&preader, uint64_t{1} << 7);
    ASSERT(not (volatile_read(preader) >> 7));
  }
#endif

  inline void
  prefetch() const
  {
#ifdef TUPLE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + size);
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

  inline ALWAYS_INLINE object*
  get_object()
  {
    object *obj = (object *)((char *)this - sizeof(object));
    ASSERT(obj->payload() == (char *)this);
    return obj;
  }

  inline ALWAYS_INLINE dbtuple*
  next()
  {
    object *myobj = get_object();
    ASSERT(myobj->payload() == (char *)this);
    if (myobj->_next.offset())
      return (dbtuple *)((object *)myobj->_next.offset())->payload();
    else
      return NULL;
  }

  inline ALWAYS_INLINE bool
  is_defunct()
  {
    return volatile_read(pvalue) == DEFUNCT_TUPLE_MARK;
  }

  inline ALWAYS_INLINE void
  mark_defunct()
  {
    // pvalue is only readable by the writer itself,
    // so this won't confuse readers.
    volatile_write(pvalue, DEFUNCT_TUPLE_MARK);
  }

private:

public:
  inline ALWAYS_INLINE ReadStatus
  // Note: the stable=false option will try to read from pvalue,
  // instead of the real data area; so giving stable=false is only
  // safe for the updating transaction itself to read its own write.
  do_read(value_reader &reader, str_arena &sa, bool stable) const
  {
    const uint8_t *data = NULL;
    if (stable)
      data = get_value_start();
    else {
      if (not pvalue) {   // so I just deleted this tuple... return empty?
        ASSERT(not size);
        return READ_EMPTY;
      }
      data = pvalue->data();
      ASSERT(pvalue->size() == size);
    }

    if (unlikely(size && !reader(data, size, sa)))
      return READ_FAILED;
    return size ? READ_RECORD : READ_EMPTY;
  }

  // move data from the user's varstr pvalue to this tuple
  inline ALWAYS_INLINE void
  do_write() const
  {
    if (pvalue) {
      ASSERT(pvalue->size() == size);
      memcpy((void *)get_value_start(), pvalue->data(), pvalue->size());
    }
  }

  static inline dbtuple *
  init(char*p ,size_type sz)
  {
    return new (p) dbtuple(sz);
  }
}
#if !defined(CHECK_INVARIANTS)
//PACKED
#endif
;

#endif /* _NDB_TUPLE_H_ */
