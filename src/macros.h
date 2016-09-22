#pragma once

#include "dbcore/sm-defs.h"

//#define USE_DYNARRAY_STR_ARENA

//#define TUPLE_PREFETCH
#define BTREE_NODE_PREFETCH
#define USE_BUILTIN_MEMFUNCS
#define BTREE_NODE_ALLOC_CACHE_ALIGNED
#define TXN_BTREE_DUMP_PURGE_STATS
//#define ENABLE_BENCH_TXN_COUNTERS
#define USE_VARINT_ENCODING
//#define DISABLE_FIELD_SELECTION
//#define BTREE_LOCK_OWNERSHIP_CHECKING

#define LG_CACHELINE_SIZE __builtin_ctz(CACHELINE_SIZE)

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT  \
    char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

#define NEVER_INLINE  __attribute__((noinline))
#define ALWAYS_INLINE __attribute__((always_inline))
#define UNUSED __attribute__((unused))

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

#ifdef NDEBUG
  #define ALWAYS_ASSERT(expr) (likely((expr)) ? (void)0 : abort())
#else
  #define ALWAYS_ASSERT(expr) ASSERT((expr))
#endif /* NDEBUG */

#define ARRAY_NELEMS(a) (sizeof(a)/sizeof((a)[0]))

#define VERBOSE(expr) ((void)0)
//#define VERBOSE(expr) (expr)

// tune away
#define SMALL_SIZE_VEC       128
#define SMALL_SIZE_MAP       64
#define EXTRA_SMALL_SIZE_MAP 8

//#define BACKOFF_SPINS_FACTOR 1000
//#define BACKOFF_SPINS_FACTOR 100
#define BACKOFF_SPINS_FACTOR 10

// throw exception after the assert(), so that GCC knows
// we'll never return
#define NDB_UNIMPLEMENTED(what) \
  do { \
    ALWAYS_ASSERT(false); \
    throw ::std::runtime_error(what); \
  } while (0)

#ifdef USE_BUILTIN_MEMFUNCS
#define NDB_MEMCPY __builtin_memcpy
#define NDB_MEMSET __builtin_memset
#else
#define NDB_MEMCPY memcpy
#define NDB_MEMSET memset
#endif
