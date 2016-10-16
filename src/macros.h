#pragma once

#include "dbcore/sm-defs.h"

//#define TUPLE_PREFETCH
#define USE_BUILTIN_MEMFUNCS
//#define ENABLE_BENCH_TXN_COUNTERS
#define USE_VARINT_ENCODING
//#define DISABLE_FIELD_SELECTION

#define LG_CACHELINE_SIZE __builtin_ctz(CACHELINE_SIZE)

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT  \
    char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

#define ALWAYS_INLINE __attribute__((always_inline))
#define UNUSED __attribute__((unused))

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

#define ALWAYS_ASSERT(expr) (likely((expr)) ? (void)0 : abort())

#define ARRAY_NELEMS(a) (sizeof(a)/sizeof((a)[0]))

#define VERBOSE(expr) ((void)0)
//#define VERBOSE(expr) (expr)

// tune away
#define SMALL_SIZE_MAP       64

#ifdef USE_BUILTIN_MEMFUNCS
#define NDB_MEMCPY __builtin_memcpy
#define NDB_MEMSET __builtin_memset
#else
#define NDB_MEMCPY memcpy
#define NDB_MEMSET memset
#endif

#define NOP_PAUSE asm volatile("pause" : :)
