#pragma once

#define USE_VARINT_ENCODING

#define LG_CACHELINE_SIZE __builtin_ctz(CACHELINE_SIZE)

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT  \
    char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

#ifndef ALWAYS_INLINE
#define ALWAYS_INLINE __attribute__((always_inline)) inline
#endif

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#ifndef ALWAYS_ASSERT
#define ALWAYS_ASSERT(expr) (likely((expr)) ? (void)0 : abort())
#endif

#define MARK_REFERENCED(x) (void)x

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

#define ARRAY_NELEMS(a) (sizeof(a)/sizeof((a)[0]))

#define VERBOSE(expr) ((void)0)
//#define VERBOSE(expr) (expr)

#define NOP_PAUSE asm volatile("pause" : :)
