/* An efficient implementation of the adler32 checksum algorithm.

   Scalar version courtesy of
   http://stackoverflow.com/questions/421419/good-choice-for-a-lightweight-checksum-algorithm

   WARNING: adler32 is wickedly fast, but has several significant
   weaknesses [1]. Perhaps the most troubling one for our purposes is
   the fact that it performs poorly for small inputs (where "small" is
   sub-kB range).

   [1] http://guru.multimedia.cx/crc32-vs-adler32/
 */

#include "adler.h"

#include "sm-defs.h"
#include "sm-exceptions.h"

struct adler32_nop_op {
  template <typename T>
  void operator()(T) {}
};

struct adler32_memcpy_op {
  char *_dest;
  adler32_memcpy_op(char *dest, char const *src) : _dest(dest) {
    int dalign = 0xf & (uintptr_t)dest;
    int salign = 0xf & (uintptr_t)src;
    THROW_IF(dalign != salign, illegal_argument,
             "Incompatible relative alignment between src and dest buffers (%d "
             "vs %d)",
             salign, dalign);
  }

  template <typename T>
  void operator()(T data) {
    *(T *)_dest = data;
    _dest += sizeof(T);
  }
};

static uint64_t const MOD_ADLER = 65521;

template <typename Op>
void adler32_single(uint64_t &a, uint64_t &b, char const *data, Op &op) {
  op(*data);
  a += (uint8_t)*data;
  b += a;
}

template <typename Op>
uint32_t 
#ifndef __clang__
__attribute__((optimize("unroll-loops")))
#endif
adler32_finish(uint64_t a, uint64_t b, char const *data, size_t i,
               size_t nbytes, Op &op) {
#ifdef __clang__
#pragma clang loop unroll(enable)
#endif
  for (; i < nbytes; i++) adler32_single(a, b, data + i, op);

  a %= MOD_ADLER;
  b %= MOD_ADLER;
  return (b << 16) | a;
}

uint32_t adler32_merge(uint32_t left, uint32_t right, size_t right_size) {
  uint64_t a = left & 0xffff;
  uint64_t b = left >> 16;
  uint64_t d1 = right & 0xffff;
  uint64_t d2 = right >> 16;
  adler32_nop_op op;

  /* Gotcha: if two sums were computed independently, the one on the
     right wrongly included an initial value of a=1 (rather than the
     true [a] we're now inheriting from the one on the left).
   */
  a += MOD_ADLER - 1;
  b += right_size * a + d2;
  a += d1;
  return adler32_finish(a, b, 0, 0, 0, op);
}

template <typename Op>
uint32_t
#ifndef __clang__
__attribute__((optimize("unroll-loops")))
#endif
adler32_vanilla(char const *data, size_t nbytes, uint32_t sofar, Op &op) {
  return adler32_finish(sofar & 0xffff, sofar >> 16, data, 0, nbytes, op);
}

uint32_t __attribute__((flatten))
adler32_vanilla(char const *data, size_t nbytes, uint32_t sofar) {
  adler32_nop_op op;
  return adler32_vanilla(data, nbytes, sofar, op);
}

uint32_t __attribute__((flatten))
adler32_memcpy_vanilla(char *dest, char const *src, size_t nbytes,
                       uint32_t sofar) {
  adler32_memcpy_op op(dest, src);
  return adler32_vanilla(src, nbytes, sofar, op);
}

#ifdef __SSSE3__
#include <x86intrin.h>

typedef __v4si v4si;
typedef __v8hi v8hi;
typedef __v16qi v16qi;
static v8hi const ZERO = {0, 0, 0, 0, 0, 0, 0, 0};
static v4si const DZERO = {0, 0, 0, 0};

static v16qi const C2 = {16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
static v16qi const BONE = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
static v16qi const B16 = {16, 16, 16, 16, 16, 16, 16, 16,
                          16, 16, 16, 16, 16, 16, 16, 16};
static v8hi const ONE = {1, 1, 1, 1, 1, 1, 1, 1};

template <typename Op>
void __attribute__((optimize("unroll-loops")))
adler32_sse_chunk4_fast(v4si &a, v4si &b, v16qi const *bx, Op &op) {
/*
uint64_t
    d0=x[0], d1=x[1], d2=x[2], d3=x[3],
    d4=x[4], d5=x[5], d6=x[6], d7=x[7],
    d8=x[8], d9=x[9], da=x[10], db=x[11],
    dc=x[12], dd=x[13], de=x[14], df=x[15];

a = d0 + d1 + d2 + d3 + d4 + ... + dc + dd + de + df;

     ...
    (64      64
    (48      47
    (32*d0 + 31*d1) + (30*d2 + 29+d3) + ... + (18*de + 17*df);
b = (16*d0 + 15*d1) + (14*d2 + 13*d3) + ... +  (2*de +  1*df);

b += 16*a + c2*bx[1]
a += bx[1];

In this case, size of s2 grows as

 31 = 15+16
 94 = 15+16+31+32
189 = 15+16+31+32+47+48
316 = 15+16+31+32+47+48+63+64

We can split that apart, though:

s2 =         c2*bx[0]	s2x = c2*bx[0] 		s1  = bx[0]
s1 = bx[0]                  s1  = bx[0]             s2x = c2*bx[0]

                            s2y = 16*s1             s2y = 16*s1
s2 += 16*s1 + c2*bx[1]      s2x += c2*bx[1]         s1  += bx[1]
s1 += bx[1]                 s1  += bx[1]            s2x = c2*bx[1]

                            s2y += 16*s1            s2y += 16*s1
s2 += 16*s1 + c2*bx[2]      s2x += c2*bx[2]         s1  += bx[2]
s1 += bx[2]                 s1  += bx[2]            s2x = c2*bx[2]

                            s2y += 16*s1            s2y += 16*s1
s2 += 16*s1 + c2*bx[3]      s2x += c2*bx[3]         s1  += bx[3]
s1 += bx[3]                 s1  += bx[3]            s2x = c2*bx[3]

                            s2  = s2x + s2y         s2  = s2x + s2y

It would be tempting to factor out c2*(bx[0] + ... + bx[3]), but
doing so would preclude using the MADD intrinsic that lets us
avoid pack/unpack operations. In any case, splitting the
computation this way reduces growth of s2y enough to allow four
rounds without overflow.

NOTE: The MADD operation we exploit is signed/saturating, but we
never multiply by more than 16. The accumulations are done as
normal (non-saturating) signed additions, so we can (as usual)
tolerate up to size 257.
*/
#define MADD(a, b) ((v8hi)__builtin_ia32_pmaddubsw128((a), (b)))
  v8hi s1, s2x, s2y;

  s2y = ZERO;
  s1 = MADD(bx[0], B16);
  s2x = MADD(bx[0], C2);
  op(bx[0]);
  // 0,32,31

  s2y += s1;
  s1 += MADD(bx[1], B16);
  s2x += MADD(bx[1], C2);
  op(bx[1]);
  // 32,64,62

  s2y += s1;
  s1 += MADD(bx[2], B16);
  s2x += MADD(bx[2], C2);
  op(bx[2]);
  // 96,96,93

  s2y += s1;
  s1 += MADD(bx[3], B16);
  s2x += MADD(bx[3], C2);
  op(bx[3]);
  // 192,128,124

  /* The final computation is:

     b += 64*a + sum(s2x) + sum(s2y)
     a += sum(s1)

     S1 can be reduced directly to a scalar (8*8 = 64 < 257) once
     we've scaled it back down where it belongs (we computed 16*s1
     to make s2y happy). S2x will overflow if we reduce more than
     one step. S2y is about to overflow, so we have to convert it to
     a pair of 32-bit vectors before starting the reduction.
   */
  s1 >>= 4;

  // reduce s1 to 4 int-sized values.
  v4si d1 = __builtin_ia32_pmaddwd128(s1, ONE);

  /* unpack the 4 s2_delta values to 32-bit integers. Unpack s2y
     while we're at it.

     It would be nice to use __builtin_ia32_pmaddwd128 for s2y, but
     that intrinsic treats both operands as signed (unlike the
     byte-based), and s2y is large enough to use the "sign" bit.
  */
  v4si d2 = (v4si)__builtin_ia32_pmaddwd128(s2x, ONE);
  d2 += (v4si)__builtin_ia32_punpcklwd128(s2y, ZERO);
  d2 += (v4si)__builtin_ia32_punpckhwd128(s2y, ZERO);

  // don't forget all those times [a] would have been added...
  b += 64 * a + d2;
  a += d1;
}

template <typename Op>
void adler32_sse_chunk1_fast(v4si &a, v4si &b, v16qi const *bx, Op &op) {
  v8hi s1 = MADD(bx[0], BONE);
  v8hi s2 = MADD(bx[0], C2);
  op(bx[0]);
  // 2,31

  /* The final computation is:

     b += 64*a + sum(s2)
     a += sum(s1)
  */

  v4si d1 = __builtin_ia32_pmaddwd128(s1, ONE);
  v4si d2 = __builtin_ia32_pmaddwd128(s2, ONE);

  // don't forget to count all those times [a] would have been added...
  b += 16 * a + d2;
  a += d1;
}

static void adler32_sse_reduce(uint64_t &a, uint64_t &b, v4si &va, v4si &vb,
                               int k) {
  // 4/4 -> 2/2 -> 1/1
  va = __builtin_ia32_phaddd128(va, vb);
  va = __builtin_ia32_phaddd128(va, va);
  ASSERT(a <= 0xffffffff and b <= 0xffffffff);
  b += k * a + va[1];
  a += va[0];
  a = ((uint32_t)a) % MOD_ADLER;
  b = ((uint32_t)b) % MOD_ADLER;
  ASSERT(a <= 0xffffffff and b <= 0xffffffff);
}

template <typename Op>
uint32_t __attribute__((optimize("unroll-loops")))
adler32_finish_sse(uint64_t a, uint64_t b, char const *data, size_t i,
                   size_t nbytes, Op &op) {
  for (; i < nbytes; i++) adler32_single(a, b, data + i, op);

  ASSERT(a <= 0xffffffff and b <= 0xffffffff);
  b = ((uint32_t)b) % MOD_ADLER;
  a = ((uint32_t)a) % MOD_ADLER;
  ASSERT(b < MOD_ADLER and a < MOD_ADLER);
  return (b << 16) | a;
}

template <typename Op>
uint32_t adler32_sse(char const *data, size_t nbytes, uint32_t sofar, Op &op) {
  uint64_t a = sofar & 0xffff, b = sofar >> 16;
  size_t i = 0;
  if (nbytes < 16) goto finish;

  // find next alignment boundary
  for (; ((uintptr_t)&data[i]) & 0xf; i++) adler32_single(a, b, data + i, op);

  /* Process aligned 16-byte chunks using SSE

     We can afford a "size" of up to 2**31/256/4 = 2097152 before we
     risk signed overflow. A small calculation [1] suggests that our
     limit is 361 16-byte chunks. We can amortize plenty well by
     handling overflow every 256 chunks instead, which is 4kB, or
     every 64 iterations of the 4-chunk loop. As a bonus, this means
     we never risk hitting our limit in the single-chunk loop.

     [1] Try this in python: sum((i+1)*32-1 for i in range(361))
   */
  {
    int nstep = 16;  // 64 is max safe value; 16 is fastest
    int blocksize = 64 * nstep;
    ASSERT(a <= 0xffffffff and b <= 0xffffffff);
    v4si va = DZERO, vb = DZERO;

    // 4-block chunks
    for (; i + blocksize <= nbytes; i += blocksize) {
      for (int j = 0; j < blocksize; j += 64)
        adler32_sse_chunk4_fast(va, vb, (v16qi const *)(data + i + j), op);

      // reduce after each block to avoid overflow...
      adler32_sse_reduce(a, b, va, vb, blocksize);
      va = vb = DZERO;
    }

    // single chunks
    nstep = (nbytes - i) / 16;
    for (; i + 16 <= nbytes; i += 16)
      adler32_sse_chunk1_fast(va, vb, (v16qi const *)(data + i), op);

    // reduce it
    adler32_sse_reduce(a, b, va, vb, 16 * nstep);
  }

// finish it off
finish:
  return adler32_finish_sse(a, b, data, i, nbytes, op);
}

uint32_t __attribute__((flatten))
adler32_sse(char const *data, size_t nbytes, uint32_t sofar) {
  adler32_nop_op op;
  return adler32_sse(data, nbytes, sofar, op);
}
uint32_t __attribute__((flatten))
adler32_memcpy_sse(char *dest, char const *src, size_t nbytes, uint32_t sofar) {
  adler32_memcpy_op op(dest, src);
  return adler32_sse(src, nbytes, sofar, op);
}

// the globally visible symbols
uint32_t adler32(char const *data, size_t nbytes, uint32_t sofar) {
  return adler32_sse(data, nbytes, sofar);
}
uint32_t adler32_memcpy(char *dest, char const *src, size_t nbytes,
                        uint32_t sofar) {
  return adler32_memcpy_sse(dest, src, nbytes, sofar);
}
#else
#warning SSSE3 not available, falling back to vanilla implementation
uint32_t adler32(char const *data, size_t nbytes, uint32_t sofar) {
  return adler32_vanilla(data, nbytes, sofar);
}
uint32_t adler32_memcpy(char *dest, char const *src, size_t nbytes,
                        uint32_t sofar) {
  return adler32_memcpy_vanilla(dest, src, nbytes, sofar);
}
#endif
