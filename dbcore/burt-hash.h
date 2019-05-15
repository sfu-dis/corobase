// -*- mode:c++ -*-
#pragma once

#include <cstdint>
#include <x86intrin.h>

/* A family of fast, high-quality integer hashes due to Bob Jenkins
   [1]. He identifies 35 compact functions that achieve "full
   avalanche" status where toggling any bit of the input toggles
   25-75% of output bits. These hashes are thus especially useful with
   power of two-sized hash tables. The integer versions require
   roughly 20 machine instructions and employ only shift, add/sub, and
   xor operations. The SIMD version is just as fast, but applies the
   hash function to four values simultaneously.

   [1] http://burtleburtle.net/bob/hash/integer.html
*/
struct burt_hash {
  typedef uint32_t(function)(uint32_t);

  static function *select_hash(uint32_t selector);

  burt_hash(uint32_t selector = 0) : fn(select_hash(selector)) {}

  uint32_t operator()(uint32_t x) const { return fn(x); }

  function *fn;
};

struct burt_hash4 {
  typedef __v4si(function)(__v4si);

  static function *select_hash(uint32_t selector);

  burt_hash4(uint32_t selector = 0) : fn(select_hash(selector)) {}

  /* Emulate hashing one value with four different functions by
     hashing four different byte-permutations of the input. To avoid
     interacting badly with hash functions' habit of using
     bidirectional shifts (= rotations, if you squint), no two
     permutations are rotations of each other and no byte is placed
     in the same position twice.

     The first computed value is equivalent to the one computed by
     the integer function.

     The hope is that this will work well, given high-quality hash
     functions, but that has not been verified empirically.
   */
  __v4si operator()(int32_t xi) const {
    __v4si x = {xi};
#ifdef __clang__
    x = (__v4si)__builtin_shufflevector((__v16qi)x, (__v16qi)x, 0, 1, 2, 3, 3, 2, 0, 1, 2, 3, 1, 0, 1, 0, 3, 2);
#else
    __v16qi k = {0, 1, 2, 3, 3, 2, 0, 1, 2, 3, 1, 0, 1, 0, 3, 2};
    x = (__v4si)__builtin_shuffle((__v16qi)x, k);
#endif
    return operator()(x);
  }

  __v4si operator()(__v4si x) const { return fn(x); }

  function *fn;
};
