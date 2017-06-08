/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                          Copyright (c) 2013

                    Department of Computer Science
                        University of Toronto

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/
#include "w_rand.h"
#include "stopwatch.h"

#include <utility>
#include <unistd.h>
#include <pthread.h>

w_rand::w_rand() : _state(0) {
  uint64_t now = stopwatch_t::now();
  uint32_t s[] = {
      (uint32_t)(uintptr_t)&_state, (uint32_t)getpid(),
      (uint32_t)(uintptr_t)pthread_self(), (uint32_t)now, (uint32_t)(now >> 32),
  };
  seed(s);
}

void w_rand::seed(uint32_t const *w, size_t n) {
  for (size_t j = 0; n and j < R; j++) _arr[(R - j) & 0xf] = w[(j % n)];

  // warm up the internal state to ensure high quality output
  for (int i = 0; i < 100; i++) rand();
}

/* Select an unsigned 32-bit integer uniformly and at random.
 */
uint32_t w_rand::rand() {
  uint32_t si = _state;
  uint32_t si7 = (si + 7) & 0xf;
  uint32_t si3 = (si + 3) & 0xf;
  uint32_t si1 = (si + 1) & 0xf;

  uint32_t v0 = _arr[si];
  uint32_t v7 = _arr[si7];
  uint32_t v3 = _arr[si3];
  uint32_t z0 = _arr[si1];

  uint32_t z1 = v0 ^ (v0 << 16) ^ v3 ^ (v3 << 15);
  uint32_t z2 = v7 ^ (v7 >> 11);
  uint32_t z3 = z1 ^ z2;
  uint32_t z4 =
      z0 ^ (z0 << 2) ^ (z1 << 18) ^ z2 ^ (z2 << 28) ^ ((z3 & 0x6d22169) << 5);

  _arr[si] = z3;
  _arr[si1] = z4;
  _state = si1;
  return z4;
}

/* Select an integer uniformly and at random from the range 0 <= x
   < @end, where @end is a 32-bit unsigned integer.
*/
uint32_t w_rand::randn(uint32_t end) {
  if (end < 2) return 0;

  uint32_t rval;
  do {
    rval = uint64_t(end) * rand() / ~uint32_t(0);
  } while (rval >= end);
  return rval;
}

uint32_t w_rand::randn(uint32_t a, uint32_t b) {
  if (b < a) {
    using std::swap;
    swap(a, b);
  }

  return a + randn(b - a + 1);
}

/* Generate a floating point value in the range 0 <= x < 1, with
   32 bits of precision (this is a 32-bit RNG, after all).

   To minimize the cost of converting a 32-bit int to double, we
   assemble a double directly, using the IEEE 754 specification:

   |S|EEEEEEEEEEE|FFFFFF....F|
       (11 bits)   (52 bits)

   The value is then (-1)**S * 2**(E-1023) * 1.F; in other words,
   F is the fractional part of an implied 53-bit fixed-point
   number that ranges from 1.000... to 1.111...).

   In our case, we let F = 1.00000xxxxxxxx (in hex notation), with
   S=0 and an exponent of 20 (E=1043), which yields the normalized
   equivalent of 100000.xxxxxxxx. From there, we simply subtract
   0x100000 to recover the number we actually want (0.xxxxxxxx).

   The compiler optimizes the below into a logical OR, two
   constant loads, and one floating point subtract. The latter
   lets us avoid a 20-bit shift and also takes care of
   normalization.
*/
double w_rand::drand() {
  uint64_t bits = 20;
  union {
    uint64_t n;
    double d;
  } u = {((1023 + bits) << 52) | rand()};
  return u.d - double(1 << bits);
}

#ifdef TEST_W_RAND
#include <cstdio>

void __attribute__((noinline)) once(w_rand &rng) { rng.rand(); }

int main() {
  w_rand rng;
  for (int i = 0; i < 16; i++) {
    for (int j = 0; j < 8; j++) printf("%08x ", rng.rand());
    printf("\n");
  }

  printf("\n");
  for (int j = 0; j < 20; j++) {
    for (int i = 0; i < 40; i++) printf("%d ", rng.randn(10));
    printf("\n");
  }

  printf("\n");
  for (int j = 0; j < 20; j++) {
    for (int i = 0; i < 10; i++) printf("%.5f ", rng.drand());
    printf("\n");
  }

  for (int i = 0; i < 1e9; i++) once(rng);
}
#endif
