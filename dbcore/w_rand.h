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
#ifndef __W_RAND_H
#define __W_RAND_H

#include <inttypes.h>
#include <cstddef>

/* A uniform pseudorandom RNG that implements the WELL512a algorithm
   [1]. WELL is a family of high-quality (but not crypto-quality!)
   PRNG that is somewhat superior to the Mersenne twister.

   This particular variant uses 512 bits internal state (one cache
   line on most machines) and is relatively inexpensive (~14ns/value
   produced), with drastically simpler code than that required for
   longer-period variants. The downside is a period length of "only"
   2**512. This is an intentional trade-off vs. longer-period WELL or
   Mersenne twister variants, whose period of 2**19337 is arguably
   overkill for most applications.

   The code is loosely based on the C code provided by the paper
   authors. The code has been tweaked significantly (array indexing
   moves forward rather than backward, redundant computations such as
   x ^ x have been eliminated, etc), but the algorithm is unchanged:
   for a given seed, it produces exactly the same output as the
   reference implementation.

   [1] http://www.iro.umontreal.ca/~panneton/WELLRNG.html

   [2] "Crush" is a battery of 96 statistical tests, of which only six
   fail for this RNG (see below). According to the paper authors, the
   failing tests "look for linear dependencies in a long sequence of
   bits," a weakness inherent to "all F2-linear generators, including
   the Mersenne twister." Testing below used TestU01-1.2.3, downloaded
   from http://www.iro.umontreal.ca/~simardr/testu01/tu01.html.

   ========= Summary results of Crush =========

    Version:          TestU01 1.2.3
    Generator:        well512a
    Number of statistics:  144
    Total CPU time:   01:13:52.22
    The following tests gave p-values outside [0.001, 0.9990]:
    (eps  means a value < 1.0e-300):
    (eps1 means a value < 1.0e-15):

          Test                          p-value
    ----------------------------------------------
    10  CollisionOver, t = 20           9.4e-5
    60  MatrixRank, 1200 x 1200          eps
    61  MatrixRank, 1200 x 1200          eps
    71  LinearComp, r = 0              1 - eps1
    72  LinearComp, r = 29             1 - eps1
    93  AutoCor, d = 1                  0.9997
    ----------------------------------------------
    All other tests were passed
*/
struct w_rand {
  static size_t const R = 16;

  uint32_t _state;
  uint32_t _arr[R];

  /* Create an RNG whose internal state is based on the object's
     location in memory. This will always produce a valid stream of
     random numbers, but the same stream might be chosen repeatedly,
     both within and between process invocations, depending on both
     the runtime system and how the object's memory was allocated.
   */
  w_rand();

  w_rand(uint32_t const *w, size_t n) : _state(0) { seed(w, n); }

  template <size_t N>
  w_rand(uint32_t const(&w)[N])
      : _state(0) {
    seed(w);
  }

  /* Seed the internal state of this RNG with 0 < @n <= R words of
     @w. If @n < R, entries from @w will be used multiple times, in
     round robin fashion. If @n=0, the internal state is left
     deliberately uninitialized, which is probably undesirable.

     WARNING: setting the internal state to all-zero words turns
     this RNG into a convoluted replacement for /dev/zero.
   */
  void seed(uint32_t const *w, size_t n);

  template <size_t N>
  void seed(uint32_t const(&w)[N]) {
    seed(w, N);
  }

  /* Select an unsigned 32-bit integer uniformly and at random.
   */
  uint32_t rand();

  /* Select an integer uniformly and at random from the range 0 <= x
     < @end, where @end is a 32-bit unsigned integer.
   */
  uint32_t randn(uint32_t end);

  // convenience method for std::random_shuffle
  uint32_t operator()(uint32_t end) { return randn(end); }

  /* Select an integer uniformly and at random from the integers
     between @a and @b, inclusive. It does not matter which order
     the two endpoints are specified in.
  */
  uint32_t randn(uint32_t a, uint32_t b);

  /* Generate a floating point value in the range 0 <= x < 1, with
     32 bits of entropy (this is a 32-bit RNG, after all).
  */
  double drand();
};

/* An adaptor that makes w_rand fit the C++11 random number generator
   interface.
 */
struct w_rand_urng {
  typedef uint32_t result_type;
  w_rand &rng;
  uint32_t operator()() { return rng.rand(); }
  uint32_t min() { return 0; }
  uint32_t max() { return ~0; }
};

#endif
