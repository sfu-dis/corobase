/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_ASSORTED_UNIFORM_RANDOM_HPP_
#define FOEDUS_ASSORTED_UNIFORM_RANDOM_HPP_

#include <stdint.h>
#include "../../macros.h"

namespace foedus {
namespace assorted {
/**
 * @brief A very simple and deterministic random generator that is more aligned with standard
 * benchmark such as TPC-C.
 * @ingroup ASSORTED
 * @details
 * Actually this is exactly from TPC-C spec.
 */
class UniformRandom {
 public:
  UniformRandom() : seed_(0) {}
  explicit UniformRandom(uint64_t seed) : seed_(seed) {}

  /**
   * In TPCC terminology, from=x, to=y.
   * NOTE both from and to are _inclusive_.
   */
  uint32_t uniform_within(uint32_t from, uint32_t to) {
    ALWAYS_ASSERT(from <= to);
    if (from == to) {
      return from;
    }
    return from + (next_uint32() % (to - from + 1));
  }
  /**
   * Same as uniform_within() except it avoids the "except" value.
   * Make sure from!=to.
   */
  uint32_t uniform_within_except(uint32_t from, uint32_t to, uint32_t except) {
    while (true) {
      uint32_t val = uniform_within(from, to);
      if (val != except) {
        return val;
      }
    }
  }

  /**
   * @brief Non-Uniform random (NURand) in TPCC spec (see Sec 2.1.6).
   * @details
   * In TPCC terminology, from=x, to=y.
   *  NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
   */
  uint32_t non_uniform_within(uint32_t A, uint32_t from, uint32_t to) {
    uint32_t C = get_c(A);
    return  (((uniform_within(0, A) | uniform_within(from, to)) + C) % (to - from + 1)) + from;
  }

  uint64_t get_current_seed() const {
    return seed_;
  }
  void set_current_seed(uint64_t seed) {
    seed_ = seed;
  }
  uint64_t next_uint64() {
    return (static_cast<uint64_t>(next_uint32()) << 32) | next_uint32();
  }
  uint32_t next_uint32() {
    seed_ = seed_ * 0xD04C3175 + 0x53DA9022;
    return (seed_ >> 32) ^ (seed_ & 0xFFFFFFFF);
  }

 private:
  uint64_t seed_;

  /**
   * C is a run-time constant randomly chosen within [0 .. A] that can be
   * varied without altering performance. The same C value, per field
   * (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals.
   * constexpr, but let's not bother C++11.
   */
  uint32_t get_c(uint32_t A) const {
    // yes, I'm lazy. but this satisfies the spec.
    const uint64_t kCSeed = 0x734b00c6d7d3bbdaULL;
    return kCSeed % (A + 1);
  }
};

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_UNIFORM_RANDOM_HPP_

