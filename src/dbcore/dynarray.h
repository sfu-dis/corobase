/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

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

#pragma once

#include <cstddef>
#include <cstdint>

namespace ermia {

/**\cond skip */

/* A memory-mapped array which exploits the capabilities provided by
   mmap in order to grow dynamically without moving existing data or
   wasting memory.

   Ideal for situations where you don't know the final size of the
   array, the potential maximum is known but very large, and a
   threaded environment makes it unsafe to resize by reallocating.

   NOTE: the array only supports growing, under the assumption that
   any array which can shrink safely at all can shrink safely to size
   zero (with the data copied to a new, smaller dynarray)

   This approach makes the most sense in a 64-bit environment where
   address space is cheap.

   Note that most systems cannot reserve more than 2-8TB of
   contiguous address space (32-128TB total), most likely because most
   machines don't have that much swap space anyway.

   NOTE: the dynarray is not POD (because it has non-trivial
   constructor/destructor, etc.), but that is only to prevent
   accidental copying and moving (= world's biggest buffer
   overflow!). The class is designed so that its contents can be
   copied easily to buffers, written to disk, etc.
 */
struct dynarray {
  /* no system I know of *requires* larger pages than this
   */
  static constexpr size_t page_bits() { return 16; }

  static constexpr size_t page_size() { return size_t(1) << page_bits(); }

  /* The size of the largest possible dynarray.
   */
  static size_t max_size();

  /* Create a useless array with maximum capacity of zero bytes. It
     can only become useful by plundering an rvalue reference of a
     useful array.

     This method exists only so we can satisfy
     std::is_move_constructible and std::is_move_assignable.
   */
  dynarray();

  /* Create a new array of [size] bytes that can grow to a maximum
     of [capacity] bytes.
  */
  dynarray(size_t capacity, size_t size = 0);

  ~dynarray();

  /* The current size of this dynarray.

     Assuming sufficient memory is available the array can grow to
     /capacity()/ bytes by using calls to /resize()/.
   */
  size_t size() const;

  /* The reserved size this array can grow to fill. The limit is set
     at initialization and cannot change later.
   */
  size_t capacity() const;

  /* Maps in memory to bring the total to /new_size/ bytes.
   */
  void resize(size_t new_size);

  /* Ensures that at least [new_size] bytes are ready to use.

     Unlike resize(), this function accepts any value of [new_size]
     (doing nothing if the array is already big enough).
   */
  void ensure_size(size_t min_size);

  /* UNSAFE, but useful for debugging */
  void truncate(size_t new_size = 0);

  char &operator[](size_t i) { return _data[i]; }
  char const &operator[](size_t i) const { return _data[i]; }

  char *data() { return _data; }
  char const *data() const { return _data; }

  operator char *() { return _data; }
  operator char const *() const { return _data; }

  // not copyable, but is movable
  dynarray(dynarray const &) = delete;
  void operator=(dynarray const &) = delete;

  dynarray(dynarray &&victim) noexcept;
  dynarray &operator=(dynarray &&victim) noexcept;

 private:
  void _adjust_mapping(size_t begin, size_t end, size_t new_size,
                       bool make_readable);

  uint32_t _capacity;
  uint32_t _size;
  char *_data;

  friend void swap(dynarray &a, dynarray &b) noexcept;
};

}  // namespace ermia
