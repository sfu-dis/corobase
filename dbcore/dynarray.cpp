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

#include "dynarray.h"

#include "sm-common.h"
#include "sm-config.h"

#include <cerrno>

#include <sys/mman.h>

namespace ermia {

/* Handy to know this is true, but I'm not aware of any code that
   depends on it yet.
 */
static_assert(sizeof(dynarray) == DEFAULT_ALIGNMENT,
              "Dynarray is not the expected size");

/* Our implementation is limited to 2**48 bits due to our choice of
   "page" size and 32-bit size/capacity variables.

   We actually chose the page size to avoid running afoul of Windows'
   large mapping granularity; it's a nice side benefit that most
   systems have a 48-bit address space (so no point going larger).
 */
size_t dynarray::max_size() { return size_t(UINT32_MAX) << page_bits(); }

dynarray::dynarray() : _capacity(0), _size(0), _data(0) {}

dynarray::dynarray(size_t capacity, size_t size) : _size(0) {
  // round up to the nearest page boundary
  capacity = align_up(capacity, page_size());
  size = align_up(size, page_size());

  _capacity = capacity >> page_bits();

  THROW_IF(max_size() < capacity, illegal_argument,
           "Requested dynarray capacity is too large (%zd bytes)", capacity);
  THROW_IF(not capacity, illegal_argument, "Dynarray capacity cannot be zero");
  THROW_IF(
      capacity < size, illegal_argument,
      "Dynarray size cannot be larger than its capacity (%zd vs %zd bytes)",
      size, capacity);
  /*
    The magical incantation below tells mmap to reserve address
    space within the process without actually allocating any
    memory. We then call mprotect() to make bits of that address
    space usable.

    Note that mprotect() is smart enough to mix and match different
    sets of permissions, so we can extend the array simply by
    updating 0..new_size to have R/W permissions. Similarly, we can
    blow everything away by unmapping 0..reserved_size.

    Tested on Cygwin/x86_64, Linux-2.6.18/x86 and Solaris-10/Sparc.
  */

  int flags = MAP_NORESERVE | MAP_ANON | MAP_PRIVATE;
  _data = (char *)mmap(0, capacity, PROT_NONE, flags, -1, 0);
  THROW_IF(_data == MAP_FAILED, os_error, errno,
           "Unable to create dynarray with capacity %zd bytes", capacity);
  DEFER_UNLESS(success, munmap(_data, capacity));

  if (size) _adjust_mapping(_size, size, size, true);

  success = true;
}

dynarray::~dynarray() {
  // ignore dummy dynarrays
  if (not _capacity) return;

  int err = munmap(_data, capacity());
  LOG_IF(FATAL, err) << "Unable to unmap " << capacity() << " bytes starting at " << _data;
}

dynarray::dynarray(dynarray &&victim) noexcept : dynarray() {
  *this = std::move(victim);
}

dynarray &dynarray::operator=(dynarray &&victim) noexcept {
  using std::swap;
  swap(*this, victim);
  return *this;
}

void swap(dynarray &a, dynarray &b) noexcept {
  using std::swap;
  swap(a._capacity, b._capacity);
  swap(a._size, b._size);
  swap(a._data, b._data);
}

size_t dynarray::size() const { return size_t(_size) << page_bits(); }

size_t dynarray::capacity() const { return size_t(_capacity) << page_bits(); }

void dynarray::truncate(size_t new_size) {
  new_size = align_up(new_size, page_size());
  THROW_IF(size() < new_size, illegal_argument,
           "Attempt to truncate to larger size");

  _adjust_mapping(new_size, size(), new_size, false);
}

void dynarray::resize(size_t new_size) {
  // round up to the nearest page boundary
  new_size = align_up(new_size, page_size());

  THROW_IF(new_size < size(), illegal_argument,
           "Attempt to resize to a smaller size");

  // mark the new range as RW. Don't mess w/ the existing region!!
  _adjust_mapping(size(), new_size, new_size, true);
}

void dynarray::ensure_size(size_t min_size) {
  min_size = align_up(min_size, page_size());
  if (size() < min_size) {
    resize(min_size + 128 * config::MB);
  }
}

void dynarray::_adjust_mapping(size_t begin, size_t end, size_t new_size,
                               bool make_readable) {
  THROW_IF(new_size > capacity(), illegal_argument,
           "Cannot resize beyond capacity (%zd bytes requested, %zd possible)",
           new_size, capacity());

  ASSERT(is_aligned(begin, page_size()));
  ASSERT(is_aligned(end, page_size()));
  int prot = make_readable ? (PROT_READ | PROT_WRITE) : PROT_NONE;
  if (begin != end) {
    int err = mprotect(_data + begin, end - begin, prot);
    mlock(_data + begin, end - begin);  // prefault the space
    THROW_IF(err, os_error, errno, "Unable to resize dynarray");
    _size = new_size >> page_bits();
  }
}
}  // namespace ermia
