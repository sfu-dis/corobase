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

#ifndef __DYNARRAY_H
#define __DYNARRAY_H

#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <sys/mman.h>
#include "sm-common.h"



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
template <typename T>
struct dynarray {

    /* no system I know of *requires* larger pages than this
     */
	inline static constexpr size_t page_bits() { return 16; }
	inline static constexpr size_t page_size() { return size_t(1) << page_bits(); }
    
    /* The size of the largest possible dynarray.
     */
	/* Our implementation is limited to 2**48 bits due to our choice of
	   "page" size and 32-bit size/capacity variables.

	   We actually chose the page size to avoid running afoul of Windows'
	   large mapping granularity; it's a nice side benefit that most
	   systems have a 48-bit address space (so no point going larger).
	 */
	inline size_t max_size() { return size_t(UINT32_MAX) << page_bits(); }


    /* Create a useless array with maximum capacity of zero bytes. It
       can only become useful by plundering an rvalue reference of a
       useful array.

       This method exists only so we can satisfy
       std::is_move_constructible and std::is_move_assignable.
     */
	dynarray() : _capacity(0), _size(0), _data(0){}
    
    /* Create a new array of [size] bytes that can grow to a maximum
       of [capacity] bytes.
    */
	dynarray(size_t capacity, size_t size)
		: _size(0)
	{
		// round up to the nearest page boundary
		capacity = align_up(capacity, page_size());
		size = align_up(size, page_size());

		_capacity = capacity >> page_bits();

		THROW_IF(max_size() < capacity, illegal_argument,
				"Requested dynarray capacity is too large (%zd bytes)", capacity);
		THROW_IF(not capacity, illegal_argument,
				"Dynarray capacity cannot be zero");
		THROW_IF(capacity < size, illegal_argument,
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
		_data = (T*) mmap(0, capacity, PROT_NONE, flags, -1, 0);
		THROW_IF(_data == MAP_FAILED, os_error, errno,
				"Unable to create dynarray with capacity %zd bytes", capacity);
		DEFER_UNLESS(success, munmap(_data, capacity));

		if (size)
			_adjust_mapping(_size, size, size, true);

		success = true;
	}
    
	~dynarray()
	{
		// ignore dummy dynarrays
		if (not _capacity)
			return;

		int err = munmap(_data, capacity());
		THROW_IF(err, os_error, errno, \
				"Unable to unmap %zd bytes starting at %p",
				capacity(), _data);
	}

    /* The current size of this dynarray.

       Assuming sufficient memory is available the array can grow to
       /capacity()/ bytes by using calls to /resize()/.
     */
    inline size_t size() const { return size_t(_size) << page_bits(); }
    
    /* The reserved size this array can grow to fill. The limit is set
       at initialization and cannot change later.
     */
    inline size_t capacity() const { return size_t(_capacity) << page_bits(); }

    /* Maps in memory to bring the total to /new_size/ bytes. 
     */
    void resize(size_t new_size)
	{
		// round up to the nearest page boundary
		new_size = align_up(new_size, page_size());

		THROW_IF (new_size < size(), illegal_argument,
				"Attempt to resize to a smaller size");

		// mark the new range as RW. Don't mess w/ the existing region!!
		_adjust_mapping(size(), new_size, new_size, true);
	}

    /* Ensures that at least [new_size] bytes are ready to use.

       Unlike resize(), this function accepts any value of [new_size]
       (doing nothing if the array is already big enough).
     */
    void ensure_size(size_t min_size)
	{
		min_size = align_up(min_size, page_size());
		if(size() < min_size) 
			resize(min_size);
	}

    /* UNSAFE, but useful for debugging */
    void truncate(size_t new_size=0)
	{
		new_size = align_up(new_size, page_size());
		THROW_IF (size() < new_size, illegal_argument,
				"Attempt to truncate to larger size");

		_adjust_mapping(new_size, size(), new_size, false);
	}

    T &operator[](size_t i) { return _data[i]; }
    T const &operator[](size_t i) const { return _data[i]; }

    T *data() { return _data; }
    T const *data() const { return _data; }
    
    operator T*() { return _data; }
    operator T const*() const { return _data; }

    // not copyable, but is movable
    dynarray(dynarray const &)=delete;
    void operator=(dynarray const &)=delete;
    
	dynarray(dynarray &&victim) noexcept
		: dynarray()
		{
			*this = std::move(victim);
		}

    dynarray &operator=(dynarray &&victim) noexcept
	{
		using std::swap;
		swap(*this, victim);
		return *this;
	}

private:
    void _adjust_mapping(size_t begin, size_t end, size_t new_size, bool make_readable)
	{
		THROW_IF(new_size > capacity(), illegal_argument,
				"Cannot resize beyond capacity (%zd bytes requested, %zd possible)",
				new_size, capacity());

		ASSERT(is_aligned(begin, page_size()));
		ASSERT(is_aligned(end, page_size()));
		int prot = make_readable? (PROT_READ|PROT_WRITE) : PROT_NONE;
		if (begin != end) {
			int err = mprotect(_data+begin, end-begin, prot);
			THROW_IF(err, os_error, errno, "Unable to resize dynarray");
			_size = new_size >> page_bits();
		}

	}
    
    uint32_t _capacity;
    uint32_t _size;
    T *_data;

    friend 
    void swap(dynarray &a, dynarray &b) noexcept
	{
		using std::swap;
		swap(a._capacity, b._capacity);
		swap(a._size, b._size);
		swap(a._data, b._data);
	}
};

/* Handy to know this is true, but I'm not aware of any code that
   depends on it yet.
 */
//static_assert(sizeof(dynarray) == DEFAULT_ALIGNMENT, "Dynarray is not the expected size");
/**\endcond skip */
#endif
