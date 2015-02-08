#pragma once

#include <memory>
#include "small_vector.h"
#include "varstr.h"
#include "dbcore/dynarray.h"

struct dynarray;
class str_arena {
public:

  static const uint64_t ReserveBytes = ((uint64_t)1<<32);			// 4GB.
  static const uint64_t InitialSize= ((uint64_t)1<<27);				// 128MB. Initial allocate size
  static const size_t MinStrReserveLength = 2 * CACHELINE_SIZE;

  str_arena()
  {
      str = dynarray(ReserveBytes, InitialSize );
      memset(str, '\0', InitialSize);  							// run-time prefault?
	  reset();
  }

  // non-copyable/non-movable for the time being
  str_arena(str_arena &&) = delete;
  str_arena(const str_arena &) = delete;
  str_arena &operator=(const str_arena &) = delete;

  inline void
  reset()
  {
    n = 0;
  }

  // next() is guaranteed to return an empty string
  varstr *
  next(uint64_t size)
  {
    uint64_t off = n;
    n += size + sizeof(varstr);
	ALWAYS_ASSERT( n <= ReserveBytes );			// can't exceed reserved size
	if( n > str.size() )
	{
		str.ensure_size( str.size() * 2 );		// get real memory allocation from reserved mapping. rapid grow
	}
    varstr *ret = new (str + off) varstr(str + off + sizeof(varstr), size);
    return ret;
  }

  inline varstr *
  operator()(uint64_t size)
  {
    return next(size);
  }

  bool
  manages(const varstr *px) const
  {
    return (char *)px >= &str[0] and
           (uint64_t)px->data() + px->size() <= (uint64_t)(&str[n]);
  }

private:
  dynarray 		str;
  size_t n;
};

class scoped_str_arena {
public:
  scoped_str_arena(str_arena *arena)
    : arena(arena)
  {
  }

  scoped_str_arena(str_arena &arena)
    : arena(&arena)
  {
  }

  scoped_str_arena(scoped_str_arena &&) = default;

  // non-copyable
  scoped_str_arena(const scoped_str_arena &) = delete;
  scoped_str_arena &operator=(const scoped_str_arena &) = delete;


  ~scoped_str_arena()
  {
    if (arena)
      arena->reset();
  }

  inline ALWAYS_INLINE str_arena *
  get()
  {
    return arena;
  }

private:
  str_arena *arena;
};
