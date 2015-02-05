#pragma once

#include <memory>
#include "small_vector.h"
#include "varstr.h"
#include "dbcore/sm-common.h"
#include "dbcore/dynarray.h"

struct dynarray;
// XXX: str arena hardcoded now to handle at most 1024 strings
class str_arena {
public:

  uint64_t ReserveBytes;
  static const size_t MinStrReserveLength = 2 * CACHELINE_SIZE;

  str_arena() : ReserveBytes(128 * 1024 * 1024)
  {
      str = dynarray(std::numeric_limits<unsigned int>::max(), ReserveBytes);
      memset(str, '\0', ReserveBytes);  // prefault
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
    if (n >= ReserveBytes) {
        ReserveBytes *= 2;
        str.ensure_size(ReserveBytes);
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
//  char str[ReserveBytes];
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
