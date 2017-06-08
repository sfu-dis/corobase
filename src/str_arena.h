#pragma once

#include <memory>
#include "varstr.h"
#include "dbcore/sm-common.h"

class str_arena {
 public:
  static const uint64_t ReserveBytes = 128 * 1024 * 1024;
  static const size_t MinStrReserveLength = 2 * CACHELINE_SIZE;
  str_arena() : n(0) {
    // adler32 (log checksum) needs it aligned
    ALWAYS_ASSERT(
        not posix_memalign((void **)&str, DEFAULT_ALIGNMENT, ReserveBytes));
    memset(str, '\0', ReserveBytes);
    reset();
  }

  // non-copyable/non-movable for the time being
  str_arena(str_arena &&) = delete;
  str_arena(const str_arena &) = delete;
  str_arena &operator=(const str_arena &) = delete;

  inline void reset() {
    ASSERT(n < ReserveBytes);
    n = 0;
  }

  varstr *next(uint64_t size) {
    uint64_t off = n;
    n += align_up(size + sizeof(varstr));  // for adler32's 16-byte alignment
    ASSERT(n < ReserveBytes);
    varstr *ret = new (str + off) varstr(str + off + sizeof(varstr), size);
    return ret;
  }

  inline varstr *operator()(uint64_t size) { return next(size); }

  bool manages(const varstr *px) const {
    return (char *)px >= str and
           (uint64_t) px->data() + px->size() <= (uint64_t)str + n;
  }

 private:
  char *str;
  size_t n;
};

class scoped_str_arena {
 public:
  scoped_str_arena(str_arena *arena) : arena(arena) {}

  scoped_str_arena(str_arena &arena) : arena(&arena) {}

  scoped_str_arena(scoped_str_arena &&) = default;

  // non-copyable
  scoped_str_arena(const scoped_str_arena &) = delete;
  scoped_str_arena &operator=(const scoped_str_arena &) = delete;

  ~scoped_str_arena() {
    if (arena) arena->reset();
  }

  inline ALWAYS_INLINE str_arena *get() { return arena; }

 private:
  str_arena *arena;
};
