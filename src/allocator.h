#ifndef _NDB_ALLOCATOR_H_
#define _NDB_ALLOCATOR_H_

#include <cstdint>
#include <iterator>
#include <mutex>

#include "util.h"
#include "core.h"
#include "macros.h"
#include "spinlock.h"

class allocator {
public:

  static const size_t LgAllocAlignment = 4; // all allocations aligned to 2^4 = 16
  static const size_t AllocAlignment = 1 << LgAllocAlignment;

  static size_t
  GetHugepageSize()
  {
    static const size_t sz = GetHugepageSizeImpl();
    return sz;
  }

private:
  static size_t GetHugepageSizeImpl();
};

#endif
