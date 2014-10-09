#include <sys/mman.h>
#include <unistd.h>
#include <map>
#include <iostream>
#include <cstring>
#include <numa.h>

#include "allocator.h"
#include "spinlock.h"
#include "lockguard.h"
#include "static_vector.h"
#include "counter.h"

using namespace util;

size_t
allocator::GetHugepageSizeImpl()
{
  FILE *f = fopen("/proc/meminfo", "r");
  assert(f);
  char *linep = NULL;
  size_t n = 0;
  static const char *key = "Hugepagesize:";
  static const int keylen = strlen(key);
  size_t size = 0;
  while (getline(&linep, &n, f) > 0) {
    if (strstr(linep, key) != linep)
      continue;
    size = atol(linep + keylen) * 1024;
    break;
  }
  fclose(f);
  assert(size);
  return size;
}

