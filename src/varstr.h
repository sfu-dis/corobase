#ifndef _NDB_varstr_H_
#define _NDB_varstr_H_

#include <endian.h>
#include <stdint.h>
#include <string.h>

#include <iostream>
#include <type_traits>
#include <limits>

#include "macros.h"
#include "util.h"

#if NDB_MASSTREE
#include "prefetch.h"
#include "masstree/config.h"
#include "masstree/string_slice.hh"
#endif

// This is basically the same as varstr - avoid using inheritance here
// to avoid ambiguious overload in serach() calls.
class varstr {
  friend std::ostream &operator<<(std::ostream &o, const varstr &k);
public:
  inline varstr() : l(0), p(NULL) {}
  inline varstr(const varstr &that) = default;
  inline varstr(varstr &&that) = default;
  inline varstr &operator=(const varstr &that) = default;

  inline varstr(const uint8_t *p, size_t l)
    : l(l), p(p)
  {
  }

  inline varstr(const char *p, size_t l)
    : l(l), p((const uint8_t *)p)
  {
  }

  explicit inline varstr(const char *s)
    : l(strlen(s)), p((const uint8_t *) s)
  {
  }

  inline void init(uint8_t *v, uint32_t s)
  {
    l = s;
    p = v;
  }

  inline void copy_from(const uint8_t *s, size_t l)
  {
    copy_from((char *)s, l);
  }

  inline void copy_from(const char *s, size_t ll)
  {
    memcpy((void*)p, s, ll);
    l = ll;
  }

  inline bool
  operator==(const varstr &that) const
  {
    if (size() != that.size())
      return false;
    return memcmp(data(), that.data(), size()) == 0;
  }

  inline bool
  operator!=(const varstr &that) const
  {
    return !operator==(that);
  }

  inline bool
  operator<(const varstr &that) const
  {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() < that.size());
  }

  inline bool
  operator>=(const varstr &that) const
  {
    return !operator<(that);
  }

  inline bool
  operator<=(const varstr &that) const
  {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() <= that.size());
  }

  inline bool
  operator>(const varstr &that) const
  {
    return !operator<=(that);
  }

  inline uint64_t
  slice() const
  {
    uint64_t ret = 0;
    uint8_t *rp = (uint8_t *) &ret;
    for (size_t i = 0; i < std::min(l, size_t(8)); i++)
      rp[i] = p[i];
    return util::host_endian_trfm<uint64_t>()(ret);
  }

#if NDB_MASSTREE
  inline uint64_t slice_at(int pos) const {
    return string_slice<uint64_t>::make_comparable((const char*) p + pos, std::min(int(l - pos), 8));
  }
#endif

  inline varstr
  shift() const
  {
    INVARIANT(l >= 8);
    return varstr(p + 8, l - 8);
  }

  inline varstr
  shift_many(size_t n) const
  {
    INVARIANT(l >= 8 * n);
    return varstr(p + 8 * n, l - 8 * n);
  }

  inline size_t
  size() const
  {
    return l;
  }

  inline int length() const {
    return l;
  }

  inline const uint8_t *
  data() const
  {
    return p;
  }

  inline uint8_t *
  data()
  {
    return (uint8_t *)p;
  }

  inline bool empty() const
  {
      return not size();
  }

#if NDB_MASSTREE
  inline operator lcdf::Str() const {
    return lcdf::Str(p, l);
  }
#endif

private:
  size_t l;
  const uint8_t *p; // must be the last field
};

#endif /* _NDB_varstr_H_ */
