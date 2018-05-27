#include "size-encode.h"

namespace ermia {

#define B(e) (size_t(16) - (size_t(15) >> (e)))
static uint8_t constexpr decode_table[] = {
    B(0x0), B(0x1), B(0x2), B(0x3), B(0x4), B(0x5), B(0x6), B(0x7),
    B(0x8), B(0x9), B(0xa), B(0xb), B(0xc), B(0xd), B(0xe), B(0xf),
};

#if 1
// use a lookup table for B rather than computing it every time
#undef B
#define B(E) decode_table[E]
#endif

size_t constexpr decode_size_helper(uint8_t code) {
#define E (size_t(code) >> 4)
#define M (size_t(code) & size_t(0x0f))
  return (M + B(E)) << E;
#undef E
#undef M
#undef B
}

static size_t constexpr _decode_size(uint8_t code) {
  /* If no M bits are set, just return the code */
  return ((code + 1) & 0xf0) ? decode_size_helper(code + 0xff) : int8_t(code);
}

size_t decode_size(uint8_t code) { return _decode_size(code); }

/*
  Encode a non-negative size in 8 bits, using a format similar to
  floating point. As with a floating point representation, precision
  decreases as the magnitude increases. Also as with floating point,
  the encoding is order-preserving and codes can be compared directly.

  The encoding can represent values ranging from 0 (0x00) to 950272
  (0xfe); 0xff is reserved as NaN/invalid. To avoid clipping objects,
  the input is always rounded up to the next representable value
  before encoding it, with a maximum rounding error of 6.25%.

  Notably, the encoding represents nearly all k*2**i <= 928k exactly,
  for integer 0 < k < 32 and integer 0 <= i < 20. The only exceptions
  are: 25*2 (52), 27*2 (56), 29*2 (60), 31*2 (64), 29*4 (120), 31*4
  (128), 31*8 (256). It also represents several other "interesting" k
  exactly because those k often contain powers of two. For example
  100*2**i = 25*2**(i+2) is always exact.

  Encoding is somewhat expensive (in no small part due to the
  requirement that we round up), but decoding is quite fast, requiring
  only 13 machine instructions (one branch) on x86_64
*/
uint8_t encode_size(size_t sz) {
#define EFIRST(x) decode_size_helper(((x) << 4))
#define ELAST(x) \
  (((x) == 15) ? decode_size(0xfe) : decode_size_helper(((x) << 4) + 0xf))
  uint8_t e, m;
  auto doit = [](size_t sz, uint8_t e) {
    uint8_t m, b;
    sz += (1 << e) - 1;
    b = 16 - (15 >> e);
    m = sz >> e;
    return (e << 4) + m - b + 1;
  };

  // special case: 0..17 are their own code
  if (sz <= ELAST(2)) {
    if (sz <= ELAST(1)) {
      if (sz < EFIRST(1)) return sz;

      return doit(sz, 1);
    }

    if (sz <= EFIRST(2)) return (2 << 4) + 1;

    return doit(sz, 2);
  }

  if (sz <= ELAST(3)) {
    if (sz <= EFIRST(3)) return (3 << 4) + 1;

    return doit(sz, 3);
  }

  if (sz <= EFIRST(4)) return (4 << 4) + 1;

  // special case: a nice pattern emerges for large values
  if (sz <= ELAST(15)) {
    int lz = __builtin_clz(sz);

    // round it up
    e = 27 - lz;
    sz += (1 << e) - 1;
    lz = __builtin_clz(sz);

    // e={4..15}
    e = 27 - lz;
    m = (sz >> e) - 16;
    return (e << 4) + m + 1;
  }

  // out of bounds
  return -1;
}

#define D(n) _decode_size(0x##n)
static size_t constexpr _encode_size_tab[] = {
    D(00), D(01), D(02), D(03), D(04), D(05), D(06), D(07), D(08), D(09), D(0a),
    D(0b), D(0c), D(0d), D(0e), D(0f), D(10), D(11), D(12), D(13), D(14), D(15),
    D(16), D(17), D(18), D(19), D(1a), D(1b), D(1c), D(1d), D(1e), D(1f), D(20),
    D(21), D(22), D(23), D(24), D(25), D(26), D(27), D(28), D(29), D(2a), D(2b),
    D(2c), D(2d), D(2e), D(2f), D(30), D(31), D(32), D(33), D(34), D(35), D(36),
    D(37), D(38), D(39), D(3a), D(3b), D(3c), D(3d), D(3e), D(3f), D(40), D(41),
    D(42), D(43), D(44), D(45), D(46), D(47), D(48), D(49), D(4a), D(4b), D(4c),
    D(4d), D(4e), D(4f), D(50), D(51), D(52), D(53), D(54), D(55), D(56), D(57),
    D(58), D(59), D(5a), D(5b), D(5c), D(5d), D(5e), D(5f), D(60), D(61), D(62),
    D(63), D(64), D(65), D(66), D(67), D(68), D(69), D(6a), D(6b), D(6c), D(6d),
    D(6e), D(6f), D(70), D(71), D(72), D(73), D(74), D(75), D(76), D(77), D(78),
    D(79), D(7a), D(7b), D(7c), D(7d), D(7e), D(7f), D(80), D(81), D(82), D(83),
    D(84), D(85), D(86), D(87), D(88), D(89), D(8a), D(8b), D(8c), D(8d), D(8e),
    D(8f), D(90), D(91), D(92), D(93), D(94), D(95), D(96), D(97), D(98), D(99),
    D(9a), D(9b), D(9c), D(9d), D(9e), D(9f), D(a0), D(a1), D(a2), D(a3), D(a4),
    D(a5), D(a6), D(a7), D(a8), D(a9), D(aa), D(ab), D(ac), D(ad), D(ae), D(af),
    D(b0), D(b1), D(b2), D(b3), D(b4), D(b5), D(b6), D(b7), D(b8), D(b9), D(ba),
    D(bb), D(bc), D(bd), D(be), D(bf), D(c0), D(c1), D(c2), D(c3), D(c4), D(c5),
    D(c6), D(c7), D(c8), D(c9), D(ca), D(cb), D(cc), D(cd), D(ce), D(cf), D(d0),
    D(d1), D(d2), D(d3), D(d4), D(d5), D(d6), D(d7), D(d8), D(d9), D(da), D(db),
    D(dc), D(dd), D(de), D(df), D(e0), D(e1), D(e2), D(e3), D(e4), D(e5), D(e6),
    D(e7), D(e8), D(e9), D(ea), D(eb), D(ec), D(ed), D(ee), D(ef), D(f0), D(f1),
    D(f2), D(f3), D(f4), D(f5), D(f6), D(f7), D(f8), D(f9), D(fa), D(fb), D(fc),
    D(fd), D(fe), D(ff),
};

/* As an alternative to the complex routine above, we can also just
   create a 256-entry lookup table and select the element to use with
   a binary search. Has the advantage of giving us the decoded value
   of the size code for free.
 */
uint8_t encode_size_aligned(size_t &size, size_t align_bits) {
  size_t x = align_up(size, 1 << align_bits) >> align_bits;
  unsigned lo = 0;
  unsigned hi = 0xff;
  // printf("encode %d\n", (int) x);
  while (lo < hi) {
    unsigned mid = (lo + hi) / 2;
    if (x <= _encode_size_tab[mid]) {
      // printf("\t%d * %d %d\n", lo, mid, hi);
      hi = mid;
    } else {
      // printf("\t%d %d * %d\n", lo, mid, hi);
      lo = mid + 1;
    }
  }
  size = _encode_size_tab[lo] << align_bits;
  return lo;
}

// use the table-based approach?
//#define encode_size encode_size_tab
}  // namespace ermia
