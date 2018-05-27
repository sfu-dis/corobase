#pragma once

#include "sm-defs.h"

#include <cstddef>
#include <cstdint>

namespace ermia {

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
uint8_t encode_size(size_t sz);

/* Encode a size, aligning it to the power of two given (in bits) and
   rounding up to the nearest value that can be encoded exactly.

   Also, update [sz] to the aligned/rounded size.
 */
uint8_t encode_size_aligned(size_t &sz,
                            size_t align_bits = DEFAULT_ALIGNMENT_BITS);

/* Decode a size code returned by encode_size.

   Size codes are used to size allocations, and because they are
   approximate the allocation may be slightly larger than the object
   that occupies it. If this matters, the user (or the object) is
   responsible to track the object's true size in some other way.

   A value of zero decodes to zero, and INVALID_SIZE_CODE decodes to
   INVALID_SIZE; all other values decode to something between 1 and
   950272.
 */
size_t decode_size(uint8_t code);

static inline size_t decode_size_aligned(
    uint8_t code, size_t align_bits = DEFAULT_ALIGNMENT_BITS) {
  return decode_size(code) << align_bits;
}

static uint8_t const INVALID_SIZE_CODE = 0xff;
static size_t const MAX_ENCODABLE_SIZE = 950272;
static size_t const INVALID_SIZE = -1;

}  // namespace ermia
