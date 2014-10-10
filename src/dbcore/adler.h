#ifndef __ADLER_H
#define __ADLER_H

#include <stdint.h>
#include <cstddef>


/* An efficient implementation of the adler32 checksum algorithm. 

   One really nice feature of adler32 (in addition to its speed) is
   that checksums compose: given N bytes to process, we can compute
   the checksum for the first M bytes separately from the remaining
   N-M bytes. We can then combine two adjacent checksums as long as we
   know the length of the second (right hand) data block.

   Vanilla (scalar) implementation courtesy of
   http://stackoverflow.com/questions/421419/good-choice-for-a-lightweight-checksum-algorithm

   If SSSE3 or better is available, a vectorized version of adler32
   beats the optimized scalar version by anywhere from ~2x (for a few
   dozen bytes) to nearly 9x (for tens of kB or more). Achieving
   speedup better than 8x is especially impressive, given that the SSE
   implementation works with 8x16-bit values internally. For a workload that fits
   in cache, the SSE variant can surpass 12 GB/s. Obviously memory
   bandwidth limitations, or short runs, might eat into that
   throughtput.

   WARNING: adler32 is wickedly fast, but has several significant
   weaknesses [1]. Perhaps the most troubling one for our purposes is
   the fact that it performs poorly for small inputs (where "small" is
   sub-kB range).

   [1] http://guru.multimedia.cx/crc32-vs-adler32/
 */

static uint32_t const ADLER32_CSUM_INIT = 1;

/* Compute an adler32 checksum over the data given. If [sofar] is
   provided, it is a previously-computed checksum of data that
   logically precedes the data to be checked with this call. This
   allows building up a checksum incrementally, or over physically
   disjoint data sets.
 */
uint32_t adler32(char const *data, size_t nbytes, uint32_t sofar=ADLER32_CSUM_INIT);
uint32_t adler32_vanilla(char const *data, size_t nbytes, uint32_t sofar=ADLER32_CSUM_INIT);
#ifdef __SSSE3__
uint32_t adler32_sse(char const *data, size_t nbytes, uint32_t sofar=ADLER32_CSUM_INIT);
#endif

/* Combine two adjacent checksums into a single one and return the
   result. Useful for creating the aggregate checksum that would have
   resulted had the pieces been processed in logical order.
 */
uint32_t adler32_merge(uint32_t left, uint32_t right, size_t right_size);

/* Compute a checksum and perform a memcpy at the same time. Like
   adler32, this accepts a partially computed checksum that
   logically precedes the data to be copied/checked with this call.

   WARNING: The src and dst buffers *must* have the same relative
   alignment to a 16-byte boundary. In other words, the both buffers
   must start at the same offset from the preceding 16-byte boundary.
 */
uint32_t adler32_memcpy(char *dest, char const *src, size_t nbytes, uint32_t sofar=ADLER32_CSUM_INIT);
uint32_t adler32_memcpy_vanilla(char *dest, char const *src, size_t nbytes, uint32_t sofar=ADLER32_CSUM_INIT);
#ifdef __SSSE3__
uint32_t adler32_memcpy_sse(char *dest, char const *src, size_t nbytes, uint32_t sofar=ADLER32_CSUM_INIT);
#endif

#endif
