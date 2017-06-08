#include "adler.h"

#include "sm-defs.h"
#include "sm-exceptions.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <unistd.h>
#include <sys/stat.h>

uint32_t /*__attribute__((aligned(16)))*/ DATA[] = {
    0x45072b09, 0xa221a546, 0xaa688b72, 0xa8b75579, 0x638f3f8b, 0x738b4787,
    0x913a33bf, 0x12ae813b, 0xbd06ada0, 0x9fe771a0, 0xb0523af5, 0x087f7ef5,
    0xdcba1718, 0x5233b7d6, 0x9587aabc, 0x8201f25e, 0xb74dba7e, 0x08c8e4c9,
    0x351ade7f, 0xccfdcae3, 0xaf73a554, 0x2acbac1e, 0x539ad967, 0x8af43d49,
    0xd709a187, 0xb4c689b8, 0xae6c1b8e, 0x7f942c4d, 0x66c41437, 0xf8234029,
    0x0c8a05c9, 0x4a1fda62, 0x3841ebaf, 0x755be549, 0xa57488d4, 0x9ca8e518,
    0xb325204e, 0x61784ff1, 0x23cabbe9, 0x0813256d, 0x113fb0c4, 0x0fdb239a,
    0x76986aea, 0xe4f94ae9, 0xd2cb642a, 0x00629987, 0x8a4cdbcc, 0x463d8c74,
    0x14959bae, 0x4331375e, 0x1b7a5844, 0x24231993, 0xfbbc875e, 0x8d7fb631,
    0xc25abe6b, 0xb512f7de, 0xc0582381, 0x6d63dcb8, 0x781169a7, 0x193a9fe5,
    0x8393cbc4, 0x3590d330, 0x3b944faa, 0xf11d5555, 0x128d830d, 0x589e5804,
    0x88ca2224, 0xea604e04, 0x9be2002b, 0xe72b7a8a, 0x48c3bc63, 0xc7b922df,
    0x7f76839e, 0x25b12d3e, 0x4812d4a2, 0x57655f21, 0x93c5469a, 0x9da52651,
    0x9b4fd544, 0x264a003a, 0x707b53ed, 0x95a1b28b, 0x00bcdf05, 0x143ca38c,
    0x7b7be6f4, 0x9489630a, 0xcde29440, 0xfead2710, 0x1eb3d9cd, 0x0cdc7b50,
    0x4774108d, 0x7d5de585, 0x1230c9c4, 0x73ed9ed4, 0x93deb583, 0x42fb1070,
    0x32954863, 0x4a747267, 0xeaa7b9a2, 0x6a828322, 0x8031c036, 0x772e7b69,
    0xb6278456, 0x718ca706, 0x8e86a6a6, 0x4d4ac1db, 0x495435a9, 0x55ac74ea,
    0xac4f0fc0, 0xdbd88ac6, 0xf170cb1a, 0x5236e491, 0x38d42015, 0xf8748fd2,
    0x9e2a7d3a, 0x36079063, 0x92a698e7, 0xe555979b, 0xf80f7161, 0xa610e567,
    0x1e9f682b, 0x56502fac, 0x554c789d, 0x155199b3, 0x77c7128f, 0xfca60433,
    0x37258fa8, 0x56b54378, 0xfd12c7f1, 0xffb3fc4c, 0x9457aa80, 0xc01f2c8a,
    0x071c3aa0, 0xfd9b91fd, 0xd4331755, 0x71d89bc4, 0x7f46d88b, 0x4dfaf768,
    0xeda0e0da, 0x44711a1c, 0x5f920f3b, 0x0623a9e2, 0x772127b7, 0x90e5f4c2,
    0x8d36892e, 0x63b79e74, 0x5fe5acd7, 0x12c89eca, 0x864d1033, 0x07858d7e,
    0xb88ac820, 0x2629fa2b, 0x2216d818, 0xce998dbe, 0x6f0e145d, 0x34bbd657,
    0xbb0591f1, 0xcd68710d, 0x438d6498, 0x284e282f, 0x1ac990ae, 0xcc9d8c8e,
    0x23008b94, 0xfa8e9b19, 0x53909eb9, 0x58f6c1d5, 0xc5fc5e38, 0x6e65518f,
    0x5ebfe405, 0x6d6c4d3b, 0x5c8555ba, 0x5d73a2a2, 0x7198a6ae, 0xf7b24564,
    0xcacfe138, 0x284708d3, 0x24fc81a6, 0x9a5ae01b, 0xc37cad95, 0xd7265433,
    0x2ed99b2b, 0x1133b7d1, 0x90196b6b, 0xbf8d1974, 0xe689b353, 0xed0329a6,
    0x9ff6b8c8, 0x783f44a7, 0x4b9f7322, 0x34016bac, 0xa7e8eee9, 0x9d627e1c,
    0x4e187426, 0x358c8cca, 0xd4aa77c7, 0x93330d6c, 0xd800aee8, 0x928fa710,
    0x41f999d3, 0x8b66115e, 0xabf93710, 0xdb9305d6, 0x6098188d, 0x2e010f93,
    0xaaa608eb, 0xaffd885d, 0x9e6736db, 0x43770ab9, 0x7fb76047, 0xded6ef8e,
    0x49c229d1, 0xa37b8b72, 0xc25d18af, 0xacda81b8, 0x2708a5f3, 0x45043f2d,
    0x6b601618, 0x9334531a, 0xff347013, 0x4d31531b, 0xcef7d88a, 0x12342d0d,
    0xefa5d316, 0xbd156e71, 0xf34de15d, 0x2270fffa, 0xdfb623d5, 0x1be313b1,
    0x66a963b8, 0xa160112f, 0x052d9dc8, 0x8715591b, 0x7cbd5571, 0xe35bf59e,
    0x7baa5dbe, 0xb0d7ab94, 0x588b1b24, 0x6b9259a9, 0x4a425d3f, 0xc2da2ce0,
    0xd37ef2c7, 0xf2a75446, 0x096e62e2, 0xb42dab7a, 0xb6180985, 0x2be8dd83,
    0x7138e515, 0xca803bcd, 0xf06415cb, 0xd872385d, 0x30a3d4e6, 0x50a6279e,
    0x53784f85, 0xd5427217, 0xa834b4bc, 0xb3213e26, 0x45072b09, 0xa221a546,
    0xaa688b72, 0xa8b75579, 0x638f3f8b, 0x738b4787, 0x913a33bf, 0x12ae813b,
    0xbd06ada0, 0x9fe771a0, 0xb0523af5, 0x087f7ef5, 0xdcba1718, 0x5233b7d6,
    0x9587aabc, 0x8201f25e, 0xb74dba7e, 0x08c8e4c9, 0x351ade7f, 0xccfdcae3,
    0xaf73a554, 0x2acbac1e, 0x539ad967, 0x8af43d49, 0xd709a187, 0xb4c689b8,
    0xae6c1b8e, 0x7f942c4d, 0x66c41437, 0xf8234029, 0x0c8a05c9, 0x4a1fda62,
    0x3841ebaf, 0x755be549, 0xa57488d4, 0x9ca8e518, 0xb325204e, 0x61784ff1,
    0x23cabbe9, 0x0813256d, 0x113fb0c4, 0x0fdb239a, 0x76986aea, 0xe4f94ae9,
    0xd2cb642a, 0x00629987, 0x8a4cdbcc, 0x463d8c74, 0x14959bae, 0x4331375e,
    0x1b7a5844, 0x24231993, 0xfbbc875e, 0x8d7fb631, 0xc25abe6b, 0xb512f7de,
    0xc0582381, 0x6d63dcb8, 0x781169a7, 0x193a9fe5, 0x8393cbc4, 0x3590d330,
    0x3b944faa, 0xf11d5555, 0x128d830d, 0x589e5804, 0x88ca2224, 0xea604e04,
    0x9be2002b, 0xe72b7a8a, 0x48c3bc63, 0xc7b922df, 0x7f76839e, 0x25b12d3e,
    0x4812d4a2, 0x57655f21, 0x93c5469a, 0x9da52651, 0x9b4fd544, 0x264a003a,
    0x707b53ed, 0x95a1b28b, 0x00bcdf05, 0x143ca38c, 0x7b7be6f4, 0x9489630a,
    0xcde29440, 0xfead2710, 0x1eb3d9cd, 0x0cdc7b50, 0x4774108d, 0x7d5de585,
    0x1230c9c4, 0x73ed9ed4, 0x93deb583, 0x42fb1070, 0x32954863, 0x4a747267,
    0xeaa7b9a2, 0x6a828322, 0x8031c036, 0x772e7b69, 0xb6278456, 0x718ca706,
    0x8e86a6a6, 0x4d4ac1db, 0x495435a9, 0x55ac74ea, 0xac4f0fc0, 0xdbd88ac6,
    0xf170cb1a, 0x5236e491, 0x38d42015, 0xf8748fd2, 0x9e2a7d3a, 0x36079063,
    0x92a698e7, 0xe555979b, 0xf80f7161, 0xa610e567, 0x1e9f682b, 0x56502fac,
    0x554c789d, 0x155199b3, 0x77c7128f, 0xfca60433, 0x37258fa8, 0x56b54378,
    0xfd12c7f1, 0xffb3fc4c, 0x9457aa80, 0xc01f2c8a, 0x071c3aa0, 0xfd9b91fd,
    0xd4331755, 0x71d89bc4, 0x7f46d88b, 0x4dfaf768, 0xeda0e0da, 0x44711a1c,
    0x5f920f3b, 0x0623a9e2, 0x772127b7, 0x90e5f4c2, 0x8d36892e, 0x63b79e74,
    0x5fe5acd7, 0x12c89eca, 0x864d1033, 0x07858d7e, 0xb88ac820, 0x2629fa2b,
    0x2216d818, 0xce998dbe, 0x6f0e145d, 0x34bbd657, 0xbb0591f1, 0xcd68710d,
    0x438d6498, 0x284e282f, 0x1ac990ae, 0xcc9d8c8e, 0x23008b94, 0xfa8e9b19,
    0x53909eb9, 0x58f6c1d5, 0xc5fc5e38, 0x6e65518f, 0x5ebfe405, 0x6d6c4d3b,
    0x5c8555ba, 0x5d73a2a2, 0x7198a6ae, 0xf7b24564, 0xcacfe138, 0x284708d3,
    0x24fc81a6, 0x9a5ae01b, 0xc37cad95, 0xd7265433, 0x2ed99b2b, 0x1133b7d1,
    0x90196b6b, 0xbf8d1974, 0xe689b353, 0xed0329a6, 0x9ff6b8c8, 0x783f44a7,
    0x4b9f7322, 0x34016bac, 0xa7e8eee9, 0x9d627e1c, 0x4e187426, 0x358c8cca,
    0xd4aa77c7, 0x93330d6c, 0xd800aee8, 0x928fa710, 0x41f999d3, 0x8b66115e,
    0xabf93710, 0xdb9305d6, 0x6098188d, 0x2e010f93, 0xaaa608eb, 0xaffd885d,
    0x9e6736db, 0x43770ab9, 0x7fb76047, 0xded6ef8e, 0x49c229d1, 0xa37b8b72,
    0xc25d18af, 0xacda81b8, 0x2708a5f3, 0x45043f2d, 0x6b601618, 0x9334531a,
    0xff347013, 0x4d31531b, 0xcef7d88a, 0x12342d0d, 0xefa5d316, 0xbd156e71,
    0xf34de15d, 0x2270fffa, 0xdfb623d5, 0x1be313b1, 0x66a963b8, 0xa160112f,
    0x052d9dc8, 0x8715591b, 0x7cbd5571, 0xe35bf59e, 0x7baa5dbe, 0xb0d7ab94,
    0x588b1b24, 0x6b9259a9, 0x4a425d3f, 0xc2da2ce0, 0xd37ef2c7, 0xf2a75446,
    0x096e62e2, 0xb42dab7a, 0xb6180985, 0x2be8dd83, 0x7138e515, 0xca803bcd,
    0xf06415cb, 0xd872385d, 0x30a3d4e6, 0x50a6279e, 0x53784f85, 0xd5427217,
    0xa834b4bc, 0xb3213e26,
};

uint32_t __attribute__((optimize("unroll-loops")))  //__attribute__((noinline))
adler32_orig(char *data, size_t nbytes) {
  static uint64_t MOD_ADLER = 65521;
  uint64_t a = 1, b = 0;

  for (size_t i = 0; i < nbytes; i++) {
    a += (uint8_t)data[i];
    b += a;
  }

  a %= MOD_ADLER;
  b %= MOD_ADLER;
  return (b << 16) | a;
}

#if 0
void adler32_chunk_orig(uint64_t &a, uint64_t &b, char const *data)
{
    for (size_t i = 0; i < 8; i++) {
        a += (uint8_t) data[i];
        b += a;
    }
#ifndef NDEBUG
    printf("a=%08lx b=%08lx\n", a, b);
#endif
    for (size_t i = 8; i < 16; i++) {
        a += (uint8_t) data[i];
        b += a;
    }
}


void adler32_chunk_fast(uint64_t &a, uint64_t &b, bvec *bx)
{
    /*    
    uint64_t
        d0=x[0], d1=x[1], d2=x[2], d3=x[3],
        d4=x[4], d5=x[5], d6=x[6], d7=x[7],
        d8=x[8], d9=x[9], da=x[10], db=x[11],
        dc=x[12], dd=x[13], de=x[14], df=x[15];
    
    a = d0 + d1 + d2 + d3 + d4 + d5 + d6 + d7 +
        d8 + d9 + da + db + dc + dd + de + df;
        
    b = 16*d0 + 15*d1 + 14*d2 + 13*d3 + 12*d4 + 11*d5 + 10*d6 +  9*d7 +
         8*d8 +  7*d9 +  6*da +  5*db +  4*dc +  3*dd +  2*de +  1*df;


    When done vector-wise, the "size" of b[0] follows:
    
        8 16 | 24 32 |  40  48 |  56  64 | ...
        8 24   48 80   120 168   224 288 ...

    while b[7] follows:
        1 9 | 17 25 | 33 41 | 49 57 | ...
        10 52 126 232

    So we'd have to stop after three rounds or risk overflow.

    However, if we rearranged the calculation like this:
    
    a = d0 + d1 + d2 + d3 + d4 + d5 + d6 + d7 +
        df + de + dd + dc + db + da + d9 + d8;
        
    b = 16*d0 + 15*d1 + 14*d2 + 13*d3 + 12*d4 + 11*d5 + 10*d6 +  9*d7 +
         1*df +  2*de +  3*dd +  4*dc +  5*db +  6*da +  7*d9 +  8*d8;

    Then b[0] and b[7] both grow with the same speed:

    1 16 | 17 32 | 33 48 | 49 64 | ...
    8  9 | 24 25 | 40 41 | 46 57 | ...

    17 66 147 260

    8+16+24+32 = 
    The "size" of a and b after each 8-byte chunk:

    b=0
    a=0

    b=8 = 8*0 + 8
    a=1 = a+1

    b=16 = 8*1 + 8
    a=2 = a+1

    b=24 = 8*2 + 8
    a=3

    b=32
    */
    bvec bzero = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
    union {
        bvec bx[2];
        myvec x[2];
    } u = {{__builtin_ia32_punpcklbw128(bx[0], bzero), __builtin_ia32_punpckhbw128(bx[0], bzero)}};
    myvec zero = {0,0,0,0,0,0,0,0};
    myvec s1 = zero, s2 = zero;
    myvec c1 = {8,7,6,5,4,3,2,1};

#ifndef NDEBUG
    printf("data[0]=%02x data[15]=%02x\n"
           "  bx[0]=%02x   bx[15]=%02x\n"
           "x[0][0]=%02x  x[1][7]=%02x\n",
           ((uint8_t*)bx)[0],  ((uint8_t*)bx)[15],
           (uint8_t) (*bx)[0], (uint8_t) (*bx)[15],
           (uint16_t) u.x[0][0], (uint16_t) u.x[1][7]);
#endif
    s2 = c1*u.x[0];
    s1 = u.x[0];

#ifndef NDEBUG
    myvec tmp;
    tmp = __builtin_ia32_phaddw128(s1, s2);
    tmp = __builtin_ia32_phaddw128(tmp, zero);
    tmp = __builtin_ia32_phaddw128(tmp, zero);
    printf("a=%08lx b=%08lx\n", a+(int) tmp[0], 8*a + b+(int) tmp[1]);
#endif
    
    s2 += 8*s1 + c1*u.x[1];
    s1 += u.x[1];

    // 16 -> 8 -> 4 -> 2
    s1 = __builtin_ia32_phaddw128(s1, s2);
    s1 = __builtin_ia32_phaddw128(s1, s1);
    s1 = __builtin_ia32_phaddw128(s1, s1);

    // don't forget to count all those times [a] would have been added...
    b += 16*a + s1[1];
    a += s1[0];
}

void adler32_chunk_faster(uint64_t &a, uint64_t &b, bvec *bx)
{
    bvec bzero = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
    myvec x[2] = {
        (myvec) __builtin_ia32_punpcklbw128(bx[0], bzero),
        (myvec) __builtin_ia32_punpckhbw128(bx[0], bzero)
    };
    myvec s1, s2, tmp;
    myvec c1 = {8,7,6,5,4,3,2,1};

    tmp = x[0];
    s1 = tmp;
    s2 = c1*tmp;

    s2 += 8*s1; // copy s1

    tmp = x[1];
    s1 += tmp;
    s2 += c1*tmp;

    // 16 -> 8 -> 4 -> 2
    s1 = __builtin_ia32_phaddw128(s1, s2);
    s1 = __builtin_ia32_phaddw128(s1, s1);
    s1 = __builtin_ia32_phaddw128(s1, s1);

    // don't forget to count all those times [a] would have been added...
    b += 16*a + s1[1];
    a += s1[0];
}

void
__attribute__((optimize("unroll-loops")))
adler32_chunk_fast4(uint64_t &a, uint64_t &b, bvec *bx)
{
    bvec bzero = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
    myvec zero = {0,0,0,0,0,0,0,0};
    myvec s1, s2, tmp, s2_delta;
    myvec c1 = {8,7,6,5,4,3,2,1};

    /* We cannot allow the "size" of any element in s1 or s2 to
       overflow 16 bits. This means we primarily have to worry about
       s2[0], because it grows the fastest.

       We have to assume every input byte has value 255, so we can
       only add 257 of them together safely (255*257 = 65535). We
       refer to the number of additions a register has accumulated so
       far as its "size."

       Note that elements of s1 only grow by 1 at each step. It's s2
       that has the nasty quadratic behaviour.
     */
    s2 = zero;
    
    tmp = (myvec) __builtin_ia32_punpcklbw128(bx[0], bzero);
    s2_delta =         c1*tmp;
    s1 =  tmp;
    // size 1,8

    for (int i=1; i < 8; i++) {
        if (i%2) 
            tmp = (myvec) __builtin_ia32_punpckhbw128(bx[i/2], bzero);
        else
            tmp = (myvec) __builtin_ia32_punpcklbw128(bx[i/2], bzero);
        
        s2 += s2_delta;
        s2_delta = 8*s1 + c1*tmp;
        s1 += tmp;
        // size 2,24  3,48  4,80  5,120  6,168  7,224  8,224+64
    }
    
    /* The final computation is:
       
       b += 64*a + sum(s2) + sum(s2_delta)
       a += sum(s1)

       S1 can be reduced directly (8*8 = 64 < 257). However, s2 is
       about to overflow, so we have to convert it to a pair of 32-bit
       vectors before starting the reduction. S2_delta can handle a
       partial reduction, so we convert it to 32-bit part way through.
     */
    
    // reduce s1 and s2_delta to 4 values each, sharing a single register
    s1 = __builtin_ia32_phaddw128(s1, s2_delta);

    /* unpack the 4 s2_delta values to 32-bit integers. Unpack s2
       while we're at it.
    */
    typedef int32_t __attribute__((vector_size(16))) dvec;
    dvec d[] = {
        (dvec) __builtin_ia32_punpcklwd128(s2, zero),
        (dvec) __builtin_ia32_punpckhwd128(s2, zero),
        (dvec) __builtin_ia32_punpckhwd128(s1, zero),
    };

    // (4) (4) 4 -> (2 2) (4) -> (1 1 2) -> (2) -> (1)
    d[0] = __builtin_ia32_phaddd128(d[0], d[1]);
    d[0] = __builtin_ia32_phaddd128(d[0], d[2]);
    d[0] = __builtin_ia32_phaddd128(d[0], d[0]);
    d[0] = __builtin_ia32_phaddd128(d[0], d[0]);

    // don't forget all those times [a] would have been added...
    b += 64*a + d[0][0];
    
    // finish off s1: 4 -> 2 -> 1
    s1 = __builtin_ia32_phaddw128(s1, s1);
    s1 = __builtin_ia32_phaddw128(s1, s1);

    a += s1[0];
}


/* This SSE-enabled version of adler32 is ~3.6x faster than the scalar
   one, even after loop unrolling. For a workload that fits in cache,
   it can hit process roughly ~4.7GB/s. Obviously memory bandwidth
   limitations, or very short runs, might eat into that throughtput.

   (There are still severe bottlenecks, BTW: adler32 examines bytes,
   so the ideal SIMD speedup would be 16. Even accounting for the fact
   that we have to examine half words, speedup should still be 8.
 */
uint32_t __attribute__((noinline)) adler32_custom(char *data, size_t nbytes)
{
    static uint64_t MOD_ADLER = 65521;
    uint64_t a = 1, b = 0;
    size_t i = 0;
    if (nbytes < 16)
        goto finish;

    // find next alignment boundary
    for (; ((uintptr_t) &data[i]) & 0xf; i++)
        adler32_single(a, b, data+i);

    // big chunks
    for (; i+64 < nbytes; i+=64)
        adler32_chunk_fast4(a, b, (bvec*) (data+i));

    // smaller chunks
    for (; i+16 < nbytes; i+=16) 
        adler32_chunk_faster(a, b, (bvec*) (data+i));

    // finish it off
 finish:
    for (; i < nbytes; i++)
        adler32_single(a, b, data+i);

#ifndef NDEBUG
    printf("a=%08lx b=%08lx\n", a, b);
#endif
    a %= MOD_ADLER;
    b %= MOD_ADLER;
    return (b << 16) | a;
}
#endif

#include "stopwatch.h"
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

void bakeoff(char const *msg, size_t nbytes, char *data, size_t len) {
  printf("\n\n%s (%zd bytes):\n", msg, len);
  size_t ntimes = nbytes / len;
  if (not ntimes) return;

  // make a dest buffer with proper alignment
  char *dest_buf = (char *)malloc(len + 32);
  uintptr_t n = (uintptr_t)data;
  n &= 0xf;
  uint32_t x = 0;
  uintptr_t m = (uintptr_t)dest_buf;
  m = (m + 0xf) & ~0xf;
  m += n;
  char *dest = (char *)m;

  // go!
  stopwatch_t timer;
  size_t tick = ntimes / 10;
  if (not tick) tick = 1;

#if 0
    for (size_t i=0; i < ntimes; i++) {
        if (not (i%tick))
            fprintf(stderr, ".");
        x = adler32_orig(data, len);
    }
    printf("\n%08xd %.3f adler3_orig\n", x, timer.time());
#endif

  for (size_t i = 0; i < ntimes; i++) {
    if (not(i % tick)) fprintf(stderr, ".");
    x = adler32_vanilla(data, len);
  }
  printf("\n%08xd %.3f adler32_vanilla\n", x, timer.time());

  for (size_t i = 0; i < ntimes; i++) {
    if (not(i % tick)) fprintf(stderr, ".");
    x = adler32_memcpy_vanilla(dest, data, len);
  }
  ASSERT(not memcmp(dest, data, len));
  printf("\n%08xd %.3f adler32_memcpy_vanilla\n", x, timer.time());

#ifdef __SSSE3__
  for (size_t i = 0; i < ntimes; i++) {
    if (not(i % tick)) fprintf(stderr, ".");
    x = adler32_sse(data, len);
  }
  printf("\n%08xd %.3f adler32_sse\n", x, timer.time());

  for (size_t i = 0; i < ntimes; i++) {
    if (not(i % tick)) fprintf(stderr, ".");
    x = adler32_memcpy_sse(dest, data, len);
  }
  printf("\n%08xd %.3f adler32_memcpy_sse\n", x, timer.time());
  ASSERT(not memcmp(dest, data, len));
#endif
  free(dest_buf);
}

int main(int argc, char const *argv[]) {
  size_t nbytes = 2ull * 1000 * 1000 * 1000;

  char *data;
  size_t len;

  if (1) {
    int fd = open(argv[0], O_RDONLY);
    struct stat buf;
    fstat(fd, &buf);
    len = buf.st_size;
    data = (char *)mmap(0, len, PROT_READ, MAP_SHARED, fd, 0);
    fprintf(stderr, "Opening %s (%zd bytes)\n", argv[0], buf.st_size);
  } else {
    data = 1 + (char *)DATA;
    len = sizeof(DATA) - 1;
  }

  try {
    bakeoff("Very short", nbytes, data, 61);
    bakeoff("Short", nbytes, data, 127);
    bakeoff("Medium", nbytes, data, 1039);
    bakeoff("Full length", nbytes, data, len);
  } catch (illegal_argument &ex) {
    fprintf(stderr, "Yikes! Caught an illegal_argument exception!\n-> %s",
            ex.msg);
    exit(-1);
  }

  len = 1039;
  uint32_t csum = adler32_orig(data, len);
  printf("\n\n\n%08xd bulk[%zd]\n", csum, len);
  for (int i = 2; i < 10; i++) {
    size_t lsz = len / i, rsz = len - lsz;
    uint32_t left = adler32(data, lsz);
    uint32_t right = adler32(data + lsz, rsz);
    uint32_t lr = adler32_merge(left, right, rsz);
    printf("%08xd incr[%zd/%zd]\n", lr, lsz, rsz);
  }
  printf("\n%08xd bulk[%zd]\n", csum, len);
  for (int i = 2; i < 10; i++) {
    size_t lsz = len / i, rsz = len - lsz;
    uint32_t left = adler32(data, lsz);
    uint32_t lr = adler32(data + lsz, rsz, left);
    printf("%08xd cont[%zd/%zd]\n", lr, lsz, rsz);
  }
}
