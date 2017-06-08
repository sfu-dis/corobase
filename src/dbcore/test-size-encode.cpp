#include "size-encode.h"

// Intentional -- tester needs access to private state...
#include "size-encode.cpp"

#include "sm-defs.h"

#include <cstdio>

int main() {
  for (auto it : enumerate(_encode_size_tab)) {
    printf("%6d ", (int)it.second);
    if (not((it.first + 1) % 8)) printf("\n");
  }

  printf("FYI: min/max representable values: %zd %zd\n", decode_size(0x00),
         decode_size(0xfe));
  printf("Verify that code -> value -> code is consistent...\n");
  for (int i = 0; i < 256; i++) {
    int x = decode_size(i);
    int j = encode_size(x);
    if (j != i)
      printf("\tOops! Non-reversible code: %02x -> %6d -> %02x\n", i, x, j);
  }

  printf("Verify decoding of the two edge cases...\n");
  int val;
  if ((val = decode_size(0)))
    printf("\tOops! Zero code decodes to %d (instead of 0)\n", val);
  if ((val = decode_size(0xff)) != -1)
    printf("\tOops! Invalid code decodes to %d (instead of (-1)\n", val);

  printf("Verify encoding of the two edge cases...\n");
  int code;
  if ((code = encode_size(0))) printf("\tOops! Zero encodes to %02x\n", code);
  if ((code = encode_size(decode_size(0xff))) != 0xff)
    printf("\tOops! Out of range value %zd encodes to %02x\n",
           decode_size(0xff), code);

  printf("Verify we always round sizes up...\n");
  for (int i = EFIRST(0); i < (int)ELAST(15); i++) {
    uint8_t code = encode_size(i);
    int j = decode_size(code);
    if (j < i) printf("\tOops! %6d -> %02x -> %6d\n", i, code, j);
  }

  printf("Verify we don't round sizes up too far...\n");
  for (int i = EFIRST(1); i < (int)ELAST(15); i++) {
    uint8_t code = encode_size(i);
    int j = decode_size(code - 1);
    if (j >= i)
      printf("\tOops! %6d -> %02x, but %02x -> %6d\n", i, code, code - 1, j);
  }

  printf("Verify that codes are ordered the same as their decoded values...\n");
  for (int i = 1; i < 255; i++) {
    int a = decode_size(i - 1);
    int b = decode_size(i);
    if (a >= b) printf("\tOops! %02x < %02x -> %d >= %d\n", i - 1, i, a, b);
  }

  printf(
      "Check whether any multiples of powers of two are not represented "
      "exactly (expect 7)...\n");
  for (size_t i = 2; decode_size(encode_size(i)) == i; i *= 2) {
    for (int j = 2; j < 32 and encode_size(j * i) != 0xff; j++) {
      if (decode_size(encode_size(j * i)) != j * i)
        printf("\t%d * %zd = %zd -> %zd\n", j, i, j * i,
               decode_size(encode_size(j * i)));
    }
  }
}
