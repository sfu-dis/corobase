#include "window-buffer.h"

#include "sm-defs.h"
#include "w_rand.h"

#include <cstdlib>
#include <cstdio>
#include <deque>

int main() {
  try {
    window_buffer buf(1 << 16);

    // test ability to fill and then empty
    size_t wstart = buf.write_begin();
    char *wbuf = buf.write_buf(wstart, buf.available_to_write());
    ASSERT(wbuf);
    for (size_t i = 0; i < buf.available_to_write(); i++) wbuf[i] = (char)i;

    buf.advance_writer(buf.write_end());

    size_t rstart = buf.read_begin();
    char const *rbuf = buf.read_buf(rstart, buf.available_to_read());
    ASSERT(rbuf);
    for (size_t i = 0; i < buf.available_to_read(); i++)
      ASSERT(rbuf[i] == (char)i);

    buf.advance_reader(buf.read_end());
    ASSERT(buf.available_to_write() == buf.window_size());

    // now mix it up
    w_rand rng;
    size_t rbytes = buf.read_begin();
    size_t wbytes = buf.write_begin();
    std::deque<char> reference;
    while (rbytes < 128 * 1024 * 1024) {
      if (rng.randn(2)) {
        // try to write some number of bytes
        size_t avail = buf.available_to_write();
        if (not avail) continue;

        size_t nbytes = rng.randn(1, avail);
        char *wbuf = buf.write_buf(wbytes, nbytes);
        for (size_t i = 0; i < nbytes; i++) {
          char val = (char)rng.randn(256);
          reference.push_back(val);
          wbuf[i] = val;
        }
        wbytes += nbytes;
        buf.advance_writer(wbytes);
      } else {
        // try to read some bytes
        size_t avail = buf.available_to_read();
        if (not avail) continue;

        size_t nbytes = rng.randn(1, avail);
        char const *rbuf = buf.read_buf(rbytes, nbytes);
        for (size_t i = 0; i < nbytes; i++) {
          char val = reference.front();
          reference.pop_front();
          ASSERT(rbuf[i] == val);
        }
        rbytes += nbytes;
        buf.advance_reader(rbytes);
      }
    }
    while (not reference.empty()) {
      char val = reference.front();
      reference.pop_front();
      ASSERT(val == *buf.read_buf(rbytes, 1));
      rbytes++;
      buf.advance_reader(rbytes);
    }
  } catch (...) {
    fprintf(stderr, "Yikes!\n");
    exit(-1);
  }
}
