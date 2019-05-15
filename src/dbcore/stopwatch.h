/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _STOPWATCH_H
#define _STOPWATCH_H

#include <time.h>
#include <stdint.h>

/**
 *  @brief a timer object.
 */
class stopwatch_t {
 private:
  uint64_t _mark;

 public:
  stopwatch_t() { reset(); }
  uint64_t time_ns() {
    uint64_t old_mark = _mark;
    return reset() - old_mark;
  }
  double time_us() { return time_ns() * 1e-3; }
  double time_ms() { return time_ns() * 1e-6; }
  double time() { return time_ns() * 1e-9; }

  /* reads the clock without storing the result */
  static uint64_t now() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    return tv.tv_nsec + tv.tv_sec * 1000000000ll;
  }

  /* returns whatever was last read */
  uint64_t mark() const { return _mark; }

  operator uint64_t() const { return mark(); }

  /* reads the clock and sets the mark to match */
  uint64_t reset() { return _mark = now(); }
};

#endif
