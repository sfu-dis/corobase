#include "sm-exceptions.h"

#include "sm-common.h"

#include <stdarg.h>
#include <stdio.h>

illegal_argument::illegal_argument(char const volatile *m, ...) {
  va_list ap;
  va_start(ap, m);
  DEFER(va_end(ap));

  int err = vasprintf(&free_msg, (char const *)m, ap);
  if (err < 0) {
    msg = "<error formatting message>";
    free_msg = 0;
  } else {
    msg = free_msg;
  }
}

os_error::os_error(int e, char const volatile *m, ...) : err(e) {
  va_list ap;
  va_start(ap, m);
  DEFER(va_end(ap));

  int err = vasprintf(&free_msg, (char const *)m, ap);
  if (err < 0) {
    msg = "<error formatting message>";
    free_msg = 0;
  } else {
    msg = free_msg;
  }
}

log_file_error::log_file_error(char const volatile *m, ...) {
  va_list ap;
  va_start(ap, m);
  DEFER(va_end(ap));

  int err = vasprintf(&free_msg, (char const *)m, ap);
  if (err < 0) {
    msg = "<error formatting message>";
    free_msg = 0;
  } else {
    msg = free_msg;
  }
}
