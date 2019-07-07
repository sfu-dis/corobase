// -*- mode:c++ -*-
#ifndef __SM_EXCEPTIONS_H
#define __SM_EXCEPTIONS_H
#include <stdio.h>

/** The exception hierarchy used in the SM.

    Exceptions are used extensively for "unexpected" (= uncommon)
    events because they move the handling code off the critical path
    and allow the compiler to generate better code. Exceptions are
    particularly helpful for dealing with cases where the "handler" of
    an event is several steps up the call chain; the alternative would
    be to manually unwind the stack by repeatedly returning the value
    until some caller decides to deal with it.

    Exception dispatch and unwind is expensive and unscalable,
    however, so it should only be used for truly uncommon events. As a
    general rule of thumb, if the event would occur more than 0.1% of
    the time, it's probably better to handle it with a return value
    rather than an exception (and if that results in any significant
    amount of code for manual unwinding, it's probably time to
    re-think the design)

    TODO: A major reason for throwing exceptions rather than returning
    error codes is that we can use the stack unwinding machinery to
    build a stack trace if we want. The best mechanism is probably
    gcc's new libbacktrace, which uses debug info to grab stack traces
    the same way gdb does. It is available with version 4.8 and later
    if configured properly (the easiest way is to enable support for
    the Go language). Second best is gcc's internal <unwind.h>, which
    uses the exception unwind mechanism to pull all pending return
    addressess (addr2line can be used to decode them manually).

    TODO: At some point it will probably make sense to make an hierarchy of
    exceptions, but for now we just create a flat list.

 */
#include <cstddef>
#include <cstdlib>

#define THROW_IF(cond, eclass, ...)                              \
  do {                                                           \
    if (cond) {                                                  \
      printf("Exception thrown at %s:%d\n", __FILE__, __LINE__); \
      throw eclass{__VA_ARGS__};                                 \
    }                                                            \
  } while (0)

#define THROW_UNLESS(cond, eclass, ...) \
  THROW_IF(not(cond), eclass, ##__VA_ARGS__)

/* C++ considers the following two functions ambiguous:

   foo(char const *)
   foo(char const *, ...)

   ... but not these:

   foo(char const *)
   foo(char const volatile *, ...)

   This lets us differentiate whether a message needs formatting or
   not, and thus whether it will need to be freed later or not.

   Unfortunately, we can't use C++11 inheriting constructors because
   they are incompatible with varargs. We also can't use variadic
   templates because they are incompatible with
   __attribute__((format)). Duplicating code once per exception type
   is less objectionable than losing the compile-time check of format
   args, so we do the boilerplate way.
 */

/* The code attempted to call a function in an unsupported way. No
   damage was done, but the call cannot be repeated as-is and
   indicates a bug in either the SM code or in a user
   transaction. Most likely the former.

   An error message string can be provided, and (if indicated) should
   be passed to rcu_free() by whoever consumes the exception.
 */
struct illegal_argument {
  char const *msg;
  char *free_msg;
  illegal_argument(char const *m = "Illegal argument") : msg(m), free_msg(0) {}
  illegal_argument(char const volatile *m, ...)
      __attribute__((format(printf, 2, 3)));
};

struct os_error {
  char const *msg;
  char *free_msg;
  int err;
  os_error(int e, char const *m) : msg(m), free_msg(0), err(e) {}
  os_error(int e, char const volatile *m, ...)
      __attribute__((format(printf, 3, 4)));
  ~os_error() { free(free_msg); }
};

/* Indicates that RCU failed to allocate memory. The caller must end
   the current RCU transaction before retrying (though this does not
   guarantee success because the system really could be out of memory).
 */
struct rcu_alloc_fail {
  size_t nbytes;
  rcu_alloc_fail(size_t n) : nbytes(n) {}
};

struct log_is_full {
  /* no members */
};

/* Something went wrong with log file handling that prevents any more
   logging. Probably not recoverable.
 */
struct log_file_error {
  char const *msg;
  char *free_msg;
  log_file_error(char const *m) : msg(m), free_msg(0) {}
  log_file_error(char const volatile *m, ...)
      __attribute__((format(printf, 2, 3)));
  ~log_file_error() { free(free_msg); }
};

/* Indicates that no more OIDs can be allocated in the specified table.
 */
struct table_is_full {};

#endif
