// -*- mode:c++ -*-
#ifndef __DEFER_H
#define __DEFER_H

/* This header provides support for a class of DEFER macros, which can
   be used to register code that should run if the current block goes
   out of scope for any reason. That includes cases where the scope
   exits normally, when an exception is thrown, and even because a
   goto transfers control above the location of the DEFER.

   This strong "always runs" guarantee makes it easy to express
   try..finally semantics. The behavior is similar to the effect of a
   destructor call in the C++ RAII idiom, but eliminates the need to
   define RAII classes for simple or one-off uses. It also improves
   code readability by making very explicit the programmers intention
   (acquire, followed immediately by a deferred release), rather than
   hiding the acquire/release sementics in some other chunk of code
   (potentially declared far away from the use site).

   Unlike destructors, execution of the deferred code can also be made
   conditional. One extremely useful variant is to condition deferred
   execution on the scope exiting before execution reaches a certain
   point in the code. This would be similar to specifying a catch(...)
   { do_cleanup(); throw; } clause, but is far more efficient. In the
   common case, the compiler actually moves the deferred code to the
   stack unwind code, leaving the main code path completely
   unaffected.

   WARNING: this furequires several C++11 features, including lambdas,
   auto, and decltype
 */

/* Execute @stmt when the current scope exits for any reason.

   NOTE: deferred actions that are still in scope when a function
   returns execute *after* the function's return value has been
   computed. In other words, the following prints "10" and "0":

   int foo(int *ptr) {
       DEFER(*ptr = 0);
       return *ptr;
   }
   int main() {
       int x = 10;
       printf("%d\n", foo(&x));
       printf("%d\n", x);
   }
 */
#define DEFER(stmt) XDEFER([&] { stmt; })

/*Invoke the lambda @x() when the current scope exits for any reason.
 */
#define XDEFER(x) _XDEFER(x, __LINE__)

#define _XDEFER(x, line) __XDEFER(x, line)

#define __XDEFER(x, line)          \
  auto __x##line = [&] { (x)(); }; \
  auto __defer##line = __defer<decltype(__x##line)>(__x##line)

/* Define a variable, @commit, initialized to false, and register a
   deferred action; the action will be performed when the current
   scope exits, unless @commit becomes true before then.

   Most of the time, the compiler can statically analyze the behavior
   of @commit and host the taken/not-taken branches into main and
   unwind code paths as appropriate (eliminating the conditional
   branch in the code that would otherwise be required).
 */
#define DEFER_UNLESS(commit, x) \
  bool commit = false;          \
  XDEFER_UNLESS(commit, x)

/* Similar to DEFER_UNLESS, but uses an existing @commit variable
   rather than declaring a new one.

   Useful for predicating multiple deferred actions on a single event.

   Less useful in the general case, because the compiler usually can't
   rule out statically the possibility that @commit might be false
   when the main code path exits, and so would have to leave a
   conditional branch in place.
 */
#define XDEFER_UNLESS(commit, x) \
  XDEFER([&] {                   \
    if (not(commit)) {           \
      x;                         \
    }                            \
  })

/* Similar to XDEFER_UNLESS, but the action [x] will only be performed
   if the existing variable named by [commit] evalutes to true.
 */
#define XDEFER_IF(commit, x) \
  XDEFER([&] {               \
    if (commit) {            \
      x;                     \
    }                        \
  })

/* Helper class that supports the above macros */
template <typename T>
struct __defer {
  T &fn;
  __defer(T &fn) : fn(fn) {}
  ~__defer() { fn(); }
  __defer(const __defer &) = default;
};

#endif
