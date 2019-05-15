#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <tuple>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

namespace ermia {

/* Low-level definitions that can be used anywhere. This file must be
   the root of any #include dependency tree it is part of.
 */

/* The log allocates things in chunks of 16 bytes, and requires that
   all buffers passed to it be 16-byte aligned. Use this whenever
   defining something that might end up in the log, to ensure proper
   alignment.
 */
#define DEFAULT_ALIGNMENT_BITS 4
#define DEFAULT_ALIGNMENT (1 << DEFAULT_ALIGNMENT_BITS)

#ifdef __GCC__
#define LOG_ALIGN __attribute__((aligned(DEFAULT_ALIGNMENT)))
#else
#define LOG_ALIGN
#endif

/* Override std::min and std::max to be sane about
   different-but-compatible types. In case the two arguments have the
   same type, a reference to the smaller is returned, and no copy is
   made (as with std::min). If the two types differ, but a suitable
   operator+ exists such that both a+b and b+a yield the same type,
   then the expression is accepted and returns a copy of the smaller
   input, converted to the the common type of a and b; alternatively,
   the user can specify the third type explicitly, though that usage
   scenario would be exceedingly rare. Also, make everything
   constexpr, because we can.

   NOTE: while it would seem attractive for the different-type version
   to accept rvalue references (or at least const references) to avoid
   unnecessary copying, that idea requires taking the address of the
   arguments, nad thus interacts badly with static const class
   variables that were initialized inline (causing linker failures).
 */
template <typename T>
static constexpr T const &min(T const &a, T const &b) {
  return (a < b) ? a : b;
}

template <typename A, typename B,
          typename C = typename std::enable_if<
              not std::is_same<A, B>::value,
              typename std::common_type<A, B>::type>::type>
static constexpr C min(A a, B b) {
  return (a < b) ? C(a) : C(b);
}

template <typename T>
static constexpr T const &max(T const &a, T const &b) {
  return (a < b) ? a : b;
}

template <typename A, typename B,
          typename C = typename std::enable_if<
              not std::is_same<A, B>::value,
              typename std::common_type<A, B>::type>::type>
static constexpr C max(A a, B b) {
  return (a < b) ? C(a) : C(b);
}

/* A wrapper for std::memset that zeroes out memory between two typed
   pointers for the begin and end points of the range to be
   initialized. Defaults to filling with zero bytes if [c] is not
   specified.
 */
template <typename T>
static inline typename std::enable_if<std::is_trivial<T>::value, T>::type *
objzero(T *begin, T *end) {
  return (T *)std::memset(begin, 0, sizeof(T) * (end - begin));
}
template <typename T>
static inline typename std::enable_if<std::is_trivial<T>::value, T>::type *
objzero(T *begin, size_t n) {
  return (T *)std::memset(begin, 0, sizeof(T) * n);
}
template <typename T, size_t N>
static inline
    typename std::enable_if<std::is_trivial<T>::value, T>::type *objzero(
        T(&arr)[N]) {
  return objzero(arr, N);
}

/* A wrapper for std::memcpy that accepts typed pointers for begin and
   end points of source and dest. Only works for trivial data types.
 */
template <typename T>
static inline typename std::enable_if<std::is_trivial<T>::value, T>::type *
objcopy(T *dest, T const *src_begin, T const *src_end) {
  return ::memcpy(dest, src_begin, sizeof(T) * (src_end - src_begin));
}

template <typename T>
static inline typename std::enable_if<std::is_trivial<T>::value, T>::type *
objcopy(T *dest, T const *src, size_t n) {
  return (T *)::memcpy(dest, src, sizeof(T) * n);
}

/* Align integers up/down to the nearest alignment boundary, and check
   for existing alignment
 */
template <typename T, typename U = int>
static constexpr typename std::common_type<T, U>::type align_down(
    T val, U amount = DEFAULT_ALIGNMENT) {
  return val & -amount;
}

template <typename T, typename U = int>
static constexpr typename std::common_type<T, U>::type align_up(
    T val, U amount = DEFAULT_ALIGNMENT) {
  return align_down(val + amount - 1, amount);
}

template <typename T, typename U = int>
static constexpr bool is_aligned(T val, U amount = DEFAULT_ALIGNMENT) {
  return not(val & (amount - 1));
}

/* A zero-sized object that serves only to force alignment of whatever
   follows it in a class declaration.
 */
template <int N>
struct __attribute__((aligned(N))) aligner {
  char *_empty;
};

// lest there be any confusion...
template <typename T>
static inline T volatile_read(T volatile &x) {
  return *&x;
}

template <typename T, typename U>
static inline void volatile_write(T volatile &x, U const &y) {
  *&x = y;
}

/* If [x] is not true, abort
 */
#ifndef NDEBUG
#define ASSERT(x) _MSG_IF(ASSERTION_FAILURE, not(x), #x, "")
#else
#define ASSERT(x) ;
#endif

/* If [x] is not true, abort with an extra message (perhaps to print
   offending values)
 */
#define ASSERT_MSG(x, msg, ...) \
  _MSG_IF(ASSERTION_FAILURE, not(x), #x, ". Extra info: " msg, ##__VA_ARGS__);

#ifndef NDEBUG
#define ASSERTION_FAILURE(cond, msg, ...) \
  DIE("assertion failure: %s" msg, cond, ##__VA_ARGS__)
#else
#define ASSERTION_FAILURE(cond, msg, ...) __builtin_unreachable()
#endif

/* Inform the compiler about a condition it can assume holds. In debug
   mode, this is equivalent to an assertion (abort if the condition
   does not hold), but in production mode, it gives an optimization
   hint that may allow the compiler to generate better code.
 */
#ifndef NDEBUG
#define ASSUME(x) ASSERT(x)
#else
#define ASSUME(x)                        \
  do {                                   \
    if (not(x)) __builtin_unreachable(); \
  } while (0)
#endif

#define DO_IF(cond, what) \
  do {                    \
    if (cond) {           \
      what;               \
    }                     \
  } while (0)

#define _MSG_IF(tp, cond, msg, ...) DO_IF(cond, tp(msg, ##__VA_ARGS__))

#define DIE_IF(cond, msg, ...) _MSG_IF(DIE, cond, msg, ##__VA_ARGS__)

#define SPAM(msg, ...)                                        \
  fprintf(stderr, "%s:%d: %s: " msg "\n", __FILE__, __LINE__, \
          __PRETTY_FUNCTION__, ##__VA_ARGS__)

#define SPAM_IF(cond, msg, ...) _MSG_IF(SPAM, cond, msg, ##__VA_ARGS__)

/* Print a warning but continue normally

   Good for things that might be a red flag (e.g. failure to close a
   file descriptor), or to document things gone wrong in a destructor.
 */
#ifdef NWARNING
#define WARN(msg, ...)
#define WARN_IF(cond, msg, ...)
#else
#define WARN(msg, ...) SPAM("WARNING: " msg, ##__VA_ARGS__)
#define WARN_IF(cond, msg, ...) _MSG_IF(WARN, cond, msg, ##__VA_ARGS__)
#endif

/* Print a message and terminate abnormally */
#define DIE(msg, ...)       \
  SPAM(msg, ##__VA_ARGS__); \
  abort();

#ifdef __GNUC__
/* GCC's offsetof() macro is broken in C++ for versions <= 4.9.0 when
   array indexes are not known at compile time.

   See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=14932

   Clang works fine. Not sure about other compilers.

   Work around: good old-fashioned null pointer arithmetic...
 */
#define OFFSETOF(tp, expr) (((uint64_t)(&((tp *)0)->expr)) - ((uint64_t)((tp *)0)))
#else
#define OFFSETOF(tp, expr) offsetof(tp, expr)
#endif

/* A C++11 version of python's enumerate() function, intended for use
   with range-based for loops.
 */
template <typename T>
struct _enumerate_helper {
  T &iterable;
  typedef typename std::iterator_traits<typename T::iterator>::reference Ref;
  struct iterator {
    size_t i;
    typename T::iterator it;
    void operator++() {
      ++it;
      ++i;
    }
    std::pair<size_t, Ref> operator*() {
      return std::pair<size_t, Ref>(i, *it);
    }
    bool operator!=(iterator const &other) { return it != other.it; }
  };
  iterator begin() { return iterator{0, iterable.begin()}; }
  iterator end() { return iterator{0, iterable.end()}; }
};

// specialize it for arrays
template <typename T, size_t N>
struct _enumerate_helper<T[N]> {
  T *iterable;
  typedef T &Ref;
  struct iterator {
    size_t i;
    T *it;
    void operator++() {
      ++it;
      ++i;
    }
    std::pair<size_t, Ref> operator*() {
      return std::pair<size_t, Ref>(i, *it);
    }
    bool operator!=(iterator const &other) { return it != other.it; }
  };
  iterator begin() { return iterator{0, iterable}; }
  iterator end() { return iterator{N, iterable + N}; }
};

template <typename T>
_enumerate_helper<T> enumerate(T &iterable) {
  return _enumerate_helper<T>{iterable};
}

/* This tangle of C++11 allows to replace the following:

   very_long_class_name *f = new very_long_class_name(...);
   struct bar {
       very_long_class_name *f;
       bar(...) : f(new very_long_class_name(...)) { }
   };
   baz(new very_long_class_name(...));

   with:

   very_long_class_name *f = make_new(...);
   struct bar {
       very_long_class_name *f;
       bar(...) : f(make_new(...)) { }
   };
   baz(make_new(...));

   The first of those three is technically not shorter than

   auto *f = new very_long_class_name(...);

   But the intent is clear and type inference doesn't help for
   constructor initializers lists or other function calls.

   Everything inlines, so there is no runtime overhead.
 */

template <typename Tuple, size_t... i>
struct make_new_decay {
  Tuple tup;
  make_new_decay(Tuple t) : tup(std::move(t)) {}

  template <typename T>
  operator T *() {
    return new T{std::get<i>(tup)...};
  }
};

template <size_t N, typename Tuple, size_t... i>
struct make_new_helper : make_new_helper<N - 1, Tuple, N - 1, i...> {};

template <typename Tuple, size_t... i>
struct make_new_helper<0, Tuple, i...> {
  typedef make_new_decay<Tuple, i...> type;
};

template <typename... Args>
auto make_new(Args &&... args) ->
    typename make_new_helper<sizeof...(Args),
                             decltype(std::forward_as_tuple(args...))>::type

{
  return std::forward_as_tuple(args...);
}

}  // namespace ermia
