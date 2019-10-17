#ifndef SM_COROUTINE_H
#define SM_COROUTINE_H
#include <experimental/coroutine>

#include "sm-defs.h"
namespace ermia {
namespace dia {

template <typename T> struct generator {
  struct promise_type;
  using handle = std::experimental::coroutine_handle<promise_type>;

  struct promise_type {
    T current_value;
    auto get_return_object() { return generator{handle::from_promise(*this)}; }
    auto initial_suspend() { return std::experimental::suspend_never{}; }
    auto final_suspend() { return std::experimental::suspend_always{}; }
    void unhandled_exception() { std::terminate(); }
    auto return_value(T value) {
      current_value = value;
      return std::experimental::suspend_always{};
    }
  };

  auto get_handle() {
    auto result = coro;
    coro = nullptr;
    return result;
  }

  generator(generator const &) = delete;
  generator(handle h = nullptr) : coro(h) {}
  generator(generator &&rhs) : coro(rhs.coro) { rhs.coro = nullptr; }
  ~generator() {
    if (coro) {
      coro.destroy();
    }
  }

  generator &operator=(generator const &) = delete;
  generator &operator=(generator &&rhs) {
    if (this != &rhs) {
      coro = rhs.coro;
      rhs.coro = nullptr;
    }
    return *this;
  }

private:
  handle coro;
};

/*
 *  task<T> is implementation of coroutine promise. It supports chained
 *  co_await, which enables writing coroutine as easy as writing normal
 *  functions. However, the coroutine scheduling of task<T> may be a
 *  little different from other 'C++ coroutine library'.
 *
 *  When a chain of task<T> being executed, it somewhat goes like this:
 *
 *    function { coroutine_handle->resume(); }
 *      => coroutine1 { co_await task<Foo1>; }
 *        => coroutine2 { co_await task<Foo2>; }
 *          => coroutine3 { ... } => ...
 *
 *  Here `=>` represents a coroutine call through
 *  `coroutine_handle->resume()` or `co_await task<Foo>`
 *
 *  When `await_suspend` happens, for example in `coroutine2`, it hand over
 *  its control flow directly to the top level `function`. Therefore, any
 *  custom scheduling in `function` (could be seen as the main loop) can
 *  continue to work.
 */
namespace coro_task_private {

using generic_coroutine_handle = std::experimental::coroutine_handle<void>;

// final_awaiter is the awaiter returned on task<T> coroutine's `final_suspend`.
// It returns the control flow directly to the caller instead of the top-level
// function
struct final_awaiter {
  final_awaiter(generic_coroutine_handle coroutine)
      : caller_coroutine_(coroutine) {}
  ~final_awaiter() {}

  final_awaiter(const final_awaiter &) = delete;
  final_awaiter(final_awaiter &&other) : caller_coroutine_(nullptr) {
    std::swap(caller_coroutine_, other.caller_coroutine_);
  }

  constexpr bool await_ready() const noexcept { return false; }
  auto await_suspend(generic_coroutine_handle) const noexcept {
    return caller_coroutine_;
  }
  void await_resume() const noexcept {}

private:
  generic_coroutine_handle caller_coroutine_;
};

// task<T>::promise_type inherits promise_base. promise_base keeps track
// of the coroutine call stack. Basically a double link list of coroutines.
//
// promise_base.depend_on_promise_ points to promise which it co_awaits on
// (i.e. it depends on depend_on_promise_ to be resolved before it continues).
//
// promise_base.awaiting_promise_ points to the promise which depends on `this`
// promise. (i.e. promise_base.awaiting_promise_ depends on `this` to be
// resolved first)
//
// XXX:
// Probably the current 'link list' implementation can be replaced by a single
// vector<coroutine_handle> to track the coroutine call stack, which can have
// positve effect on performance.
// The idea is to let each `promise_base` hold a pointer of vector<>, namely
// `pCallStack`. Initialy let `pCallStack` be a nullptr. When a task<>
// being co_awaited, it will have knowledge of the its dependency in the chained
// coroutine call. It can initialize the vector<> if its the first call in the,
// or it takes the reference `pCallStack` of its parent and append itself into
// the vector<>.
struct promise_base {
  promise_base()
      : coroutine_handle_addr(nullptr), depend_on_promise_(nullptr),
        awaiting_promise_(nullptr) {}
  ~promise_base() {}

  promise_base(const promise_base &) = delete;
  promise_base(promise_base &&) = delete;

  auto initial_suspend() { return std::experimental::suspend_always{}; }
  auto final_suspend() {
    // For the first coroutine in the coroutine chain, it is started by
    // normal function through coroutine_handle.resume(). Therefore, it
    // does not be co_awaited on and has no awaiting_promise_.
    // In its final suspend, it returns control flow to its caller by
    // returning std::experimental::noop_coroutine.
    if (!awaiting_promise_)
      return coro_task_private::final_awaiter(
        std::experimental::noop_coroutine());
    // Every other coroutine in the coroutine chain returns control flow to the
    // caller instead of the top-level function by returning the final_awaiter
    // object which will resume its awaiting_promise_
    awaiting_promise_->clear_depend();
    return coro_task_private::final_awaiter(
        awaiting_promise_->get_promise_coroutine());
  }
  void unhandled_exception() { std::terminate(); }

  // TODO: Use arena allocator. Probably one arena for
  // each chain of coroutine task.
  // It is very important to use arena to reduce the
  // cache miss in access promise_base * which happens
  // a lot in task<T>.resume();

  // void *operator new(std::size_t) noexcept {}
  // void operator delete( void* ptr ) noexcept {}

  void set_depend_on_promise(const promise_base *promise) {
    depend_on_promise_ = promise;
  }
  const promise_base *get_depend_on_promise() const {
    return depend_on_promise_;
  }
  void clear_depend() const { depend_on_promise_ = nullptr; }

  void set_awaiting_promise(const promise_base *awaiting_promise) {
    awaiting_promise_ = awaiting_promise;
  }

  generic_coroutine_handle get_promise_coroutine() const {
    ASSERT(coroutine_handle_addr);
    return generic_coroutine_handle::from_address(coroutine_handle_addr);
  }

protected:
  void *coroutine_handle_addr;

  mutable const promise_base *depend_on_promise_;
  const promise_base *awaiting_promise_;
};

} // namespace coro_task_private

template <typename T> class task {
public:
  struct promise_type;
  struct awaiter;
  using coroutine_handle = std::experimental::coroutine_handle<promise_type>;

  task() : coroutine_(nullptr) {}
  task(coroutine_handle coro) : coroutine_(coro) {}
  ~task() {
    if (coroutine_) {
      destroy();
    }
  }

  task(task &&other) : coroutine_(nullptr) {
    std::swap(coroutine_, other.coroutine_);
  }
  task(const task &) = delete;

  task & operator=(task &&other) {
    if (coroutine_) {
      destroy();
    }

    coroutine_ = other.coroutine_;
    other.coroutine_ = nullptr;
    return *this;
  }
  task & operator=(const task &other) = delete;

  bool valid() const {
      return coroutine_ != nullptr;
  }

  bool done() const {
    ASSERT(coroutine_);
    return coroutine_.done();
  }

  void resume() {
    ASSERT(coroutine_);

    using namespace coro_task_private;

    // Iterates the coroutine call stack using the `link list` tracked
    // by promise_base. Find the deepest coroutine call and resume it.
    const promise_base *depend_on_promise = &coroutine_.promise();
    generic_coroutine_handle depend_on_coroutine = coroutine_;
    generic_coroutine_handle coroutine_to_resume = nullptr;
    do {
      ASSERT(depend_on_promise);
      coroutine_to_resume = depend_on_coroutine;
      depend_on_promise = depend_on_promise->get_depend_on_promise();
      if (!depend_on_promise) {
        break;
      }
      depend_on_coroutine = depend_on_promise->get_promise_coroutine();
      ASSERT(depend_on_coroutine);
    } while (!depend_on_coroutine.done());

    coroutine_to_resume.resume();
  }

  void destroy() {
    coroutine_.destroy();
    coroutine_ = nullptr;
  }

  awaiter operator co_await() const { return awaiter(coroutine_); }

  // Call get_return_value() on task<> for more than one time is undefined
  template<typename U = T>
  typename std::enable_if<not std::is_same<U, void>::value, U>::type 
  get_return_value() const {
    return coroutine_.promise().transfer_return_value();
  }

private:
  coroutine_handle coroutine_;
};

template <> struct task<void>::promise_type : coro_task_private::promise_base {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<void>::promise_type>;

  promise_type() {}
  ~promise_type() {}

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle::from_promise(*this);
    coroutine_handle_addr = coroutine_handle.address();
    return task{coroutine_handle};
  }

  void return_void() {};
};

template <typename T>
struct task<T>::promise_type : coro_task_private::promise_base {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<T>::promise_type>;

  friend struct task<T>::awaiter;

  promise_type() : return_value_(nullptr) {}
  ~promise_type() {
      delete return_value_;
  }

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle::from_promise(*this);
    coroutine_handle_addr = coroutine_handle.address();
    return task{coroutine_handle};
  }

  // XXX: explore if there is anyway to get ride of
  // the new copy constructing.
  void return_value(const T &value) {
    return_value_ = new T(value);
  }

  T transfer_return_value() { return T(std::move(*return_value_)); }

private:
  T *return_value_;
};

/*
 * In the first traversal of nested coroutines, a chain of coroutine_handles 
 * will be created by co_await(more exactly, await_suspend) layer-by-layer.
 * Then on the surface it feels like that the deepest coroutine returns the 
 * control to the top-level corutine directly, but in fact the compiler
 * generates return_to_the_caller() to return the control flow layer-by-layer.
 *
 * In the following traversals, since the top-level coroutine is the direct
 * caller or resumer of the deepest coroutine, the deepest coroutine will
 * actually return the control flow to the top-level coroutine directly.
 *
 * The semantics of co_await <expr> can be translated (roughly) as follows:
 * {
 *   if (not awaiter.await_ready()) {
 *     //if await_suspend returns void
 *     ...
 *     //if await_suspend returns bool
 *     ...
 *     //if await_suspend returns another coroutine_handle
 *     try {
 *       another_coro_handle = awaiter.await_suspend(coroutine_handle);
 *     } catch (...) {
 *       goto resume_point;
 *     }
 *     another_coro_handle.resume();
 *     return_to_the_caller();
 *   }
 *  return awaiter.await_resume();
 * }
 *
 */
template <typename T> struct task<T>::awaiter {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<T>::promise_type>;

  awaiter(coroutine_handle task_coroutine)
      : suspended_task_coroutine_(task_coroutine) {}
  ~awaiter() {}

  awaiter(const awaiter &) = delete;
  awaiter(awaiter && other) : suspended_task_coroutine_(nullptr){
      std::swap(suspended_task_coroutine_, other.suspended_task_coroutine_);
  }

  template <typename awaiting_promise_t>
  auto await_suspend(std::experimental::coroutine_handle<awaiting_promise_t>
                         awaiting_coroutine) noexcept {
    awaiting_coroutine.promise().set_depend_on_promise(
        &suspended_task_coroutine_.promise());
    suspended_task_coroutine_.promise().set_awaiting_promise(
        &awaiting_coroutine.promise());
    return suspended_task_coroutine_;
  }
  constexpr bool await_ready() const noexcept {
    return suspended_task_coroutine_.done();
  }

  template <typename U>
  using non_void_T =
      typename std::enable_if<not std::is_same<U, void>::value, U>::type;
  template <typename U>
  using void_T =
      typename std::enable_if<std::is_same<U, void>::value, void>::type;

  template <typename U = T> non_void_T<U> await_resume() noexcept {
    ASSERT(suspended_task_coroutine_.done());
    return suspended_task_coroutine_.promise().transfer_return_value();
  }

  template <typename U = T> void_T<U> await_resume() noexcept {
    ASSERT(suspended_task_coroutine_.done());
  }

private:
  coroutine_handle suspended_task_coroutine_;
};


} // namespace dia
} // namespace ermia

#ifdef USE_STATIC_COROUTINE
  #define PROMISE(t) ermia::dia::task<t>
  #define RETURN co_return
  #define AWAIT co_await
  #define SUSPEND co_await std::experimental::suspend_always{}

template<typename T>
inline T sync_wait_coro(ermia::dia::task<T> &&coro_task) {
    while(!coro_task.done()) {
        coro_task.resume();
    }

    return coro_task.get_return_value();
}

template<>
inline void sync_wait_coro(ermia::dia::task<void> &&coro_task) {
    while(!coro_task.done()) {
        coro_task.resume();
    }
}

#else
  #define PROMISE(t) t
  #define RETURN return
  #define AWAIT
  #define SUSPEND 

template<typename T>
T sync_wait_coro(const T &t) { return t; }

#endif // USE_STATIC_COROUTINE

#endif

