#ifndef SM_COROUTINE_H
#define SM_COROUTINE_H
#include <experimental/coroutine>
#include <vector>
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
    if (awaiting_promise_) {
      awaiting_promise_->clear_depend();
      return coro_task_private::final_awaiter(
          awaiting_promise_->get_promise_coroutine());
    }
    return coro_task_private::final_awaiter(
        std::experimental::noop_coroutine());
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

private:
  coroutine_handle coroutine_;
};

template <> struct task<void>::promise_type : coro_task_private::promise_base {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<void>::promise_type>;

  friend struct task<void>::awaiter;

  promise_type() {}
  ~promise_type() {}

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle::from_promise(*this);
    coroutine_handle_addr = coroutine_handle.address();
    return task{coroutine_handle};
  }

  void return_void(){};
};

template <typename T>
struct task<T>::promise_type : coro_task_private::promise_base {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<T>::promise_type>;

  friend struct task<T>::awaiter;

  promise_type() : return_value_(nullptr) {}
  ~promise_type() {}

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle::from_promise(*this);
    coroutine_handle_addr = coroutine_handle.address();
    return task{coroutine_handle};
  }

  void return_value(T &value) {
    // XXX: not sure if it's really safe.
    return_value_ = &value;
  }

private:
  T &&transfer_return_value() { return std::move(*return_value_); }
  T *return_value_;
};

template <typename T> struct task<T>::awaiter {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<T>::promise_type>;

  awaiter(coroutine_handle task_coroutine)
      : suspended_task_coroutine_(task_coroutine) {}
  ~awaiter() {}

  awaiter(const awaiter &) = delete;
  awaiter(awaiter &&) = delete;

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

  template <typename U = T> constexpr non_void_T<U> &&await_resume() noexcept {
    ASSERT(suspended_task_coroutine_.done());
    return std::move(
        suspended_task_coroutine_.promise().transfer_return_value());
  }

  template <typename U = T> constexpr void_T<U> await_resume() noexcept {
    ASSERT(suspended_task_coroutine_.done());
  }

private:
  const coroutine_handle suspended_task_coroutine_;
};

} // namespace dia
} // namespace ermia
#endif
