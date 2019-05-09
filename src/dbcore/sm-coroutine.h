#ifndef SM_COROUTINE_H
#define SM_COROUTINE_H
#include <experimental/coroutine>
namespace ermia {
namespace dia {

template <typename T> struct generator {
  struct promise_type;
  using handle = std::experimental::coroutine_handle<promise_type>;

  struct promise_type {
    T current_value;
    auto get_return_object() { return generator{handle::from_promise(*this)}; }
    auto initial_suspend() { return std::experimental::never{}; }
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

} // namespace dia
} // namespace ermia
#endif
