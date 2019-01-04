#ifndef SM_COROUTINE_H
#define SM_COROUTINE_H
#include <experimental/coroutine>
namespace ermia {
namespace dia {

template<typename T>
struct generator {
  struct promise_type;
  using handle = std::experimental::coroutine_handle<promise_type>;

  struct promise_type {
    T current_value;
    bool go_on;
    auto get_return_object() { return generator{handle::from_promise(*this)}; }
    auto initial_suspend() { go_on = true; return std::experimental::suspend_never{}; }
    auto final_suspend() { return std::experimental::suspend_always{}; }
    void unhandled_exception() { std::terminate(); }
    auto return_value(T value) {
      current_value = value;
      go_on = false;
      return std::experimental::suspend_never{};
    }
  };
  bool advance() { return coro ? (coro.resume(), !coro.done()) : false; }
  T current_value() { return coro.promise().current_value; }
  generator(handle h = nullptr) : coro(h) {}
  ~generator() { if (coro) { coro.destroy();} }

  auto operator co_await(){
    struct awaitable_type {
      std::experimental::coroutine_handle<promise_type> coro;
      bool await_ready() { return coro.done(); }
      void await_suspend(std::experimental::coroutine_handle<> awaiting) { coro.resume(); }
      bool await_resume() { return coro.promise().go_on; }
    };
    return awaitable_type{coro};
  }

private:
  handle coro;
};

}  // namespace dia
}  // namespace ermia
#endif
