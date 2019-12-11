#ifndef SM_COROUTINE_H
#define SM_COROUTINE_H
#include <mmintrin.h>
#include <xmmintrin.h>
#include <experimental/coroutine>
#include <array>

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

class memory_pool {
private:
  struct mem_node {
    size_t size;
    mem_node *next;
  };

public:
  memory_pool() noexcept {
    ASSERT(!instance_);
    headers_size_ = 0;
    instance_ = this;
  }
  ~memory_pool() {
    for(uint32_t i = 0; i < headers_size_; i++) {
        mem_node *p = headers_[i].next;
        while(p != nullptr) {
          mem_node *q = p->next;
          free(p);
          p = q;
        }
    }

    instance_ = nullptr;
  }

  static memory_pool *instance() {
      ASSERT(instance_);
      return instance_;
  }

  void *allocate_bytes(size_t bytes) {
    // FIXME: reclaim free memory

    mem_node * cur_bytes_header = nullptr;
    for(uint32_t i = 0; i < headers_size_; i++) {
      if (headers_[i].size == bytes) {
        cur_bytes_header = &headers_[i];
        break;
      }
    }

    if (!cur_bytes_header) {
      ASSERT(headers_size_ < kHeadersCapacity);
      headers_[headers_size_].next = nullptr;
      headers_[headers_size_].size = bytes;
      cur_bytes_header = &headers_[headers_size_];
      headers_size_++;
    }

    if (!cur_bytes_header->next) {
      // FIXME: pay attention to alignment,
      // the default alignment in emia is 16, which is fine here.
      // However, it would be better to explicitly say the alignment
      // TODO: use allocate_onnode()
      mem_node *new_node = static_cast<mem_node*>(
        malloc(sizeof(mem_node) + bytes)
      );

      new_node->size = bytes;
      return static_cast<void*>(new_node + 1);
    }

    mem_node * ret_node = cur_bytes_header->next;
    ASSERT(ret_node->size == bytes);
    cur_bytes_header->next = ret_node->next;
    return static_cast<void*>(ret_node + 1);
  }

  void deallocate_bytes(void *p) {
    mem_node * cur_node = static_cast<mem_node*>(p) - 1;
    const size_t bytes = cur_node->size;

    mem_node * cur_bytes_header = nullptr;
    for(uint32_t i = 0; i < headers_size_; i++) {
      if (headers_[i].size == bytes) {
        cur_bytes_header = &headers_[i];
        break;
      }
    }
    ASSERT(cur_bytes_header);

    cur_node->next = cur_bytes_header->next;
    cur_bytes_header->next = cur_node;
  }

private:
  static thread_local memory_pool * instance_;

  // FIXME: dynamic increase
  static constexpr size_t kHeadersCapacity = 20;
  std::array<mem_node, kHeadersCapacity> headers_;
  size_t headers_size_;
};

// task<T>::promise_type inherits promise_base. promise_base keeps track
// of the coroutine call stack.
struct promise_base {
  promise_base()
      : handle_(nullptr), call_stack_(nullptr) {}
  ~promise_base() {}

  promise_base(const promise_base &) = delete;
  promise_base(promise_base &&) = delete;

  auto initial_suspend() { return std::experimental::suspend_always{}; }
  auto final_suspend() {
    // 1. if call_stack_ has size 1, the coroutine pointed by this promise is
    //    the top level coroutine call, so after its final suspend,
    //    we hand the control flow back to non-coroutine execution by return
    //    noop_coroutine().
    //
    // 2. if call_stack_ has size more than 1, we pop it out of the call stack
    //    also we find the caller of this coroutine by looking at the call stack.
    //
    // Note that there is no need to coroutine_handle.destroy() call here,
    // the destroy all happens in the destruction of task<> class.
    ASSERT(call_stack_);
    ASSERT(call_stack_->size() > 0);
    if (call_stack_->size() == 1) {
      return coro_task_private::final_awaiter(
        std::experimental::noop_coroutine());
    }

    // Other cases, pop this coroutine from the call_stack_ and hand over
    // the control flow to its caller.
    call_stack_->pop_back();
    return coro_task_private::final_awaiter(call_stack_->back());
  }
  void unhandled_exception() { std::terminate(); }

  // TODO: Use arena allocator. Probably one arena for
  // each chain of coroutine task.
  // It is very important to use arena to reduce the
  // cache miss in access promise_base * which happens
  // a lot in task<T>.resume();

  void *operator new(std::size_t sz) noexcept {
    return memory_pool::instance()->allocate_bytes(sz);
  }
  void operator delete( void* ptr ) noexcept {
    memory_pool::instance()->deallocate_bytes(ptr);
  }

  void add_to_stack(std::vector<generic_coroutine_handle> * stack) {
    call_stack_ = stack;
    call_stack_->emplace_back(handle_);
  }

  std::vector<generic_coroutine_handle> *get_call_stack() const {
    return call_stack_;
  }

protected:
  generic_coroutine_handle handle_;

  std::vector<generic_coroutine_handle> * call_stack_;
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

  void set_call_stack(
      std::vector<std::experimental::coroutine_handle<void>> *stack) {
    stack->clear();
    coroutine_.promise().add_to_stack(stack);
  }

  void resume() {
    ASSERT(coroutine_);

    using namespace coro_task_private;

    ASSERT(coroutine_.promise().get_call_stack());

    generic_coroutine_handle coroutine_to_resume = coroutine_.promise().get_call_stack()->back();
    coroutine_to_resume.resume();
  }

  void destroy() {
    coroutine_.destroy();
    coroutine_ = nullptr;
  }

  awaiter operator co_await() const {
    _mm_prefetch(coroutine_.promise().get_call_stack(), _MM_HINT_T0);
    return awaiter(coroutine_);
  }

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
    handle_ = coroutine_handle;
    return task{coroutine_handle};
  }

  void return_void() {};
};

template <typename T>
struct task<T>::promise_type : coro_task_private::promise_base {
  using coroutine_handle =
      std::experimental::coroutine_handle<typename task<T>::promise_type>;

  friend struct task<T>::awaiter;

  promise_type() {}
  ~promise_type() {
    reinterpret_cast<T*>(&ret_val_buf_)->~T();
  }

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle::from_promise(*this);
    handle_ = coroutine_handle;
    return task{coroutine_handle};
  }

  // XXX: explore if there is anyway to get ride of
  // the copy constructing.
  void return_value(const T &value) {
    new (&ret_val_buf_) T(value);
  }

  T transfer_return_value() {
      return T(std::move(*reinterpret_cast<T*>(&ret_val_buf_)));
  }

private:
  struct alignas(alignof(T)) T_Buf {
    uint8_t buf[sizeof(T)];
  };
  T_Buf ret_val_buf_;
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

  // suspended_task_coroutine points to coroutine being co_awaited on
  //
  // awaiting_coroutine points to the coroutine running co_await
  // (i.e. it waiting for suspended_task_coroutine to complete first)
  template <typename awaiting_promise_t>
  auto await_suspend(std::experimental::coroutine_handle<awaiting_promise_t>
                         awaiting_coroutine) noexcept {
    std::vector<std::experimental::coroutine_handle<void>> *call_stack =
        awaiting_coroutine.promise().get_call_stack();
    suspended_task_coroutine_.promise().add_to_stack(call_stack);
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
    std::vector<std::experimental::coroutine_handle<void>> call_stack;
    coro_task.set_call_stack(&call_stack);
    while(!coro_task.done()) {
        coro_task.resume();
    }

    return coro_task.get_return_value();
}

template<>
inline void sync_wait_coro(ermia::dia::task<void> &&coro_task) {
    std::vector<std::experimental::coroutine_handle<void>> call_stack;
    coro_task.set_call_stack(&call_stack);
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
inline T sync_wait_coro(const T &t) { return t; }

#endif // USE_STATIC_COROUTINE

#endif

