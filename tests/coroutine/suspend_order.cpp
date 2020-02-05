#include <gtest/gtest.h>

#include <sm-coroutine.h>

using namespace ermia::dia;

task<void> CoroutineCall_LevelThree(int *counter) {
    (*counter)++;

    co_await std::experimental::suspend_always{};

    (*counter)--;
    co_return;
}

task<void> CoroutineCall_LevelTwo(int *counter) {
    (*counter)++;

    co_await CoroutineCall_LevelThree(counter);

    co_await std::experimental::suspend_always{};

    (*counter)--;
    co_return;
}

task<void> CoroutineCall_LevelOne(int *counter) {
    (*counter)++;

    co_await std::experimental::suspend_always{};

    co_await CoroutineCall_LevelTwo(counter);

    (*counter)--;
    co_return;
}

TEST(CoroutineSuspendOrder, FixedLogic) {
    coro_task_private::memory_pool memory_pool;

    int counter = 0;
    task<void> future_task = CoroutineCall_LevelOne(&counter);
    future_task.start();

    ASSERT_EQ(counter, 1);

    future_task.resume();
    ASSERT_EQ(counter, 3);

    future_task.resume();
    ASSERT_EQ(counter, 2);

    future_task.resume();
    ASSERT_EQ(counter, 0);

    ASSERT_TRUE(future_task.done());
}

