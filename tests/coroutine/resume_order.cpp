#include <array>
#include <cassert>
#include <iostream>

#include "coroutine_test_base.h"

template <int call_depth>
task<void> ChainedCoroCall(int *callLevelCounter) {
    (*callLevelCounter)++;

    const int currentLevel = *callLevelCounter;

    // std::cout << *callLevelCounter <<std::endl;

    co_await std::experimental::suspend_always{};
    co_await ChainedCoroCall<call_depth - 1>(callLevelCounter);
    co_await std::experimental::suspend_always{};

    assert(*callLevelCounter == currentLevel);

    (*callLevelCounter)--;
    co_return;
}

template <>
task<void> ChainedCoroCall<0>(int *callLevelCounter) {
    (*callLevelCounter)++;

    const int currentLevel = *callLevelCounter;

    co_await std::experimental::suspend_always{};

    assert(*callLevelCounter == currentLevel);

    (*callLevelCounter)--;
    co_return;
}

class CoroResumeOrderTest : public CoroutineTestBase<void> {
   public:
    void run() {
        std::array<int, 5> callLevelCounters = {0};

        addTask(ChainedCoroCall<7>(&callLevelCounters[0]));
        addTask(ChainedCoroCall<9>(&callLevelCounters[1]));
        addTask(ChainedCoroCall<0>(&callLevelCounters[2]));
        addTask(ChainedCoroCall<1>(&callLevelCounters[3]));
        addTask(ChainedCoroCall<3>(&callLevelCounters[4]));

        runTasksUntilComplete();
    }
};

TEST_F(CoroResumeOrderTest, Run) { run(); }

