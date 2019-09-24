#include "coroutine_test_base.h"

#include <memory>

template <typename T>
task<T> justReturn(T input) {
    co_return input;
}

template <typename T>
task<T> returnWithSuspend(T input) {
    co_await std::experimental::suspend_always{};
    co_return input;
}

template <typename T>
task<T> returnFromChainedCoro_LevelThree(T input) {
    co_await std::experimental::suspend_always{};
    co_return input;
}

template <typename T>
task<T> returnFromChainedCoro_LevelTwo(T input) {
    T res = co_await returnFromChainedCoro_LevelThree(input);
    co_return res;
}

template <typename T>
task<T> returnFromChainedCoro_LevelOne(T input) {
    T res = co_await returnFromChainedCoro_LevelTwo(input);
    co_return res;
}

enum CoroutineCallType {
    JUST_RETURN,
    SUSPEND_AND_RETURN,
    CHAINED_CALL_AND_RETURN,
};

struct ReturnValueCheckerParams {
    struct PrintToStringParamName {
        std::string operator()(
            const ::testing::TestParamInfo<ReturnValueCheckerParams>& info)
            const {
            switch (info.param.callType) {
                case JUST_RETURN:
                    return "JustReturn";
                    break;
                case SUSPEND_AND_RETURN:
                    return "SuspendAndReturn";
                    break;
                case CHAINED_CALL_AND_RETURN:
                    return "ChainedCallAndReturn";
                    break;
                default:
                    return "";
            }
        }
    };

    int taskCount;
    CoroutineCallType callType;
};

class CoroReturnValueChecker_Int
    : public CoroutineTestBase<int>,
      public ::testing::WithParamInterface<ReturnValueCheckerParams> {
   public:
    void run() {
        const int taskCount = GetParam().taskCount;
        std::vector<int> inputs;
        for (uint32_t i = 0; i < taskCount; i++) {
            switch (GetParam().callType) {
                case JUST_RETURN: {
                    inputs.emplace_back(std::rand());
                    addTask(justReturn<int>(inputs.back()));
                    break;
                }
                case SUSPEND_AND_RETURN: {
                    inputs.emplace_back(std::rand());
                    addTask(returnWithSuspend<int>(inputs.back()));
                    break;
                }
                case CHAINED_CALL_AND_RETURN: {
                    inputs.emplace_back(std::rand());
                    addTask(returnFromChainedCoro_LevelOne<int>(inputs.back()));
                    break;
                }
                default:
                    std::terminate();
            }
        }

        runTasksUntilComplete();

        std::vector<int> returnValues = getReturnValues();
        ASSERT_EQ(inputs.size(), returnValues.size());
        for (uint32_t i = 0; i < inputs.size(); i++) {
            ASSERT_EQ(inputs[i], returnValues[i]);
        }
    }
};

TEST_P(CoroReturnValueChecker_Int, Run) { run(); }

INSTANTIATE_TEST_SUITE_P(
    CoroReturnValueChecker_Int, CoroReturnValueChecker_Int,
    ::testing::Values(ReturnValueCheckerParams{5, JUST_RETURN},
                      ReturnValueCheckerParams{5, SUSPEND_AND_RETURN},
                      ReturnValueCheckerParams{5, CHAINED_CALL_AND_RETURN}),
    ReturnValueCheckerParams::PrintToStringParamName());

