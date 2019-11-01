#pragma once

#include <cstdlib>
#include <cassert>
#include <vector>

#include <gtest/gtest.h>

#include <sm-coroutine.h>

using namespace ermia::dia;

template <typename T>
class CoroutineTestBase : public ::testing::Test {
   public:
    CoroutineTestBase() {}
    ~CoroutineTestBase() {}

    void addTask(task<T> &&task_to_run) {
        future_tasks_.emplace_back(std::move(task_to_run));
    }

    void runTasksUntilComplete() {
        std::vector<std::vector<std::experimental::coroutine_handle<void>>> call_stacks;
        call_stacks.resize(future_tasks_.size());

        for(uint32_t i = 0; i < future_tasks_.size(); i++) {
            future_tasks_[i].set_call_stack(&(call_stacks[i]));
        }

        while (1) {
            bool hasUnfinishedTasks = false;
            for (task<T> &task : future_tasks_) {
                if (!task.done()) {
                    hasUnfinishedTasks = true;
                    task.resume();
                }
            }

            if (!hasUnfinishedTasks) {
                break;
            }
        }
    }

    std::vector<T> getReturnValues() {
        std::vector<T> rets;
        rets.reserve(future_tasks_.size());

        for (task<T> &task : future_tasks_) {
            assert(task.done());
            rets.emplace_back(task.get_return_value());
        }

        return rets;
    }

    virtual void run() = 0;

    virtual void SetUp() override { std::srand(std::time(nullptr)); }
    virtual void TearDown() override {
        for (task<T> &task : future_tasks_) {
            task.destroy();
        }
    }

   private:
    std::vector<task<T>> future_tasks_;
};

