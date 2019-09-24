#include <memory>

#include "coroutine_test_base.h"

template <typename T>
task<T> returnValueCoroutineChain_LevelTwo(std::unique_ptr<T> *pReturn) {
    co_await std::experimental::suspend_always{};
    *pReturn = std::unique_ptr<T>(new T(std::rand()));
    co_await std::experimental::suspend_always{};
    co_return *(pReturn->get());
}

template <typename T>
task<T> returnValueInCoroutineChain_LevelOne(std::unique_ptr<T> *pReturn) {
    T ret = co_await returnValueCoroutineChain_LevelTwo<T>(pReturn);
    co_return ret;
}

class RAII_Int {
   public:
    RAII_Int(int value) { data_ = new int(value); }
    ~RAII_Int() { delete data_; }

    RAII_Int(const RAII_Int &other) { data_ = new int(other.value()); }
    RAII_Int(RAII_Int &&other) : data_(nullptr) {
        std::swap(data_, other.data_);
    }

    int value() const { return *data_; }

   private:
    int *data_;
};

class CoroReturnDynamicAllocValue : public CoroutineTestBase<RAII_Int> {
   public:
    void run() {
        std::unique_ptr<RAII_Int> returnValueBuf;

        addTask(
            returnValueInCoroutineChain_LevelOne<RAII_Int>(&returnValueBuf));

        runTasksUntilComplete();

        std::vector<RAII_Int> returnValues = getReturnValues();

        ASSERT_EQ(returnValues.size(), 1);
        ASSERT_EQ(returnValues[0].value(), returnValueBuf.get()->value());
    }
};

TEST_F(CoroReturnDynamicAllocValue, DynamicInt) { run(); }

