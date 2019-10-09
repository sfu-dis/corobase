set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})

set(GTEST_INCLUDES ${CMAKE_SOURCE_DIR}/third-party/googletest/googletest/include)
set(TEST_MAIN ${CMAKE_CURRENT_SOURCE_DIR}/test_main.cpp)

add_subdirectory(coroutine)
add_subdirectory(masstree)
