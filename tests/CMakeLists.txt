set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})
set(GTEST_INCLUDES ${CMAKE_SOURCE_DIR}/third-party/googletest/googletest/include)

add_subdirectory(coroutine)
