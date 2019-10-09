set(DB_CORE_INCLUDES
    ${CMAKE_SOURCE_DIR}/dbcore
) 

set(TEST_SRCS
    ${TEST_MAIN}
    coroutine_test_base.h
    return_primary_type.cpp
    return_complex_type.cpp
    resume_order.cpp
    suspend_order.cpp
)

add_executable(test_coroutine ${TEST_SRCS})
target_include_directories(test_coroutine PRIVATE ${GTEST_INCLUDES})
target_include_directories(test_coroutine PRIVATE ${DB_CORE_INCLUDES})
target_link_libraries(test_coroutine gtest_main)
