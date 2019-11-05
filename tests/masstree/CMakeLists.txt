add_subdirectory(utils)

set(ERMIA_INCLUDES
    ${CMAKE_SOURCE_DIR}
)

set(MASSTREE_SRCS
  ${CMAKE_SOURCE_DIR}/masstree/compiler.cc
  ${CMAKE_SOURCE_DIR}/masstree/straccum.cc
  ${CMAKE_SOURCE_DIR}/masstree/str.cc
  ${CMAKE_SOURCE_DIR}/masstree/string.cc
)

set(DBCORE_SRCS
    ${CMAKE_SOURCE_DIR}/dbcore/adler.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/burt-hash.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/dynarray.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/epoch.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/mcs_lock.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/rcu.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/rdma.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/serial.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/size-encode.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-alloc.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-chkpt.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-cmd-log.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-common.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-config.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-coroutine.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-dia.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-exceptions.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-index.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-alloc.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-file.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-offset.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-offset-replay-impl.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-oid-replay-impl.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-recover.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-log-recover-impl.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-object.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-oid-alloc-impl.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-oid.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-rep.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-rep-tcp.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-rep-rdma.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-thread.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/sm-tx-log.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/tcp.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/window-buffer.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/w_rand.cpp
    ${CMAKE_SOURCE_DIR}/dbcore/xid.cpp
)

set(TEST_SRCS
    ${DBCORE_SRCS}
    ${MASSTREE_SRCS}
    ${TEST_MAIN}
    ${TEST_UTILS_SRCS}
    single_threaded.cpp
    concurrent.cpp
)

add_executable(test_masstree ${TEST_SRCS})
target_include_directories(test_masstree PRIVATE ${GTEST_INCLUDES})
target_include_directories(test_masstree PRIVATE ${ERMIA_INCLUDES})
target_link_libraries(test_masstree gtest_main)


set(PERF_SRCS
    ${DBCORE_SRCS}
    ${MASSTREE_SRCS}
    ${TEST_UTILS_SRCS}
    ${PERF_MAIN}
    perf_single_threaded.cpp
)

add_executable(perf_masstree ${PERF_SRCS})
target_include_directories(perf_masstree PRIVATE ${ERMIA_INCLUDES})
target_link_libraries(perf_masstree benchmark_main)
