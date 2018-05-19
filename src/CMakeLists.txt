# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

project(ermia)
cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(LINK_FLAGS "-libverbs -lnuma -lpthread -lrt -lglog -lgflags")

# Use masstree for index
add_definitions(-DMASSTREE)

set(MASSTREE_CONFIG "--enable-max-key-len=1024 --with-malloc=malloc")

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/masstree/config-debug.h")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/masstree/config-release.h")
endif()

# Assume 64-byte cache line
add_definitions(-DCACHELINE_SIZE=64)

include_directories(dbcore)
include_directories(masstree)
include_directories(third-party/sparsehash/src)

set_property(GLOBAL PROPERTY ALL_ERMIA_SRC "")

add_subdirectory(benchmarks)
add_subdirectory(dbcore)
add_subdirectory(masstree)
add_subdirectory(record)

set_property(GLOBAL APPEND PROPERTY ALL_ERMIA_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/base_txn_btree.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tuple.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn.cc
)

get_property(TMP_ALL_ERMIA_SRC GLOBAL PROPERTY ALL_ERMIA_SRC)

# SI
add_library(ermia_si SHARED ${TMP_ALL_ERMIA_SRC})
add_executable(ermia_SI ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
target_link_libraries(ermia_SI ermia_si ${LINK_FLAGS})

# SI+SSN
add_library(ermia_si_ssn SHARED ${TMP_ALL_ERMIA_SRC})
add_executable(ermia_SI_SSN ${TMP_ALL_ERMIA_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(ermia_SI_SSN PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")
target_link_libraries(ermia_SI_SSN ermia_si_ssn ${LINK_FLAGS})

# SSI
add_executable(ermia_SSI ${TMP_ALL_ERMIA_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
add_library(ermia_ssi SHARED ${TMP_ALL_ERMIA_SRC})
set_target_properties(ermia_SSI PROPERTIES COMPILE_FLAGS "-DSSI")
target_link_libraries(ermia_SSI ermia_ssi ${LINK_FLAGS})

# MVOCC
add_executable(ermia_MVOCC ${TMP_ALL_ERMIA_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
add_library(ermia_mvocc SHARED ${TMP_ALL_ERMIA_SRC})
set_target_properties(ermia_MVOCC PROPERTIES COMPILE_FLAGS "-DMVOCC")
target_link_libraries(ermia_MVOCC ermia_mvocc ${LINK_FLAGS})

# Benchmark scripts
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/run.sh" DESTINATION ${CMAKE_BINARY_DIR})
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/run2.sh" DESTINATION ${CMAKE_BINARY_DIR})
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/run-cluster.sh" DESTINATION ${CMAKE_BINARY_DIR})
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/run-rdma-cluster.sh" DESTINATION ${CMAKE_BINARY_DIR})
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/run-tcp-cluster.sh" DESTINATION ${CMAKE_BINARY_DIR})
