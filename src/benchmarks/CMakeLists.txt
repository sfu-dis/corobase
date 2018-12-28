add_subdirectory(egen)
add_subdirectory(record)

set_property(GLOBAL APPEND PROPERTY BENCHMARK_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cc
  #${CMAKE_CURRENT_SOURCE_DIR}/tpcc-dora.cc
  #${CMAKE_CURRENT_SOURCE_DIR}/tpce.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-dia.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/bench.cc
)
