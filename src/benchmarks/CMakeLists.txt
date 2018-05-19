add_subdirectory(egen)

set_property(GLOBAL APPEND PROPERTY BENCHMARK_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/bench.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ndb_wrapper.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ordered_index.cc
)
