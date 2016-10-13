add_subdirectory(egen)

set_property(GLOBAL APPEND PROPERTY ALL_ERMIA_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/bench.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/dbtest.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ndb_wrapper.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cc
)

