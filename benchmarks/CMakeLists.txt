add_subdirectory(egen)
add_subdirectory(record)

set_property(GLOBAL APPEND PROPERTY ALL_ERMIA_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cc
#  ${CMAKE_CURRENT_SOURCE_DIR}/tpce.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-config.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-cs-simple.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-cs-advance.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/bench.cc
)
