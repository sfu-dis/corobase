set_property(GLOBAL APPEND PROPERTY ALL_ERMIA_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/btree.cpp
)

add_executable(test_btree ${CMAKE_CURRENT_SOURCE_DIR}/test_btree.cpp ${CMAKE_CURRENT_SOURCE_DIR}/btree.cpp)
target_link_libraries(test_btree gtest)
