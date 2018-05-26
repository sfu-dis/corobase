# Generate burt-hash.cpp
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.cpp")
  set(BURT_HASH_GEN python2 ${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.py)
  execute_process(COMMAND ${BURT_HASH_GEN} ERROR_VARIABLE out OUTPUT_VARIABLE out)
  file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.cpp "${out}")
endif()

set(DBCORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/adler.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/epoch.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mcs_lock.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/rcu.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/rdma.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/serial.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/size-encode.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-chkpt.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-cmd-log.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-common.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-exceptions.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-index.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-file.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-offset.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-offset-replay-impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-oid-replay-impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-recover.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-log-recover-impl.cpp
  #${CMAKE_CURRENT_SOURCE_DIR}/sm-oid-alloc.cpp     # belongs to test cases only
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-object.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-oid-alloc-impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-oid.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-rep.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-rep-tcp.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-rep-rdma.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-thread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-tx-log.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tcp.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/window-buffer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_rand.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/xid.cpp
)

set_property(GLOBAL APPEND PROPERTY ALL_ERMIA_SRC ${DBCORE_SRC})

# TODO(tzwang): generate executables for test cases below
#${CMAKE_CURRENT_SOURCE_DIR}/test-adler.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-dynarray.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-epoch.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-rcu.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sc-hash.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-size-encode.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sm-log-alloc.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sm-log.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sm-log-file.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sm-log-offset.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sm-oid-alloc-impl.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-sm-oid.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-window-buffer.cpp
#${CMAKE_CURRENT_SOURCE_DIR}/test-xid.cpp

