cmake_minimum_required(VERSION 2.8)

project(common)

include(${CMAKE_CURRENT_LIST_DIR}/../cmake/Common.cmake)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

include_directories(thirdparty)

add_library(common STATIC
  event_loop.cc
  common.cc
  io.cc
  thirdparty/ae/ae.c
  thirdparty/sha256.c)

function(define_test test_name library)
  add_executable(${test_name} test/${test_name}.cc ${ARGN})
  add_dependencies(${test_name} hiredis flatbuffers_ep)
  target_link_libraries(${test_name} common ${FLATBUFFERS_STATIC_LIB} ${library})
  target_compile_options(${test_name} PUBLIC "-DPLASMA_TEST -DLOCAL_SCHEDULER_TEST -DCOMMON_TEST -DRAY_COMMON_LOG_LEVEL=4 -DRAY_TIMEOUT=50")
endfunction()

# define_test(common_tests "")
# define_test(db_tests "")
# define_test(io_tests "")
# define_test(task_tests "")
# define_test(redis_tests "")
# define_test(task_table_tests "")
# define_test(object_table_tests "")
