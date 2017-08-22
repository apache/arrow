# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

function(ADD_ARROW_CUDA_TEST REL_TEST_NAME)
  set(options)
  set(single_value_args)
  set(multi_value_args STATIC_LINK_LIBS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NO_TESTS OR NOT ARROW_BUILD_STATIC)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    cuda_add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")

    if (ARG_STATIC_LINK_LIBS)
      # Customize link libraries
      target_link_libraries(${TEST_NAME} ${ARG_STATIC_LINK_LIBS})
    else()
      target_link_libraries(${TEST_NAME} ${ARROW_TEST_LINK_LIBS})
    endif()
    add_dependencies(unittest ${TEST_NAME})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  if (ARROW_TEST_MEMCHECK)
    SET_PROPERTY(TARGET ${TEST_NAME}
      APPEND_STRING PROPERTY
      COMPILE_FLAGS " -DARROW_VALGRIND")
    add_test(${TEST_NAME}
      bash -c "cd ${EXECUTABLE_OUTPUT_PATH}; valgrind --tool=memcheck --leak-check=full --leak-check-heuristics=stdstring --error-exitcode=1 ${TEST_PATH}")
  elseif(MSVC)
    add_test(${TEST_NAME} ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_PATH})
  endif()
  set_tests_properties(${TEST_NAME} PROPERTIES LABELS "unittest")
endfunction()

#######################################
# arrow_gpu
#######################################

if (DEFINED ENV{CUDA_HOME})
  set(CUDA_TOOLKIT_ROOT_DIR "$ENV{CUDA_HOME}")
endif()

find_package(CUDA REQUIRED)
include_directories(SYSTEM ${CUDA_INCLUDE_DIRS})

set(ARROW_GPU_SRCS
  cuda_memory.cc
)

set(ARROW_GPU_SHARED_LINK_LIBS
  arrow_shared
)

add_library(arrow_gpu_objlib OBJECT
  ${ARROW_GPU_SRCS}
)
set_property(TARGET arrow_gpu_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)

if (ARROW_BUILD_SHARED)
  cuda_add_library(arrow_gpu_shared SHARED $<TARGET_OBJECTS:arrow_gpu_objlib>)
  install(TARGETS arrow_gpu_shared
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

if (ARROW_BUILD_STATIC)
  add_library(arrow_gpu_static STATIC $<TARGET_OBJECTS:arrow_gpu_objlib>)
  install(TARGETS arrow_gpu_static
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

install(FILES
  cuda_common.h
  cuda_memory.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/gpu")

# pkg-config support
configure_file(arrow-gpu.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-gpu.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-gpu.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

set(ARROW_GPU_TEST_LINK_LIBS
  arrow_gpu_shared
  ${ARROW_TEST_LINK_LIBS})

if (ARROW_BUILD_TESTS)
  ADD_ARROW_CUDA_TEST(cuda-test
    STATIC_LINK_LIBS ${ARROW_GPU_TEST_LINK_LIBS})
endif()

if (ARROW_BUILD_BENCHMARKS)
  cuda_add_executable(cuda-benchmark cuda-benchmark.cc)
  target_link_libraries(cuda-benchmark
    arrow_gpu_shared
    gtest
    ${ARROW_BENCHMARK_LINK_LIBS})
endif()
