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

add_custom_target(arrow-compute-tests)

arrow_install_all_headers("arrow/compute")

#
# Unit tests
#
if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_COMPUTE_TEST_LINK_LIBS arrow_compute_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_COMPUTE_TEST_LINK_LIBS arrow_compute_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

# Define arrow_compute_core_testing static library for common test files requiring
# only core compute. No extra kernels are required.
#
# It links libraries for ARROW_TEST_LINKAGE. So it uses the same compile
# definitions (e.g. ARROW_STATIC on Windows) as tests.
if(ARROW_TESTING)
  add_library(arrow_compute_core_testing STATIC test_util_internal.cc)
  if(ARROW_TEST_LINKAGE STREQUAL "static")
    target_link_libraries(arrow_compute_core_testing
                          PUBLIC ${ARROW_TEST_STATIC_LINK_LIBS})
  else()
    target_link_libraries(arrow_compute_core_testing
                          PUBLIC ${ARROW_TEST_SHARED_LINK_LIBS})
  endif()
endif()

# Define arrow_compute_testing_main interface library that provides main()
# initializing extra kernels.
#
# test_env.cc is compiled as a source of each test executable. If we use
# a static library, main() in GTest::gmock_main may be used instead.
#
# This doesn't link any Apache Arrow C++ library. So this can be used
# by tests that use different linkage from ARROW_TEST_LINKAGE such as
# Flight SQL tests.
if(ARROW_TESTING AND ARROW_COMPUTE)
  add_library(arrow_compute_testing_main INTERFACE)
  target_sources(arrow_compute_testing_main INTERFACE test_env.cc)
  target_link_libraries(arrow_compute_testing_main INTERFACE ${ARROW_GTEST_GTEST})
endif()

set(ARROW_COMPUTE_TEST_PREFIX "arrow-compute")
set(ARROW_COMPUTE_TEST_LABELS "arrow-compute-tests")
set(ARROW_COMPUTE_TEST_ARGS PREFIX ${ARROW_COMPUTE_TEST_PREFIX} LABELS
                            ${ARROW_COMPUTE_TEST_LABELS})

# This will only add the test if ARROW_COMPUTE is enabled, meaning the full kernel registry is available.
#
# The following kernels are always present in default builds:
#  - array_filter
#  - array_take
#  - cast
#  - dictionary_encode
#  - drop_null
#  - filter
#  - indices_nonzero
#  - take
#  - unique
#  - value_counts
#
# Also see: GH-34388, GH-34615
function(ADD_ARROW_COMPUTE_TEST REL_TEST_NAME)
  if(NOT ARROW_COMPUTE)
    # libarrow_compute is not enabled, skip building test
    return()
  endif()

  set(one_value_args PREFIX)
  set(multi_value_args LABELS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(ARG_PREFIX)
    set(PREFIX ${ARG_PREFIX})
  else()
    set(PREFIX ${ARROW_COMPUTE_TEST_PREFIX})
  endif()

  if(ARG_LABELS)
    set(LABELS ${ARG_LABELS})
  else()
    set(LABELS ${ARROW_COMPUTE_TEST_LABELS})
  endif()

  add_arrow_test(${REL_TEST_NAME}
                 PREFIX
                 ${PREFIX}
                 LABELS
                 ${LABELS}
                 STATIC_LINK_LIBS
                 ${ARROW_COMPUTE_TEST_LINK_LIBS}
                 ${ARG_UNPARSED_ARGUMENTS})
endfunction()

# This function is used to add a custom main to the benchmarks in order
# to initialize the compute kernels registry before running them.
# This is necessary for benchmarks that use compute kernels that are not
# part of libarrow.
# It will also link the compute libraries to the benchmark target.
function(add_arrow_compute_benchmark REL_TEST_NAME)
  if(NOT ARROW_COMPUTE)
    # libarrow_compute is not enabled, skip building benchmark
    return()
  endif()

  set(options)
  set(one_value_args PREFIX)
  set(multi_value_args EXTRA_SOURCES EXTRA_LINK_LIBS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_PREFIX)
    set(PREFIX ${ARG_PREFIX})
  else()
    set(PREFIX "arrow-compute")
  endif()
  set(EXTRA_SOURCES "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/benchmark_main.cc")
  if(ARG_EXTRA_SOURCES)
    list(APPEND EXTRA_SOURCES ${ARG_EXTRA_SOURCES})
  endif()
  if(ARROW_TEST_LINKAGE STREQUAL "static")
    set(EXTRA_LINK_LIBS arrow_compute_static)
  else()
    set(EXTRA_LINK_LIBS arrow_compute_shared)
  endif()
  if(ARG_EXTRA_LINK_LIBS)
    list(APPEND EXTRA_LINK_LIBS ${ARG_EXTRA_LINK_LIBS})
  endif()
  add_benchmark(${REL_TEST_NAME}
                PREFIX
                ${PREFIX}
                LABELS
                "arrow-benchmarks"
                EXTRA_SOURCES
                ${EXTRA_SOURCES}
                EXTRA_LINK_LIBS
                ${EXTRA_LINK_LIBS}
                ${ARG_UNPARSED_ARGUMENTS})
endfunction()

add_arrow_test(internals_test
               ${ARROW_COMPUTE_TEST_ARGS}
               SOURCES
               function_test.cc
               exec_test.cc
               kernel_test.cc
               registry_test.cc
               EXTRA_LINK_LIBS
               arrow_compute_core_testing)

add_arrow_compute_test(expression_test
                       SOURCES
                       expression_test.cc
                       EXTRA_LINK_LIBS
                       arrow_compute_testing_main)

add_arrow_compute_test(row_test
                       SOURCES
                       key_hash_test.cc
                       key_map_test.cc
                       light_array_test.cc
                       row/compare_test.cc
                       row/grouper_test.cc
                       row/row_encoder_internal_test.cc
                       row/row_test.cc
                       util_internal_test.cc
                       EXTRA_LINK_LIBS
                       arrow_compute_core_testing
                       arrow_compute_testing_main)

add_arrow_compute_benchmark(function_benchmark)

add_subdirectory(kernels)

add_subdirectory(row)
