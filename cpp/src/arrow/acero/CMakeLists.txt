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

add_custom_target(arrow_acero)

arrow_install_all_headers("arrow/acero")

set(ARROW_ACERO_SRCS
    accumulation_queue.cc
    scalar_aggregate_node.cc
    groupby_aggregate_node.cc
    aggregate_internal.cc
    asof_join_node.cc
    bloom_filter.cc
    exec_plan.cc
    fetch_node.cc
    filter_node.cc
    hash_join.cc
    hash_join_dict.cc
    hash_join_node.cc
    map_node.cc
    options.cc
    order_by_node.cc
    order_by_impl.cc
    partition_util.cc
    pivot_longer_node.cc
    project_node.cc
    query_context.cc
    sink_node.cc
    sorted_merge_node.cc
    source_node.cc
    swiss_join.cc
    task_util.cc
    time_series_util.cc
    tpch_node.cc
    union_node.cc
    util.cc)

append_runtime_avx2_src(ARROW_ACERO_SRCS bloom_filter_avx2.cc)
append_runtime_avx2_src(ARROW_ACERO_SRCS swiss_join_avx2.cc)

set(ARROW_ACERO_SHARED_LINK_LIBS)
set(ARROW_ACERO_SHARED_PRIVATE_LINK_LIBS)
set(ARROW_ACERO_STATIC_LINK_LIBS)
set(ARROW_ACERO_STATIC_INSTALL_INTERFACE_LIBS)
set(ARROW_ACERO_SHARED_INSTALL_INTERFACE_LIBS)

if(ARROW_WITH_OPENTELEMETRY)
  list(APPEND ARROW_ACERO_SHARED_PRIVATE_LINK_LIBS ${ARROW_OPENTELEMETRY_LIBS})
  list(APPEND ARROW_ACERO_STATIC_LINK_LIBS ${ARROW_OPENTELEMETRY_LIBS})
endif()

list(APPEND ARROW_ACERO_STATIC_INSTALL_INTERFACE_LIBS Arrow::arrow_static)
list(APPEND ARROW_ACERO_SHARED_INSTALL_INTERFACE_LIBS Arrow::arrow_shared)
list(APPEND ARROW_ACERO_STATIC_LINK_LIBS arrow_static)
list(APPEND ARROW_ACERO_SHARED_LINK_LIBS arrow_shared)

add_arrow_lib(arrow_acero
              CMAKE_PACKAGE_NAME
              ArrowAcero
              PKG_CONFIG_NAME
              arrow-acero
              OUTPUTS
              ARROW_ACERO_LIBRARIES
              SOURCES
              ${ARROW_ACERO_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/acero/pch.h>"
              SHARED_LINK_LIBS
              ${ARROW_ACERO_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_ACERO_SHARED_PRIVATE_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_ACERO_SHARED_INSTALL_INTERFACE_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_ACERO_STATIC_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_ACERO_STATIC_INSTALL_INTERFACE_LIBS})

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_acero_static PUBLIC ARROW_ACERO_STATIC)
endif()

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_ACERO_TEST_LINK_LIBS arrow_acero_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_ACERO_TEST_LINK_LIBS arrow_acero_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

foreach(LIB_TARGET ${ARROW_ACERO_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_ACERO_EXPORTING)
endforeach()

# Define arrow_acero_testing object library for common test files
if(ARROW_TESTING)
  # test_nodes.cc isn't used by all tests but link to it for simple
  # CMakeLists.txt.
  add_library(arrow_acero_testing OBJECT test_nodes.cc test_util_internal.cc)
  # Even though this is still just an object library we still need to "link" our
  # dependencies so that include paths are configured correctly
  target_link_libraries(arrow_acero_testing PRIVATE ${ARROW_ACERO_TEST_LINK_LIBS})
  # Only for test_nodes.cc.
  if(ARROW_WITH_OPENTELEMETRY)
    target_link_libraries(arrow_acero_testing PRIVATE ${ARROW_OPENTELEMETRY_LIBS})
  endif()
  list(APPEND ARROW_ACERO_TEST_LINK_LIBS arrow_acero_testing)
endif()
# Only for hash_aggregate_test.cc.
if(ARROW_USE_BOOST)
  list(APPEND ARROW_ACERO_TEST_LINK_LIBS Boost::headers)
endif()

# Adding unit tests part of the "dataset" portion of the test suite
function(add_arrow_acero_test REL_TEST_NAME)
  set(options)
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
    set(PREFIX "arrow-acero")
  endif()

  if(ARG_LABELS)
    set(LABELS ${ARG_LABELS})
  else()
    set(LABELS "arrow_acero")
  endif()

  add_arrow_test(${REL_TEST_NAME}
                 EXTRA_LINK_LIBS
                 ${ARROW_ACERO_TEST_LINK_LIBS}
                 PREFIX
                 ${PREFIX}
                 LABELS
                 ${LABELS}
                 ${ARG_UNPARSED_ARGUMENTS})
endfunction()

add_arrow_acero_test(plan_test SOURCES plan_test.cc test_nodes_test.cc)
add_arrow_acero_test(source_node_test SOURCES source_node_test.cc)
add_arrow_acero_test(fetch_node_test SOURCES fetch_node_test.cc)
add_arrow_acero_test(order_by_node_test SOURCES order_by_node_test.cc)
add_arrow_acero_test(hash_join_node_test SOURCES hash_join_node_test.cc
                     bloom_filter_test.cc)
add_arrow_acero_test(pivot_longer_node_test SOURCES pivot_longer_node_test.cc)

add_arrow_acero_test(asof_join_node_test SOURCES asof_join_node_test.cc)
add_arrow_acero_test(sorted_merge_node_test SOURCES sorted_merge_node_test.cc)

add_arrow_acero_test(tpch_node_test SOURCES tpch_node_test.cc)
add_arrow_acero_test(union_node_test SOURCES union_node_test.cc)
add_arrow_acero_test(aggregate_node_test SOURCES aggregate_node_test.cc)
add_arrow_acero_test(util_test SOURCES util_test.cc task_util_test.cc)
add_arrow_acero_test(hash_aggregate_test SOURCES hash_aggregate_test.cc)

if(ARROW_BUILD_BENCHMARKS)
  function(add_arrow_acero_benchmark REL_BENCHMARK_NAME)
    set(options)
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
      set(PREFIX "arrow-acero")
    endif()

    if(ARG_LABELS)
      set(LABELS ${ARG_LABELS})
    else()
      set(LABELS "arrow_acero")
    endif()

    add_arrow_benchmark(${REL_BENCHMARK_NAME}
                        EXTRA_LINK_LIBS
                        ${ARROW_ACERO_TEST_LINK_LIBS}
                        PREFIX
                        ${PREFIX}
                        LABELS
                        ${LABELS}
                        ${ARG_UNPARSED_ARGUMENTS})
  endfunction()

  add_arrow_acero_benchmark(expression_benchmark SOURCES expression_benchmark.cc)

  add_arrow_acero_benchmark(filter_benchmark SOURCES benchmark_util.cc
                            filter_benchmark.cc)

  add_arrow_acero_benchmark(project_benchmark SOURCES benchmark_util.cc
                            project_benchmark.cc)

  add_arrow_acero_benchmark(asof_join_benchmark SOURCES asof_join_benchmark.cc)

  add_arrow_acero_benchmark(tpch_benchmark SOURCES tpch_benchmark.cc)

  add_arrow_acero_benchmark(aggregate_benchmark SOURCES aggregate_benchmark.cc)

  if(ARROW_BUILD_OPENMP_BENCHMARKS)
    find_package(OpenMP REQUIRED)
    add_arrow_acero_benchmark(hash_join_benchmark
                              EXTRA_LINK_LIBS
                              OpenMP::OpenMP_CXX
                              SOURCES
                              hash_join_benchmark.cc)
    if(MSVC)
      target_compile_options(arrow-compute-hash-join-benchmark
                             PRIVATE "-openmp:experimental -openmp:llvm")
    endif()
  endif()

  if(ARROW_BUILD_STATIC)
    target_link_libraries(arrow-acero-expression-benchmark PUBLIC arrow_acero_static)
    target_link_libraries(arrow-acero-filter-benchmark PUBLIC arrow_acero_static)
    target_link_libraries(arrow-acero-project-benchmark PUBLIC arrow_acero_static)
    target_link_libraries(arrow-acero-asof-join-benchmark PUBLIC arrow_acero_static)
    target_link_libraries(arrow-acero-tpch-benchmark PUBLIC arrow_acero_static)
    if(ARROW_BUILD_OPENMP_BENCHMARKS)
      target_link_libraries(arrow-acero-hash-join-benchmark PUBLIC arrow_acero_static)
    endif()
  else()
    target_link_libraries(arrow-acero-expression-benchmark PUBLIC arrow_acero_shared)
    target_link_libraries(arrow-acero-filter-benchmark PUBLIC arrow_acero_shared)
    target_link_libraries(arrow-acero-project-benchmark PUBLIC arrow_acero_shared)
    target_link_libraries(arrow-acero-asof-join-benchmark PUBLIC arrow_acero_shared)
    target_link_libraries(arrow-acero-tpch-benchmark PUBLIC arrow_acero_shared)
    if(ARROW_BUILD_OPENMP_BENCHMARKS)
      target_link_libraries(arrow-acero-hash-join-benchmark PUBLIC arrow_acero_shared)
    endif()
  endif()
endif()
