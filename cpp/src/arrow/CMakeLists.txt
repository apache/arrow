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

add_custom_target(arrow-all)
add_custom_target(arrow)
add_custom_target(arrow-benchmarks)
add_custom_target(arrow-tests)
add_dependencies(arrow-all arrow arrow-tests arrow-benchmarks)

# Adding unit tests part of the "arrow" portion of the test suite
function(ADD_ARROW_TEST REL_TEST_NAME)
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
    set(PREFIX "arrow")
  endif()

  if(ARG_LABELS)
    set(LABELS ${ARG_LABELS})
  else()
    set(LABELS "arrow-tests")
  endif()

  add_test_case(${REL_TEST_NAME}
                PREFIX
                ${PREFIX}
                LABELS
                ${LABELS}
                ${ARG_UNPARSED_ARGUMENTS})
endfunction()

function(ADD_ARROW_BENCHMARK REL_TEST_NAME)
  set(options)
  set(one_value_args PREFIX)
  set(multi_value_args)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_PREFIX)
    set(PREFIX ${ARG_PREFIX})
  else()
    set(PREFIX "arrow")
  endif()
  add_benchmark(${REL_TEST_NAME}
                PREFIX
                ${PREFIX}
                LABELS
                "arrow-benchmarks"
                ${ARG_UNPARSED_ARGUMENTS})
endfunction()

set(ARROW_SRCS
    array.cc
    builder.cc
    array/builder_adaptive.cc
    array/builder_base.cc
    array/builder_binary.cc
    array/builder_decimal.cc
    array/builder_dict.cc
    array/builder_nested.cc
    array/builder_primitive.cc
    array/builder_union.cc
    buffer.cc
    compare.cc
    memory_pool.cc
    pretty_print.cc
    record_batch.cc
    scalar.cc
    sparse_tensor.cc
    status.cc
    table.cc
    table_builder.cc
    tensor.cc
    type.cc
    visitor.cc
    csv/converter.cc
    csv/chunker.cc
    csv/column-builder.cc
    csv/options.cc
    csv/parser.cc
    csv/reader.cc
    json/options.cc
    json/chunker.cc
    json/parser.cc
    json/reader.cc
    io/buffered.cc
    io/compressed.cc
    io/file.cc
    io/interfaces.cc
    io/memory.cc
    io/readahead.cc
    testing/util.cc
    util/basic_decimal.cc
    util/bit-util.cc
    util/compression.cc
    util/cpu-info.cc
    util/decimal.cc
    util/int-util.cc
    util/io-util.cc
    util/logging.cc
    util/key_value_metadata.cc
    util/memory.cc
    util/task-group.cc
    util/thread-pool.cc
    util/trie.cc
    util/utf8.cc
    vendored/datetime/tz.cpp)

if("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE util/io-util.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-unused-macros ")
endif()

if(ARROW_COMPUTE)
  add_subdirectory(compute)
  set(ARROW_SRCS
      ${ARROW_SRCS}
      compute/context.cc
      compute/kernels/aggregate.cc
      compute/kernels/boolean.cc
      compute/kernels/cast.cc
      compute/kernels/hash.cc
      compute/kernels/sum.cc
      compute/kernels/util-internal.cc)
endif()

if(ARROW_CUDA)
  # IPC extensions required to build the CUDA library
  set(ARROW_IPC ON)
  add_subdirectory(gpu)
endif()

if(ARROW_JEMALLOC AND JEMALLOC_VENDORED)
  add_dependencies(arrow_dependencies jemalloc_static)
endif()

if(ARROW_WITH_BROTLI)
  add_definitions(-DARROW_WITH_BROTLI)
  set(ARROW_SRCS util/compression_brotli.cc ${ARROW_SRCS})
endif()

if(ARROW_WITH_BZ2)
  add_definitions(-DARROW_WITH_BZ2)
  set(ARROW_SRCS util/compression_bz2.cc ${ARROW_SRCS})
endif()

if(ARROW_WITH_LZ4)
  add_definitions(-DARROW_WITH_LZ4)
  set(ARROW_SRCS util/compression_lz4.cc ${ARROW_SRCS})
endif()

if(ARROW_WITH_SNAPPY)
  add_definitions(-DARROW_WITH_SNAPPY)
  set(ARROW_SRCS util/compression_snappy.cc ${ARROW_SRCS})
endif()

if(ARROW_WITH_ZLIB)
  add_definitions(-DARROW_WITH_ZLIB)
  set(ARROW_SRCS util/compression_zlib.cc ${ARROW_SRCS})
endif()

if(ARROW_WITH_ZSTD)
  add_definitions(-DARROW_WITH_ZSTD)
  set(ARROW_SRCS util/compression_zstd.cc ${ARROW_SRCS})
endif()

if(ARROW_ORC)
  add_subdirectory(adapters/orc)
  set(ARROW_SRCS adapters/orc/adapter.cc ${ARROW_SRCS})
endif()

if(ARROW_TENSORFLOW)
  add_subdirectory(adapters/tensorflow)
endif()

if(NOT ARROW_BOOST_HEADER_ONLY)
  set(ARROW_SRCS ${ARROW_SRCS} io/hdfs.cc io/hdfs-internal.cc)
endif()

if(ARROW_IPC)
  add_subdirectory(ipc)

  set(ARROW_IPC_SRCS
      ipc/dictionary.cc
      ipc/feather.cc
      ipc/json-integration.cc
      ipc/json-internal.cc
      ipc/json-simple.cc
      ipc/message.cc
      ipc/metadata-internal.cc
      ipc/reader.cc
      ipc/writer.cc)
  set(ARROW_SRCS ${ARROW_SRCS} ${ARROW_IPC_SRCS})

  add_dependencies(arrow_dependencies metadata_fbs)
endif()

if(NOT APPLE AND NOT MSVC)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_SHARED_LINK_FLAGS
      "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

set(ARROW_ALL_SRCS ${ARROW_SRCS})

add_arrow_lib(arrow
              SOURCES
              ${ARROW_ALL_SRCS}
              OUTPUTS
              ARROW_LIBRARIES
              DEPENDENCIES
              arrow_dependencies
              SHARED_LINK_FLAGS
              ${ARROW_SHARED_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${ARROW_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_SHARED_PRIVATE_LINK_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_STATIC_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_SHARED_INSTALL_INTERFACE_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_STATIC_INSTALL_INTERFACE_LIBS})

add_dependencies(arrow ${ARROW_LIBRARIES})

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_static PUBLIC ARROW_STATIC)
endif()

if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  # that depend on gtest
  add_arrow_lib(arrow_testing
                SOURCES
                testing/gtest_util.cc
                testing/random.cc
                OUTPUTS
                ARROW_TESTING_LIBRARIES
                DEPENDENCIES
                arrow_test_dependencies
                SHARED_LINK_LIBS
                arrow_shared
                ${GTEST_LIBRARY}
                STATIC_LINK_LIBS
                arrow_static)

  if(ARROW_BUILD_STATIC AND WIN32)
    target_compile_definitions(arrow_testing_static PUBLIC ARROW_STATIC)
  endif()

  set(ARROW_LIBRARIES ${ARROW_LIBRARIES} ${ARROW_TESTING_LIBRARIES})
endif()

find_package(Backtrace)

foreach(LIB_TARGET ${ARROW_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_EXPORTING)
  if(Backtrace_FOUND AND ARROW_WITH_BACKTRACE)
    target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_WITH_BACKTRACE)
  endif()
endforeach()

# Headers: top level
arrow_install_all_headers("arrow")

# pkg-config support
arrow_add_pkg_config("arrow")

#
# Unit tests
#

add_arrow_test(allocator-test)
add_arrow_test(array-test
               SOURCES
               array-test.cc
               array-binary-test.cc
               array-dict-test.cc
               array-list-test.cc
               array-struct-test.cc)
add_arrow_test(buffer-test)
add_arrow_test(memory_pool-test)
add_arrow_test(pretty_print-test)
add_arrow_test(public-api-test)
add_arrow_test(scalar-test)
add_arrow_test(status-test)
add_arrow_test(stl-test)
add_arrow_test(type-test)
add_arrow_test(table-test)
add_arrow_test(table_builder-test)
add_arrow_test(tensor-test)
add_arrow_test(sparse_tensor-test)

add_arrow_benchmark(builder-benchmark)
add_arrow_benchmark(column-benchmark)

add_subdirectory(array)
add_subdirectory(csv)
add_subdirectory(json)
add_subdirectory(io)
add_subdirectory(util)
add_subdirectory(vendored)

if(ARROW_FLIGHT)
  add_subdirectory(flight)
endif()

if(ARROW_PYTHON)
  add_subdirectory(python)
endif()

if(ARROW_HIVESERVER2)
  add_subdirectory(dbi/hiveserver2)
endif()
