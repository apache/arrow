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

set(ARROW_SRCS
  array.cc
  buffer.cc
  builder.cc
  builder-adaptive.cc
  builder-binary.cc
  builder-dict.cc
  compare.cc
  memory_pool.cc
  pretty_print.cc
  record_batch.cc
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

  io/buffered.cc
  io/compressed.cc
  io/file.cc
  io/interfaces.cc
  io/memory.cc
  io/readahead.cc

  util/bit-util.cc
  util/compression.cc
  util/cpu-info.cc
  util/decimal.cc
  util/int-util.cc
  util/io-util.cc
  util/logging.cc
  util/key_value_metadata.cc
  util/task-group.cc
  util/thread-pool.cc
  util/utf8.cc
)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE util/io-util.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-unused-macros ")
endif()

if (ARROW_COMPUTE)
  add_subdirectory(compute)
  set(ARROW_SRCS ${ARROW_SRCS}
    compute/context.cc
    compute/kernels/boolean.cc
    compute/kernels/cast.cc
    compute/kernels/hash.cc
    compute/kernels/util-internal.cc
  )
endif()

if (ARROW_GPU)
  # IPC extensions required to build the GPU library
  set(ARROW_IPC ON)
  add_subdirectory(gpu)
endif()

if (ARROW_JEMALLOC AND JEMALLOC_VENDORED)
  add_dependencies(arrow_dependencies jemalloc_static)
endif()

if (ARROW_WITH_BROTLI)
  add_definitions(-DARROW_WITH_BROTLI)
  SET(ARROW_SRCS util/compression_brotli.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_BZ2)
  add_definitions(-DARROW_WITH_BZ2)
  SET(ARROW_SRCS util/compression_bz2.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_LZ4)
  add_definitions(-DARROW_WITH_LZ4)
  SET(ARROW_SRCS util/compression_lz4.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_SNAPPY)
  add_definitions(-DARROW_WITH_SNAPPY)
  SET(ARROW_SRCS util/compression_snappy.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_ZLIB)
  add_definitions(-DARROW_WITH_ZLIB)
  SET(ARROW_SRCS util/compression_zlib.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_ZSTD)
  add_definitions(-DARROW_WITH_ZSTD)
  SET(ARROW_SRCS util/compression_zstd.cc ${ARROW_SRCS})
endif()

if (ARROW_ORC)
  add_subdirectory(adapters/orc)
  SET(ARROW_SRCS adapters/orc/adapter.cc ${ARROW_SRCS})
endif()

if (ARROW_TENSORFLOW)
  add_subdirectory(adapters/tensorflow)
endif()

if (NOT ARROW_BOOST_HEADER_ONLY)
  set(ARROW_SRCS ${ARROW_SRCS}
    io/hdfs.cc
    io/hdfs-internal.cc
  )
endif()

if (ARROW_IPC)
  add_subdirectory(ipc)

  set(ARROW_IPC_SRCS
    ipc/dictionary.cc
    ipc/feather.cc
    ipc/json.cc
    ipc/json-internal.cc
    ipc/message.cc
    ipc/metadata-internal.cc
    ipc/reader.cc
    ipc/writer.cc
  )
  SET(ARROW_SRCS ${ARROW_SRCS}
    ${ARROW_IPC_SRCS})

  add_dependencies(arrow_dependencies metadata_fbs)
endif()

if(NOT APPLE AND NOT MSVC)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_SHARED_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

set(ARROW_ALL_SRCS
  ${ARROW_SRCS})

ADD_ARROW_LIB(arrow
  SOURCES ${ARROW_ALL_SRCS}
  OUTPUTS ARROW_LIBRARIES
  DEPENDENCIES arrow_dependencies
  SHARED_LINK_FLAGS ${ARROW_SHARED_LINK_FLAGS}
  SHARED_LINK_LIBS ${ARROW_LINK_LIBS}
  SHARED_PRIVATE_LINK_LIBS ${ARROW_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS ${ARROW_STATIC_LINK_LIBS})

if (ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_static PUBLIC ARROW_STATIC)
endif()

if (ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  # that depend on gtest
  ADD_ARROW_LIB(arrow_testing
    SOURCES test-util.cc
    OUTPUTS ARROW_TESTING_LIBRARIES
    DEPENDENCIES gtest_static
    SHARED_LINK_LIBS arrow_shared gtest_static
    STATIC_LINK_LIBS arrow_static)

  if (ARROW_BUILD_STATIC AND WIN32)
    target_compile_definitions(arrow_testing_static PUBLIC ARROW_STATIC)
  endif()

  set(ARROW_LIBRARIES
    ${ARROW_LIBRARIES}
    ${ARROW_TESTING_LIBRARIES})
endif()

find_package(Backtrace)

foreach(LIB_TARGET ${ARROW_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
    PRIVATE ARROW_EXPORTING)
  if (Backtrace_FOUND)
    target_compile_definitions(${LIB_TARGET}
      PRIVATE ARROW_WITH_BACKTRACE)
  endif()
endforeach()

# Headers: top level
install(FILES
  allocator.h
  api.h
  array.h
  buffer.h
  builder.h
  compare.h
  memory_pool.h
  pretty_print.h
  record_batch.h
  status.h
  stl.h
  table.h
  table_builder.h
  tensor.h
  type.h
  type_fwd.h
  type_traits.h
  test-util.h
  visitor.h
  visitor_inline.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow")

# pkg-config support
configure_file(arrow.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

#######################################
# Unit tests
#######################################

ADD_ARROW_TEST(allocator-test)
ADD_ARROW_TEST(array-test
  SOURCES array-test.cc array-binary-test.cc array-dict-test.cc
          array-list-test.cc array-struct-test.cc)
ADD_ARROW_TEST(buffer-test)
ADD_ARROW_TEST(memory_pool-test)
ADD_ARROW_TEST(pretty_print-test)
ADD_ARROW_TEST(public-api-test)
ADD_ARROW_TEST(status-test)
ADD_ARROW_TEST(stl-test)
ADD_ARROW_TEST(type-test)
ADD_ARROW_TEST(table-test)
ADD_ARROW_TEST(table_builder-test)
ADD_ARROW_TEST(tensor-test)

ADD_ARROW_BENCHMARK(builder-benchmark)
ADD_ARROW_BENCHMARK(column-benchmark)

add_subdirectory(csv)
add_subdirectory(io)
add_subdirectory(util)
