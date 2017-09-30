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
  compare.cc
  memory_pool.cc
  pretty_print.cc
  status.cc
  table.cc
  tensor.cc
  type.cc
  visitor.cc

  io/file.cc
  io/interfaces.cc
  io/memory.cc

  util/bit-util.cc
  util/compression.cc
  util/cpu-info.cc
  util/decimal.cc
  util/key_value_metadata.cc
)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE io/file.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-unused-macros ")
endif()

if (ARROW_COMPUTE)
  add_subdirectory(compute)
  set(ARROW_SRCS ${ARROW_SRCS}
    compute/cast.cc
    compute/context.cc
  )
endif()

if (ARROW_GPU)
  # IPC extensions required to build the GPU library
  set(ARROW_IPC ON)
  add_subdirectory(gpu)
endif()

if (ARROW_IPC)
  add_subdirectory(ipc)
  add_dependencies(arrow_dependencies metadata_fbs)
endif()

if (ARROW_WITH_BROTLI)
  add_definitions(-DARROW_WITH_BROTLI)
  SET(ARROW_SRCS util/compression_brotli.cc ${ARROW_SRCS})
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

if (NOT ARROW_BOOST_HEADER_ONLY)
  set(ARROW_SRCS ${ARROW_SRCS}
    io/hdfs.cc
    io/hdfs-internal.cc
  )
endif()

if (ARROW_IPC)
  set(ARROW_SRCS ${ARROW_SRCS}
    ipc/dictionary.cc
    ipc/feather.cc
    ipc/json.cc
    ipc/json-internal.cc
    ipc/message.cc
    ipc/metadata-internal.cc
    ipc/reader.cc
    ipc/writer.cc
  )
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
  DEPENDENCIES arrow_dependencies
  SHARED_LINK_FLAGS ${ARROW_SHARED_LINK_FLAGS}
  SHARED_LINK_LIBS ${ARROW_LINK_LIBS}
  SHARED_PRIVATE_LINK_LIBS ${ARROW_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS ${ARROW_STATIC_LINK_LIBS}
  STATIC_PRIVATE_LINK_LIBS ${ARROW_STATIC_PRIVATE_LINK_LIBS}
)

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
  status.h
  table.h
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
ADD_ARROW_TEST(array-test)
ADD_ARROW_TEST(buffer-test)
ADD_ARROW_TEST(memory_pool-test)
ADD_ARROW_TEST(pretty_print-test)
ADD_ARROW_TEST(status-test)
ADD_ARROW_TEST(type-test)
ADD_ARROW_TEST(table-test)
ADD_ARROW_TEST(tensor-test)

ADD_ARROW_BENCHMARK(builder-benchmark)
ADD_ARROW_BENCHMARK(column-benchmark)

add_subdirectory(io)
add_subdirectory(util)
