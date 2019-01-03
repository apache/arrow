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

add_custom_target(parquet-all)
add_custom_target(parquet)
add_custom_target(parquet-benchmarks)
add_custom_target(parquet-tests)
add_dependencies(parquet-all parquet parquet-tests parquet-benchmarks)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/.parquetcppversion" PARQUET_VERSION)
string(REPLACE "\n" "" PARQUET_VERSION "${PARQUET_VERSION}")
string(REGEX MATCH "^([0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?)" VERSION ${PARQUET_VERSION})
if(NOT VERSION)
  message(FATAL_ERROR "invalid .parquetcppversion")
endif()

function(ADD_PARQUET_TEST REL_TEST_NAME)
  set(options USE_STATIC_LINKING)
  set(one_value_args)
  set(multi_value_args EXTRA_DEPENDENCIES LABELS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  set(TEST_ARGUMENTS
    PREFIX "parquet"
    LABELS "parquet-tests")

  # By default we prefer shared linking with libparquet, as it's faster
  # and uses less disk space, but in some cases we need to force static
  # linking (see rationale below).
  if (ARG_USE_STATIC_LINKING)
    ADD_TEST_CASE(${REL_TEST_NAME}
      STATIC_LINK_LIBS ${PARQUET_STATIC_TEST_LINK_LIBS}
      ${TEST_ARGUMENTS})
  else()
    ADD_TEST_CASE(${REL_TEST_NAME}
      STATIC_LINK_LIBS ${PARQUET_SHARED_TEST_LINK_LIBS}
      ${TEST_ARGUMENTS})
  endif()
endfunction()

# ----------------------------------------------------------------------
# Link libraries setup

# TODO(wesm): Handling of ABI/SO version

if (ARROW_BUILD_STATIC)
  set(PARQUET_STATIC_LINK_LIBS
    arrow_static)
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS
    arrow_testing_static
    arrow_static)
else()
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS
    arrow_testing_shared
    arrow_shared)
endif()

set(PARQUET_BOOST_LINK_LIBS
  ${BOOST_REGEX_LIBRARY})

if(MSVC)
  set(PARQUET_BOOST_LINK_LIBS ${PARQUET_BOOST_LINK_LIBS}
    ${BOOST_SYSTEM_LIBRARY})
endif()

set(PARQUET_MIN_TEST_LIBS
  gtest_main_static
  gtest_static)

if (APPLE)
  set(PARQUET_MIN_TEST_LIBS
    ${PARQUET_MIN_TEST_LIBS}
    ${CMAKE_DL_LIBS})
elseif(NOT MSVC)
  set(PARQUET_MIN_TEST_LIBS
    ${PARQUET_MIN_TEST_LIBS}
    pthread
    ${CMAKE_DL_LIBS})
endif()

set(PARQUET_SHARED_TEST_LINK_LIBS
  arrow_testing_shared
  ${PARQUET_MIN_TEST_LIBS}
  parquet_shared
  thriftstatic)

set(PARQUET_STATIC_TEST_LINK_LIBS
  ${PARQUET_MIN_TEST_LIBS}
  ${ARROW_LIBRARIES_FOR_STATIC_TESTS}
  parquet_static)

set(PARQUET_BENCHMARK_LINK_LIBRARIES
  arrow_benchmark_main
  parquet_shared)

############################################################
# Generated Thrift sources

if (NOT MSVC)
  set_source_files_properties(src/parquet/parquet_types.cpp PROPERTIES
    COMPILE_FLAGS -Wno-unused-variable)
endif()

# List of thrift output targets
set(THRIFT_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(THRIFT_OUTPUT_FILES "${THRIFT_OUTPUT_DIR}/parquet_types.cpp")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${THRIFT_OUTPUT_DIR}/parquet_types.h")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${THRIFT_OUTPUT_DIR}/parquet_constants.cpp")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${THRIFT_OUTPUT_DIR}/parquet_constants.h")

set_source_files_properties(${THRIFT_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(ABS_PARQUET_THRIFT parquet.thrift ABSOLUTE)

add_custom_command(
  OUTPUT ${THRIFT_OUTPUT_FILES}
  COMMAND ${THRIFT_COMPILER} --gen cpp -out ${THRIFT_OUTPUT_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/parquet.thrift
  DEPENDS ${ABS_PARQUET_THRIFT} thriftstatic
  COMMENT "Running thrift compiler on parquet.thrift"
  VERBATIM
)

############################################################
# Library config

set(PARQUET_SRCS
  arrow/reader.cc
  arrow/record_reader.cc
  arrow/schema.cc
  arrow/writer.cc
  bloom_filter.cc
  column_reader.cc
  column_scanner.cc
  column_writer.cc
  file_reader.cc
  file_writer.cc
  metadata.cc
  murmur3.cc
  parquet_constants.cpp
  parquet_types.cpp
  printer.cc
  schema.cc
  statistics.cc
  types.cc
  util/comparison.cc
  util/memory.cc
)

# Ensure that thrift compilation is done before using its generated headers
# in parquet code.
add_custom_target(parquet-thrift-deps ALL
  DEPENDS ${THRIFT_OUTPUT_FILES})
set(PARQUET_DEPENDENCIES ${PARQUET_DEPENDENCIES} parquet-thrift-deps)

if (NOT PARQUET_MINIMAL_DEPENDENCY)
  set(PARQUET_SHARED_LINK_LIBS
    arrow_shared)

# These are libraries that we will link privately with parquet_shared (as they
# do not need to be linked transitively by other linkers)
  set(PARQUET_SHARED_PRIVATE_LINK_LIBS
    ${PARQUET_BOOST_LINK_LIBS}
    thriftstatic
  )

# Link publicly with # parquet_static (because internal users need to
# transitively link all dependencies)
  set(PARQUET_STATIC_LINK_LIBS
    ${PARQUET_STATIC_LINK_LIBS}
    ${PARQUET_BOOST_LINK_LIBS}
    thriftstatic
  )

# Although we don't link parquet_objlib against anything, we need it to depend
# on these libs as we may generate their headers via ExternalProject_Add
set(PARQUET_DEPENDENCIES ${PARQUET_DEPENDENCIES}
  ${PARQUET_SHARED_LINK_LIBS}
  ${PARQUET_SHARED_PRIVATE_LINK_LIBS}
  ${PARQUET_STATIC_LINK_LIBS})
endif()

if(NOT APPLE AND NOT MSVC)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(SHARED_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/symbols.map")
endif()

# Because of PARQUET-1420 (Thrift-generated symbols not exported in DLL),
# parquet tests must be linked against the static parquet lib.
if (NO_TESTS)
  set(PARQUET_LIB_OPTIONS)
else ()
  set(PARQUET_LIB_OPTIONS "BUILD_STATIC")
endif()

ADD_ARROW_LIB(parquet
  ${PARQUET_LIB_OPTIONS}
  SOURCES ${PARQUET_SRCS}
  OUTPUTS PARQUET_LIBRARIES
  DEPENDENCIES ${PARQUET_DEPENDENCIES}
  SHARED_LINK_FLAGS ${PARQUET_SHARED_LINK_FLAGS}
  SHARED_LINK_LIBS ${PARQUET_SHARED_LINK_LIBS}
  SHARED_PRIVATE_LINK_LIBS ${PARQUET_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS ${PARQUET_STATIC_LINK_LIBS}
)

add_dependencies(parquet ${PARQUET_LIBRARIES})

# Thrift requires these definitions for some types that we use
foreach(LIB_TARGET ${PARQUET_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
    PRIVATE PARQUET_EXPORTING
    PRIVATE HAVE_INTTYPES_H
    PRIVATE HAVE_NETDB_H)
  if (MSVC)
    target_compile_definitions(${LIB_TARGET}
      PRIVATE NOMINMAX)
  else()
    target_compile_definitions(${LIB_TARGET}
      PRIVATE HAVE_NETINET_IN_H)
  endif()
endforeach()

add_subdirectory(api)
add_subdirectory(arrow)
add_subdirectory(util)

ARROW_INSTALL_ALL_HEADERS("parquet")

configure_file(parquet_version.h.in
  "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h"
  @ONLY)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/parquet")

# pkg-config support
ARROW_ADD_PKG_CONFIG("parquet")

ADD_PARQUET_TEST(bloom_filter-test)
ADD_PARQUET_TEST(column_reader-test)
ADD_PARQUET_TEST(column_scanner-test)
ADD_PARQUET_TEST(column_writer-test)
ADD_PARQUET_TEST(file-serialize-test)
ADD_PARQUET_TEST(properties-test)
ADD_PARQUET_TEST(statistics-test)
ADD_PARQUET_TEST(encoding-test)
ADD_PARQUET_TEST(metadata-test)
ADD_PARQUET_TEST(public-api-test)
ADD_PARQUET_TEST(types-test)
ADD_PARQUET_TEST(reader-test)

# Those tests need to use static linking as they access thrift-generated
# symbols which are not exported by parquet.dll on Windows (PARQUET-1420).
ADD_PARQUET_TEST(file-deserialize-test USE_STATIC_LINKING)
ADD_PARQUET_TEST(schema-test USE_STATIC_LINKING)

ADD_ARROW_BENCHMARK(column-io-benchmark
  PREFIX "parquet"
  LABELS "parquet-benchmarks"
  EXTRA_LINK_LIBS ${PARQUET_BENCHMARK_LINK_LIBRARIES})
ADD_ARROW_BENCHMARK(encoding-benchmark
  PREFIX "parquet"
  LABELS "parquet-benchmarks"
  EXTRA_LINK_LIBS ${PARQUET_BENCHMARK_LINK_LIBRARIES})

# Required for tests, the ExternalProject for zstd does not build on CMake < 3.7
if (ARROW_WITH_ZSTD)
  add_definitions(-DARROW_WITH_ZSTD)
endif()
