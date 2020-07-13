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
set(PARQUET_VERSION "${PARQUET_VERSION}" PARENT_SCOPE)

string(REGEX
       REPLACE "^([0-9]+)\.[0-9]+\.[0-9]+(-.+)?$" "\\1" PARQUET_VERSION_MAJOR
               "${PARQUET_VERSION}")
string(REGEX
       REPLACE "^[0-9]+\.([0-9]+)\.[0-9]+(-.+)?$" "\\1" PARQUET_VERSION_MINOR
               "${PARQUET_VERSION}")
string(REGEX
       REPLACE "^[0-9]+\.[0-9]+\.([0-9]+)(-.+)?$" "\\1" PARQUET_VERSION_PATCH
               "${PARQUET_VERSION}")

function(ADD_PARQUET_TEST REL_TEST_NAME)
  set(one_value_args)
  set(multi_value_args EXTRA_DEPENDENCIES LABELS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  set(TEST_ARGUMENTS PREFIX "parquet" LABELS "parquet-tests")

  if(ARROW_TEST_LINKAGE STREQUAL "static")
    add_test_case(${REL_TEST_NAME}
                  STATIC_LINK_LIBS
                  ${PARQUET_STATIC_TEST_LINK_LIBS}
                  ${TEST_ARGUMENTS}
                  ${ARG_UNPARSED_ARGUMENTS})
  else()
    add_test_case(${REL_TEST_NAME}
                  STATIC_LINK_LIBS
                  ${PARQUET_SHARED_TEST_LINK_LIBS}
                  ${TEST_ARGUMENTS}
                  ${ARG_UNPARSED_ARGUMENTS})
  endif()
endfunction()

function(ADD_PARQUET_FUZZ_TARGET REL_FUZZING_NAME)
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
    set(PREFIX "parquet")
  endif()

  if(ARROW_BUILD_STATIC)
    set(LINK_LIBS parquet_static)
  else()
    set(LINK_LIBS parquet_shared)
  endif()
  add_fuzz_target(${REL_FUZZING_NAME}
                  PREFIX
                  ${PREFIX}
                  LINK_LIBS
                  ${LINK_LIBS}
                  ${ARG_UNPARSED_ARGUMENTS})
endfunction()

function(ADD_PARQUET_BENCHMARK REL_TEST_NAME)
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
    set(PREFIX "parquet")
  endif()
  add_benchmark(${REL_TEST_NAME}
                PREFIX
                ${PREFIX}
                LABELS
                "parquet-benchmarks"
                ${PARQUET_BENCHMARK_LINK_OPTION}
                ${ARG_UNPARSED_ARGUMENTS})
endfunction()

# ----------------------------------------------------------------------
# Link libraries setup

# TODO(wesm): Handling of ABI/SO version

if(ARROW_BUILD_STATIC)
  set(PARQUET_STATIC_LINK_LIBS arrow_static)
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS arrow_testing_static arrow_static)
else()
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS arrow_testing_shared arrow_shared)
endif()

set(PARQUET_BOOST_LINK_LIBS)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9")
  add_definitions(-DPARQUET_USE_BOOST_REGEX)
  list(APPEND PARQUET_BOOST_LINK_LIBS ${BOOST_REGEX_LIBRARY})
endif()

set(PARQUET_MIN_TEST_LIBS GTest::gtest_main GTest::gtest)

if(APPLE)
  set(PARQUET_MIN_TEST_LIBS ${PARQUET_MIN_TEST_LIBS} ${CMAKE_DL_LIBS})
elseif(NOT MSVC)
  set(PARQUET_MIN_TEST_LIBS ${PARQUET_MIN_TEST_LIBS} pthread ${CMAKE_DL_LIBS})
endif()

set(PARQUET_SHARED_TEST_LINK_LIBS arrow_testing_shared ${PARQUET_MIN_TEST_LIBS}
                                  parquet_shared thrift::thrift)

set(PARQUET_STATIC_TEST_LINK_LIBS ${PARQUET_MIN_TEST_LIBS} parquet_static thrift::thrift
                                  ${ARROW_LIBRARIES_FOR_STATIC_TESTS})

#
# Generated Thrift sources
set_source_files_properties(src/generated/parquet_types.cpp
                            src/generated/parquet_types.h
                            src/generated/parquet_constants.cpp
                            src/generated/parquet_constants.h
                            PROPERTIES
                            SKIP_PRECOMPILE_HEADERS
                            ON
                            SKIP_UNITY_BUILD_INCLUSION
                            ON)

if(NOT MSVC)
  set_source_files_properties(src/parquet/parquet_types.cpp PROPERTIES COMPILE_FLAGS
                              -Wno-unused-variable)
endif()

#
# Library config

set(PARQUET_SRCS
    arrow/path_internal.cc
    arrow/reader.cc
    arrow/reader_internal.cc
    arrow/schema.cc
    arrow/schema_internal.cc
    arrow/writer.cc
    bloom_filter.cc
    column_reader.cc
    column_scanner.cc
    column_writer.cc
    deprecated_io.cc
    encoding.cc
    encryption.cc
    file_reader.cc
    file_writer.cc
    internal_file_decryptor.cc
    internal_file_encryptor.cc
    level_conversion.cc
    metadata.cc
    murmur3.cc
    "${ARROW_SOURCE_DIR}/src/generated/parquet_constants.cpp"
    "${ARROW_SOURCE_DIR}/src/generated/parquet_types.cpp"
    platform.cc
    printer.cc
    properties.cc
    schema.cc
    statistics.cc
    stream_reader.cc
    stream_writer.cc
    types.cc)

if(PARQUET_REQUIRE_ENCRYPTION)
  set(PARQUET_SRCS ${PARQUET_SRCS} encryption_internal.cc)
else()
  set(PARQUET_SRCS ${PARQUET_SRCS} encryption_internal_nossl.cc)
endif()

if(NOT PARQUET_MINIMAL_DEPENDENCY)
  set(PARQUET_SHARED_LINK_LIBS arrow_shared)

  # These are libraries that we will link privately with parquet_shared (as they
  # do not need to be linked transitively by other linkers)
  set(PARQUET_SHARED_PRIVATE_LINK_LIBS ${PARQUET_BOOST_LINK_LIBS} thrift::thrift)

  # Link publicly with parquet_static (because internal users need to
  # transitively link all dependencies)
  set(PARQUET_STATIC_LINK_LIBS ${PARQUET_STATIC_LINK_LIBS} ${PARQUET_BOOST_LINK_LIBS}
                               thrift::thrift)

  # Although we don't link parquet_objlib against anything, we need it to depend
  # on these libs as we may generate their headers via ExternalProject_Add
  if(ARROW_BUILD_SHARED)
    set(PARQUET_DEPENDENCIES ${PARQUET_DEPENDENCIES} ${PARQUET_SHARED_LINK_LIBS}
                             ${PARQUET_SHARED_PRIVATE_LINK_LIBS})
  endif()

  if(ARROW_BUILD_STATIC)
    set(PARQUET_DEPENDENCIES ${PARQUET_DEPENDENCIES} ${PARQUET_STATIC_LINK_LIBS})
  endif()

endif(NOT PARQUET_MINIMAL_DEPENDENCY)

if(NOT APPLE AND NOT MSVC)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(PARQUET_SHARED_LINK_FLAGS
      "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

add_arrow_lib(parquet
              CMAKE_PACKAGE_NAME
              Parquet
              PKG_CONFIG_NAME
              parquet
              SOURCES
              ${PARQUET_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:parquet/pch.h>"
              OUTPUTS
              PARQUET_LIBRARIES
              DEPENDENCIES
              ${PARQUET_DEPENDENCIES}
              SHARED_LINK_FLAGS
              ${PARQUET_SHARED_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${PARQUET_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${PARQUET_SHARED_PRIVATE_LINK_LIBS}
              STATIC_LINK_LIBS
              ${PARQUET_STATIC_LINK_LIBS})

if(WIN32 AND NOT (ARROW_TEST_LINKAGE STREQUAL "static"))
  add_library(parquet_test_support STATIC
              "${ARROW_SOURCE_DIR}/src/generated/parquet_constants.cpp"
              "${ARROW_SOURCE_DIR}/src/generated/parquet_types.cpp")
  add_dependencies(parquet_test_support thrift::thrift)
  set(PARQUET_SHARED_TEST_LINK_LIBS ${PARQUET_SHARED_TEST_LINK_LIBS} parquet_test_support)
  set(PARQUET_LIBRARIES ${PARQUET_LIBRARIES} parquet_test_support)
endif()

if(NOT ARROW_BUILD_SHARED)
  set(PARQUET_BENCHMARK_LINK_OPTION STATIC_LINK_LIBS benchmark::benchmark_main
                                    ${PARQUET_STATIC_TEST_LINK_LIBS})
else()
  set(PARQUET_BENCHMARK_LINK_OPTION EXTRA_LINK_LIBS ${PARQUET_SHARED_TEST_LINK_LIBS})
endif()

if(ARROW_BUILD_STATIC AND WIN32)
  # ARROW-4848: Static Parquet lib needs to import static symbols on Windows
  target_compile_definitions(parquet_static PUBLIC ARROW_STATIC)
endif()

add_dependencies(parquet ${PARQUET_LIBRARIES} thrift::thrift)

# Thrift requires these definitions for some types that we use
foreach(LIB_TARGET ${PARQUET_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
                             PRIVATE
                             PARQUET_EXPORTING
                             PRIVATE
                             HAVE_INTTYPES_H
                             PRIVATE
                             HAVE_NETDB_H)
  if(WIN32)
    target_compile_definitions(${LIB_TARGET} PRIVATE NOMINMAX)
  else()
    target_compile_definitions(${LIB_TARGET} PRIVATE HAVE_NETINET_IN_H)
  endif()
endforeach()

if(WIN32 AND ARROW_BUILD_STATIC)
  target_compile_definitions(parquet_static PUBLIC PARQUET_STATIC)
endif()

add_subdirectory(api)
add_subdirectory(arrow)

arrow_install_all_headers("parquet")

configure_file(parquet_version.h.in "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h" @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/parquet")

add_parquet_test(internals-test
                 SOURCES
                 bloom_filter_test.cc
                 deprecated_io_test.cc
                 properties_test.cc
                 statistics_test.cc
                 encoding_test.cc
                 metadata_test.cc
                 public_api_test.cc
                 types_test.cc
                 test_util.cc)

set_source_files_properties(public_api_test.cc
                            PROPERTIES
                            SKIP_PRECOMPILE_HEADERS
                            ON
                            SKIP_UNITY_BUILD_INCLUSION
                            ON)

add_parquet_test(reader_test
                 SOURCES
                 column_reader_test.cc
                 level_conversion_test.cc
                 column_scanner_test.cc
                 reader_test.cc
                 stream_reader_test.cc
                 test_util.cc)

add_parquet_test(writer-test
                 SOURCES
                 column_writer_test.cc
                 file_serialize_test.cc
                 stream_writer_test.cc
                 test_util.cc)

add_parquet_test(arrow-test
                 SOURCES
                 arrow/arrow_reader_writer_test.cc
                 arrow/arrow_schema_test.cc
                 arrow/path_internal_test.cc
                 test_util.cc)

if(PARQUET_REQUIRE_ENCRYPTION)
  add_parquet_test(encryption-test
                   SOURCES
                   encryption_write_configurations_test.cc
                   encryption_read_configurations_test.cc
                   encryption_properties_test.cc
                   test_util.cc)
endif()

# Those tests need to use static linking as they access thrift-generated
# symbols which are not exported by parquet.dll on Windows (PARQUET-1420).
add_parquet_test(file_deserialize_test SOURCES file_deserialize_test.cc test_util.cc)
add_parquet_test(schema_test)

add_parquet_benchmark(column_io_benchmark)
add_parquet_benchmark(encoding_benchmark)
add_parquet_benchmark(level_conversion_benchmark)
add_parquet_benchmark(arrow/reader_writer_benchmark PREFIX "parquet-arrow")

if(ARROW_WITH_BROTLI)
  add_definitions(-DARROW_WITH_BROTLI)
endif()

if(ARROW_WITH_BZ2)
  add_definitions(-DARROW_WITH_BZ2)
endif()

if(ARROW_WITH_LZ4)
  add_definitions(-DARROW_WITH_LZ4)
endif()

if(ARROW_WITH_SNAPPY)
  add_definitions(-DARROW_WITH_SNAPPY)
endif()

if(ARROW_WITH_ZLIB)
  add_definitions(-DARROW_WITH_ZLIB)
endif()

if(ARROW_WITH_ZSTD)
  add_definitions(-DARROW_WITH_ZSTD)
endif()
