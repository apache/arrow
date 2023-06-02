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
  set(PARQUET_STATIC_LINK_LIBS arrow_static ${ARROW_STATIC_LINK_LIBS})
  set(PARQUET_STATIC_INSTALL_INTERFACE_LIBS Arrow::arrow_static)
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS arrow_testing_static arrow_static
                                       ${ARROW_STATIC_LINK_LIBS})
else()
  set(PARQUET_STATIC_INSTALL_INTERFACE_LIBS)
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS arrow_testing_shared arrow_shared)
endif()

set(PARQUET_MIN_TEST_LIBS ${ARROW_GTEST_GTEST_MAIN} ${ARROW_GTEST_GTEST} Boost::headers)

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
set(PARQUET_THRIFT_SOURCE_DIR "${ARROW_SOURCE_DIR}/src/generated/")

set_source_files_properties("${PARQUET_THRIFT_SOURCE_DIR}/parquet_types.cpp"
                            "${PARQUET_THRIFT_SOURCE_DIR}/parquet_types.h"
                            "${PARQUET_THRIFT_SOURCE_DIR}/parquet_constants.cpp"
                            "${PARQUET_THRIFT_SOURCE_DIR}/parquet_constants.h"
                            PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                       SKIP_UNITY_BUILD_INCLUSION ON)

if(NOT MSVC)
  set_source_files_properties(src/parquet/parquet_types.cpp
                              PROPERTIES COMPILE_FLAGS -Wno-unused-variable)
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
    bloom_filter_reader.cc
    column_reader.cc
    column_scanner.cc
    column_writer.cc
    encoding.cc
    encryption/encryption.cc
    encryption/internal_file_decryptor.cc
    encryption/internal_file_encryptor.cc
    exception.cc
    file_reader.cc
    file_writer.cc
    level_comparison.cc
    level_conversion.cc
    metadata.cc
    xxhasher.cc
    page_index.cc
    "${PARQUET_THRIFT_SOURCE_DIR}/parquet_constants.cpp"
    "${PARQUET_THRIFT_SOURCE_DIR}/parquet_types.cpp"
    platform.cc
    printer.cc
    properties.cc
    schema.cc
    statistics.cc
    stream_reader.cc
    stream_writer.cc
    types.cc)

if(ARROW_HAVE_RUNTIME_AVX2)
  # AVX2 is used as a proxy for BMI2.
  list(APPEND PARQUET_SRCS level_comparison_avx2.cc level_conversion_bmi2.cc)
  # We need CMAKE_CXX_FLAGS_RELEASE here to prevent the one-definition-rule
  # violation with -DCMAKE_BUILD_TYPE=MinSizeRel. CMAKE_CXX_FLAGS_RELEASE
  # will force inlining as much as possible.
  # See also: ARROW-15664 and ARROW-15678
  #
  # TODO: Use COMPILE_OPTIONS instead of COMPILE_FLAGS when we require
  # CMake 3.11 or later.
  set(AVX2_FLAGS "${ARROW_AVX2_FLAG}")
  if(NOT MSVC)
    string(APPEND AVX2_FLAGS " ${CMAKE_CXX_FLAGS_RELEASE}")
  endif()
  set_source_files_properties(level_comparison_avx2.cc
                              PROPERTIES SKIP_PRECOMPILE_HEADERS ON COMPILE_FLAGS
                                                                    "${AVX2_FLAGS}")
  # WARNING: DO NOT BLINDLY COPY THIS CODE FOR OTHER BMI2 USE CASES.
  # This code is always guarded by runtime dispatch which verifies
  # BMI2 is present.  For a very small number of CPUs AVX2 does not
  # imply BMI2.
  #
  # We need CMAKE_CXX_FLAGS_RELEASE here to prevent the one-definition-rule
  # violation with -DCMAKE_BUILD_TYPE=MinSizeRel. CMAKE_CXX_FLAGS_RELEASE
  # will force inlining as much as possible.
  # See also: ARROW-15664 and ARROW-15678
  #
  # TODO: Use COMPILE_OPTIONS instead of COMPILE_FLAGS when we require
  # CMake 3.11 or later.
  set(BMI2_FLAGS "${AVX2_FLAGS} -DARROW_HAVE_BMI2 -mbmi2")
  set_source_files_properties(level_conversion_bmi2.cc
                              PROPERTIES SKIP_PRECOMPILE_HEADERS ON COMPILE_FLAGS
                                                                    "${BMI2_FLAGS}")
endif()

if(PARQUET_REQUIRE_ENCRYPTION)
  set(PARQUET_SRCS ${PARQUET_SRCS} encryption/encryption_internal.cc)
  # Encryption key management
  set(PARQUET_SRCS
      ${PARQUET_SRCS}
      encryption/crypto_factory.cc
      encryption/file_key_unwrapper.cc
      encryption/file_key_wrapper.cc
      encryption/file_system_key_material_store.cc
      encryption/kms_client.cc
      encryption/key_material.cc
      encryption/key_metadata.cc
      encryption/key_toolkit.cc
      encryption/key_toolkit_internal.cc
      encryption/local_wrap_kms_client.cc)
else()
  set(PARQUET_SRCS ${PARQUET_SRCS} encryption/encryption_internal_nossl.cc)
endif()

if(NOT PARQUET_MINIMAL_DEPENDENCY)
  set(PARQUET_SHARED_LINK_LIBS arrow_shared)

  # These are libraries that we will link privately with parquet_shared (as they
  # do not need to be linked transitively by other linkers)
  set(PARQUET_SHARED_PRIVATE_LINK_LIBS thrift::thrift)

  # Link publicly with parquet_static (because internal users need to
  # transitively link all dependencies)
  list(APPEND PARQUET_STATIC_LINK_LIBS thrift::thrift)
  if(NOT THRIFT_VENDORED)
    list(APPEND PARQUET_STATIC_INSTALL_INTERFACE_LIBS thrift::thrift)
  endif()
endif()

if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
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
              SHARED_LINK_FLAGS
              ${PARQUET_SHARED_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${PARQUET_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${PARQUET_SHARED_PRIVATE_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              Arrow::arrow_shared
              STATIC_LINK_LIBS
              ${PARQUET_STATIC_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${PARQUET_STATIC_INSTALL_INTERFACE_LIBS})

if(WIN32 AND NOT (ARROW_TEST_LINKAGE STREQUAL "static"))
  add_library(parquet_test_support STATIC
              "${PARQUET_THRIFT_SOURCE_DIR}/parquet_constants.cpp"
              "${PARQUET_THRIFT_SOURCE_DIR}/parquet_types.cpp")
  target_link_libraries(parquet_test_support thrift::thrift)
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
  target_compile_definitions(parquet_static PUBLIC PARQUET_STATIC)
endif()

add_definitions(-DPARQUET_THRIFT_VERSION_MAJOR=${Thrift_VERSION_MAJOR})
add_definitions(-DPARQUET_THRIFT_VERSION_MINOR=${Thrift_VERSION_MINOR})

# Thrift requires these definitions for some types that we use
foreach(LIB_TARGET ${PARQUET_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
                             PRIVATE PARQUET_EXPORTING
                             PRIVATE HAVE_INTTYPES_H
                             PRIVATE HAVE_NETDB_H)
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
add_subdirectory(encryption)

arrow_install_all_headers("parquet")

configure_file(parquet_version.h.in "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h" @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/parquet")

add_parquet_test(internals-test
                 SOURCES
                 bloom_filter_test.cc
                 bloom_filter_reader_test.cc
                 properties_test.cc
                 statistics_test.cc
                 encoding_test.cc
                 metadata_test.cc
                 page_index_test.cc
                 public_api_test.cc
                 types_test.cc
                 test_util.cc)

set_source_files_properties(public_api_test.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                                          SKIP_UNITY_BUILD_INCLUSION ON)

add_parquet_test(reader-test
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
                 arrow/arrow_statistics_test.cc
                 test_util.cc)

add_parquet_test(arrow-internals-test
                 SOURCES
                 arrow/path_internal_test.cc
                 arrow/reconstruct_internal_test.cc
                 test_util.cc)

if(PARQUET_REQUIRE_ENCRYPTION)
  add_parquet_test(encryption-test
                   SOURCES
                   encryption/write_configurations_test.cc
                   encryption/read_configurations_test.cc
                   encryption/properties_test.cc
                   encryption/test_encryption_util.cc
                   test_util.cc)
  add_parquet_test(encryption-key-management-test
                   SOURCES
                   encryption/key_management_test.cc
                   encryption/key_metadata_test.cc
                   encryption/key_wrapping_test.cc
                   encryption/test_encryption_util.cc
                   encryption/test_in_memory_kms.cc
                   encryption/two_level_cache_with_expiration_test.cc
                   test_util.cc)
endif()

# Those tests need to use static linking as they access thrift-generated
# symbols which are not exported by parquet.dll on Windows (PARQUET-1420).
add_parquet_test(file_deserialize_test SOURCES file_deserialize_test.cc test_util.cc)
add_parquet_test(schema_test)

add_parquet_benchmark(bloom_filter_benchmark)
add_parquet_benchmark(column_reader_benchmark)
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
