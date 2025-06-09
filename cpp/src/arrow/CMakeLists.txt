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
add_custom_target(arrow-integration)
add_dependencies(arrow-all
                 arrow
                 arrow-tests
                 arrow-benchmarks
                 arrow-integration)

# Libraries to link with libarrow.so. They aren't exported.
set(ARROW_SHARED_PRIVATE_LINK_LIBS)

# Libraries to link with exported libarrow.{so,a}.
set(ARROW_SHARED_INSTALL_INTERFACE_LIBS)
set(ARROW_STATIC_INSTALL_INTERFACE_LIBS)

if(ARROW_GCS)
  if(google_cloud_cpp_storage_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS google-cloud-cpp::storage)
  endif()
endif()

if(ARROW_USE_OPENSSL)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_OPENSSL_LIBS})
endif()

if(ARROW_WITH_BROTLI)
  if(Brotli_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_BROTLI_LIBS})
  endif()
endif()

if(ARROW_WITH_BZ2)
  if(BZip2_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS BZip2::BZip2)
  endif()
endif()

if(ARROW_WITH_LZ4)
  if(lz4_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS LZ4::lz4)
  endif()
endif()

if(ARROW_WITH_SNAPPY)
  if(Snappy_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${Snappy_TARGET})
  endif()
endif()

if(ARROW_WITH_ZLIB)
  if(ZLIB_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ZLIB::ZLIB)
  endif()
endif()

if(ARROW_WITH_ZSTD)
  if(zstd_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_ZSTD_LIBZSTD})
  endif()
endif()

if(ARROW_ORC)
  if(ORC_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS orc::orc)
  endif()
endif()

if(ARROW_USE_GLOG)
  if(GLOG_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS glog::glog)
  endif()
endif()

if(ARROW_S3)
  if(AWSSDK_SOURCE STREQUAL "SYSTEM")
    list(APPEND
         ARROW_STATIC_INSTALL_INTERFACE_LIBS
         aws-cpp-sdk-identity-management
         aws-cpp-sdk-sts
         aws-cpp-sdk-cognito-identity
         aws-cpp-sdk-s3
         aws-cpp-sdk-core)
  elseif(AWSSDK_SOURCE STREQUAL "BUNDLED")
    if(UNIX)
      list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS CURL::libcurl)
    endif()
  endif()
endif()

if(ARROW_WITH_OPENTELEMETRY)
  if(opentelemetry_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_OPENTELEMETRY_LIBS})
  endif()
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS CURL::libcurl)
endif()

if(ARROW_WITH_UTF8PROC)
  if(utf8proc_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS utf8proc::utf8proc)
  endif()
endif()

if(ARROW_WITH_RE2)
  if(re2_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS re2::re2)
  endif()
endif()

if(ARROW_PROTOBUF_ARROW_CMAKE_PACKAGE_NAME STREQUAL "Arrow")
  if(Protobuf_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF})
  endif()
endif()

if(ARROW_ENABLE_THREADING)
  list(APPEND ARROW_SHARED_PRIVATE_LINK_LIBS Threads::Threads)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS Threads::Threads)
endif()

set(ARROW_TEST_LINK_TOOLCHAIN ${ARROW_GTEST_GMOCK} ${ARROW_GTEST_GTEST_MAIN})
set(ARROW_TEST_STATIC_LINK_LIBS arrow::flatbuffers arrow_testing_static arrow_static
                                ${ARROW_TEST_LINK_TOOLCHAIN})
set(ARROW_TEST_SHARED_LINK_LIBS arrow::flatbuffers arrow_testing_shared arrow_shared
                                ${ARROW_TEST_LINK_TOOLCHAIN})

if(CMAKE_DL_LIBS)
  list(APPEND ARROW_SHARED_INSTALL_INTERFACE_LIBS ${CMAKE_DL_LIBS})
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${CMAKE_DL_LIBS})
  list(APPEND ARROW_TEST_SHARED_LINK_LIBS ${CMAKE_DL_LIBS})
  list(APPEND ARROW_TEST_STATIC_LINK_LIBS ${CMAKE_DL_LIBS})
  string(APPEND ARROW_PC_LIBS_PRIVATE " -l${CMAKE_DL_LIBS}")
endif()

# ----------------------------------------------------------------------
# Handle platform-related libraries like -pthread

set(ARROW_SYSTEM_LINK_LIBS)

if(CMAKE_THREAD_LIBS_INIT)
  string(APPEND ARROW_PC_LIBS_PRIVATE " ${CMAKE_THREAD_LIBS_INIT}")
endif()

if(WIN32)
  list(APPEND ARROW_SYSTEM_LINK_LIBS "ws2_32")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND ARROW_SYSTEM_LINK_LIBS rt)
endif()

list(APPEND ARROW_SHARED_INSTALL_INTERFACE_LIBS ${ARROW_SYSTEM_LINK_LIBS})
list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_SYSTEM_LINK_LIBS})

# Need -latomic on Raspbian.
# See also: https://issues.apache.org/jira/browse/ARROW-12860
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux" AND ${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv7")
  string(APPEND ARROW_PC_LIBS_PRIVATE " -latomic")
  list(APPEND ARROW_SHARED_INSTALL_INTERFACE_LIBS "atomic")
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS "atomic")
endif()

# This creates OBJECT libraries for arrow_shared/arrow_static. This is
# not intended to use for other libraries such as
# arrow_acero_shared/arrow_acero_static for now.
#
# arrow_shared/arrow_static depends on many external libraries such as
# Zstandard and jemalloc. If we use bundled libraries, we can't start
# building arrow_shared/arrow_static until all bundled libraries are
# built. It prevent parallel build speedup.
#
# We can avoid the situation by creating small OBJECT libraries that
# depend only needed external libraries. If an OBJECT library doesn't
# depend on any bundled libraries, it can be built before bundled
# libraries are built. If an OBJECT library depend on only a few
# bundled libraries, it can be built after only they are built.
function(arrow_add_object_library PREFIX)
  set(SOURCES ${ARGN})
  string(TOLOWER "${PREFIX}" prefix)
  if(WIN32)
    set(targets)
    if(ARROW_BUILD_SHARED)
      add_library(${prefix}_shared OBJECT ${SOURCES})
      set_target_properties(${prefix}_shared PROPERTIES POSITION_INDEPENDENT_CODE ON)
      target_compile_definitions(${prefix}_shared PRIVATE ARROW_EXPORTING)
      target_compile_features(${prefix}_shared PRIVATE cxx_std_17)
      set(${PREFIX}_TARGET_SHARED
          ${prefix}_shared
          PARENT_SCOPE)
      list(APPEND targets ${prefix}_shared)
    endif()
    if(ARROW_BUILD_STATIC)
      add_library(${prefix}_static OBJECT ${SOURCES})
      set_target_properties(${prefix}_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
      target_compile_definitions(${prefix}_static PRIVATE ARROW_STATIC)
      target_compile_features(${prefix}_static PRIVATE cxx_std_17)
      set(${PREFIX}_TARGET_STATIC
          ${prefix}_static
          PARENT_SCOPE)
      list(APPEND targets ${prefix}_static)
    endif()
    set(${PREFIX}_TARGETS
        ${targets}
        PARENT_SCOPE)
  else()
    add_library(${prefix} OBJECT ${SOURCES})
    set_target_properties(${prefix} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_compile_features(${prefix} PRIVATE cxx_std_17)
    set(${PREFIX}_TARGET_SHARED
        ${prefix}
        PARENT_SCOPE)
    set(${PREFIX}_TARGET_STATIC
        ${prefix}
        PARENT_SCOPE)
    set(${PREFIX}_TARGETS
        ${prefix}
        PARENT_SCOPE)
  endif()
endfunction()

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

function(ADD_ARROW_FUZZ_TARGET REL_FUZZING_NAME)
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

  if(ARROW_BUILD_STATIC)
    set(LINK_LIBS arrow_static)
  else()
    set(LINK_LIBS arrow_shared)
  endif()
  add_fuzz_target(${REL_FUZZING_NAME}
                  PREFIX
                  ${PREFIX}
                  LINK_LIBS
                  ${LINK_LIBS}
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

macro(append_runtime_avx2_src SRCS SRC)
  if(ARROW_HAVE_RUNTIME_AVX2)
    list(APPEND ${SRCS} ${SRC})
    set_source_files_properties(${SRC} PROPERTIES COMPILE_FLAGS ${ARROW_AVX2_FLAG})
  endif()
endmacro()

macro(append_runtime_avx2_bmi2_src SRCS SRC)
  if(ARROW_HAVE_RUNTIME_AVX2 AND ARROW_HAVE_RUNTIME_BMI2)
    list(APPEND ${SRCS} ${SRC})
    set_source_files_properties(${SRC} PROPERTIES COMPILE_FLAGS
                                                  "${ARROW_AVX2_FLAG} ${ARROW_BMI2_FLAG}")
  endif()
endmacro()

macro(append_runtime_avx512_src SRCS SRC)
  if(ARROW_HAVE_RUNTIME_AVX512)
    list(APPEND ${SRCS} ${SRC})
    set_source_files_properties(${SRC} PROPERTIES COMPILE_FLAGS ${ARROW_AVX512_FLAG})
  endif()
endmacro()

# Write out compile-time configuration constants
configure_file("util/config.h.cmake" "util/config.h" ESCAPE_QUOTES)
configure_file("util/config_internal.h.cmake" "util/config_internal.h" ESCAPE_QUOTES)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/util/config.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/util")

set(ARROW_SRCS
    builder.cc
    buffer.cc
    chunked_array.cc
    chunk_resolver.cc
    compare.cc
    config.cc
    datum.cc
    device.cc
    device_allocation_type_set.cc
    extension_type.cc
    extension/bool8.cc
    extension/json.cc
    extension/uuid.cc
    pretty_print.cc
    record_batch.cc
    result.cc
    scalar.cc
    sparse_tensor.cc
    status.cc
    table.cc
    table_builder.cc
    tensor.cc
    tensor/coo_converter.cc
    tensor/csf_converter.cc
    tensor/csx_converter.cc
    type.cc
    type_traits.cc
    visitor.cc
    c/bridge.cc
    c/dlpack.cc)

arrow_add_object_library(ARROW_ARRAY
                         array/array_base.cc
                         array/array_binary.cc
                         array/array_decimal.cc
                         array/array_dict.cc
                         array/array_nested.cc
                         array/array_primitive.cc
                         array/array_run_end.cc
                         array/builder_adaptive.cc
                         array/builder_base.cc
                         array/builder_binary.cc
                         array/builder_decimal.cc
                         array/builder_dict.cc
                         array/builder_run_end.cc
                         array/builder_nested.cc
                         array/builder_primitive.cc
                         array/builder_union.cc
                         array/concatenate.cc
                         array/data.cc
                         array/diff.cc
                         array/statistics.cc
                         array/util.cc
                         array/validate.cc)

arrow_add_object_library(ARROW_IO
                         io/buffered.cc
                         io/caching.cc
                         io/compressed.cc
                         io/file.cc
                         io/hdfs.cc
                         io/hdfs_internal.cc
                         io/interfaces.cc
                         io/memory.cc
                         io/slow.cc
                         io/stdio.cc
                         io/transform.cc)
foreach(ARROW_IO_TARGET ${ARROW_IO_TARGETS})
  target_link_libraries(${ARROW_IO_TARGET} PRIVATE arrow::hadoop)
  if(NOT MSVC)
    target_link_libraries(${ARROW_IO_TARGET} PRIVATE ${CMAKE_DL_LIBS})
  endif()
endforeach()

set(ARROW_MEMORY_POOL_SRCS memory_pool.cc)
if(ARROW_JEMALLOC)
  list(APPEND ARROW_MEMORY_POOL_SRCS memory_pool_jemalloc.cc)
  set_source_files_properties(memory_pool_jemalloc.cc
                              PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
endif()
arrow_add_object_library(ARROW_MEMORY_POOL ${ARROW_MEMORY_POOL_SRCS})
if(ARROW_JEMALLOC)
  foreach(ARROW_MEMORY_POOL_TARGET ${ARROW_MEMORY_POOL_TARGETS})
    target_link_libraries(${ARROW_MEMORY_POOL_TARGET} PRIVATE jemalloc::jemalloc)
  endforeach()
endif()
if(ARROW_MIMALLOC)
  foreach(ARROW_MEMORY_POOL_TARGET ${ARROW_MEMORY_POOL_TARGETS})
    target_link_libraries(${ARROW_MEMORY_POOL_TARGET} PRIVATE mimalloc::mimalloc)
  endforeach()
endif()

set(ARROW_VENDORED_SRCS
    vendored/base64.cpp
    vendored/datetime.cpp
    vendored/double-conversion/bignum-dtoa.cc
    vendored/double-conversion/bignum.cc
    vendored/double-conversion/cached-powers.cc
    vendored/double-conversion/double-to-string.cc
    vendored/double-conversion/fast-dtoa.cc
    vendored/double-conversion/fixed-dtoa.cc
    vendored/double-conversion/string-to-double.cc
    vendored/double-conversion/strtod.cc
    vendored/musl/strptime.c
    vendored/uriparser/UriCommon.c
    vendored/uriparser/UriCompare.c
    vendored/uriparser/UriEscape.c
    vendored/uriparser/UriFile.c
    vendored/uriparser/UriIp4.c
    vendored/uriparser/UriIp4Base.c
    vendored/uriparser/UriMemory.c
    vendored/uriparser/UriNormalize.c
    vendored/uriparser/UriNormalizeBase.c
    vendored/uriparser/UriParse.c
    vendored/uriparser/UriParseBase.c
    vendored/uriparser/UriQuery.c
    vendored/uriparser/UriRecompose.c
    vendored/uriparser/UriResolve.c
    vendored/uriparser/UriShorten.c)
if(APPLE)
  list(APPEND ARROW_VENDORED_SRCS vendored/datetime/ios.mm)
endif()
set_source_files_properties(vendored/datetime.cpp PROPERTIES SKIP_UNITY_BUILD_INCLUSION
                                                             ON)
arrow_add_object_library(ARROW_VENDORED ${ARROW_VENDORED_SRCS})
# Disable DLL exports in vendored uriparser library
foreach(ARROW_VENDORED_TARGET ${ARROW_VENDORED_TARGETS})
  target_compile_definitions(${ARROW_VENDORED_TARGET} PRIVATE URI_STATIC_BUILD)
endforeach()

set(ARROW_UTIL_SRCS
    util/align_util.cc
    util/async_util.cc
    util/atfork_internal.cc
    util/basic_decimal.cc
    util/bit_block_counter.cc
    util/bit_run_reader.cc
    util/bit_util.cc
    util/bitmap.cc
    util/bitmap_builders.cc
    util/bitmap_ops.cc
    util/bpacking.cc
    util/byte_size.cc
    util/cancel.cc
    util/compression.cc
    util/counting_semaphore.cc
    util/cpu_info.cc
    util/crc32.cc
    util/debug.cc
    util/decimal.cc
    util/delimiting.cc
    util/dict_util.cc
    util/fixed_width_internal.cc
    util/float16.cc
    util/formatting.cc
    util/future.cc
    util/hashing.cc
    util/int_util.cc
    util/io_util.cc
    util/list_util.cc
    util/logger.cc
    util/logging.cc
    util/key_value_metadata.cc
    util/math_internal.cc
    util/memory.cc
    util/mutex.cc
    util/ree_util.cc
    util/secure_string.cc
    util/string.cc
    util/string_builder.cc
    util/task_group.cc
    util/tdigest.cc
    util/thread_pool.cc
    util/time.cc
    util/tracing.cc
    util/trie.cc
    util/union_util.cc
    util/unreachable.cc
    util/uri.cc
    util/utf8.cc
    util/value_parsing.cc)

append_runtime_avx2_src(ARROW_UTIL_SRCS util/bpacking_avx2.cc)
append_runtime_avx512_src(ARROW_UTIL_SRCS util/bpacking_avx512.cc)
if(ARROW_HAVE_NEON)
  list(APPEND ARROW_UTIL_SRCS util/bpacking_neon.cc)
endif()

if(ARROW_WITH_BROTLI)
  list(APPEND ARROW_UTIL_SRCS util/compression_brotli.cc)
endif()
if(ARROW_WITH_BZ2)
  list(APPEND ARROW_UTIL_SRCS util/compression_bz2.cc)
endif()
if(ARROW_WITH_LZ4)
  list(APPEND ARROW_UTIL_SRCS util/compression_lz4.cc)
endif()
if(ARROW_WITH_OPENTELEMETRY)
  list(APPEND ARROW_UTIL_SRCS util/tracing_internal.cc)
endif()
if(ARROW_WITH_SNAPPY)
  list(APPEND ARROW_UTIL_SRCS util/compression_snappy.cc)
endif()
if(ARROW_WITH_ZLIB)
  list(APPEND ARROW_UTIL_SRCS util/compression_zlib.cc)
endif()
if(ARROW_WITH_ZSTD)
  list(APPEND ARROW_UTIL_SRCS util/compression_zstd.cc)
endif()

arrow_add_object_library(ARROW_UTIL ${ARROW_UTIL_SRCS})

# Disable DLL exports in vendored uriparser library
foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
  target_compile_definitions(${ARROW_UTIL_TARGET} PRIVATE URI_STATIC_BUILD)
endforeach()

if(ARROW_USE_BOOST)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE Boost::headers)
  endforeach()
endif()
if(ARROW_USE_GLOG)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE glog::glog)
  endforeach()
endif()
if(ARROW_USE_OPENSSL)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ${ARROW_OPENSSL_LIBS})
  endforeach()
endif()
if(ARROW_USE_XSIMD)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ${ARROW_XSIMD})
  endforeach()
endif()
if(ARROW_WITH_BROTLI)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ${ARROW_BROTLI_LIBS})
  endforeach()
endif()
if(ARROW_WITH_BZ2)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE BZip2::BZip2)
  endforeach()
endif()
if(ARROW_WITH_LZ4)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE LZ4::lz4)
  endforeach()
endif()
if(ARROW_WITH_SNAPPY)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ${Snappy_TARGET})
  endforeach()
endif()
if(ARROW_WITH_OPENTELEMETRY)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ${ARROW_OPENTELEMETRY_LIBS})
  endforeach()
endif()
if(ARROW_WITH_RAPIDJSON)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE RapidJSON)
  endforeach()
endif()
if(ARROW_WITH_ZLIB)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ZLIB::ZLIB)
  endforeach()
endif()
if(ARROW_WITH_ZSTD)
  foreach(ARROW_UTIL_TARGET ${ARROW_UTIL_TARGETS})
    target_link_libraries(${ARROW_UTIL_TARGET} PRIVATE ${ARROW_ZSTD_LIBZSTD})
  endforeach()
endif()

if(ARROW_WITH_OPENTELEMETRY)
  arrow_add_object_library(ARROW_TELEMETRY telemetry/logging.cc)

  foreach(ARROW_TELEMETRY_TARGET ${ARROW_TELEMETRY_TARGETS})
    target_link_libraries(${ARROW_TELEMETRY_TARGET} PRIVATE ${ARROW_OPENTELEMETRY_LIBS})
  endforeach()
else()
  set(ARROW_TELEMETRY_TARGET_SHARED)
  set(ARROW_TELEMETRY_TARGET_STATIC)
endif()

set(ARROW_TESTING_SHARED_LINK_LIBS arrow_shared ${ARROW_GTEST_GTEST})
set(ARROW_TESTING_SHARED_PRIVATE_LINK_LIBS arrow::flatbuffers RapidJSON Boost::process)
set(ARROW_TESTING_STATIC_LINK_LIBS
    arrow::flatbuffers
    RapidJSON
    Boost::process
    arrow_static
    ${ARROW_GTEST_GTEST})
set(ARROW_TESTING_SHARED_INSTALL_INTERFACE_LIBS Arrow::arrow_shared)
set(ARROW_TESTING_STATIC_INSTALL_INTERFACE_LIBS Arrow::arrow_static)
# that depend on gtest
if(GTest_SOURCE STREQUAL "SYSTEM")
  list(APPEND ARROW_TESTING_SHARED_INSTALL_INTERFACE_LIBS ${ARROW_GTEST_GTEST})
  list(APPEND ARROW_TESTING_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_GTEST_GTEST})
else()
  list(APPEND ARROW_TESTING_SHARED_INSTALL_INTERFACE_LIBS ArrowTesting::gtest)
  list(APPEND ARROW_TESTING_STATIC_INSTALL_INTERFACE_LIBS ArrowTesting::gtest)
endif()
if(WIN32)
  list(APPEND ARROW_TESTING_SHARED_LINK_LIBS "ws2_32")
  list(APPEND ARROW_TESTING_STATIC_LINK_LIBS "ws2_32")
  list(APPEND ARROW_TESTING_STATIC_INSTALL_INTERFACE_LIBS "ws2_32")
endif()

set(ARROW_TESTING_SRCS
    io/test_common.cc
    ipc/test_common.cc
    testing/fixed_width_test_util.cc
    testing/generator.cc
    testing/gtest_util.cc
    testing/math.cc
    testing/process.cc
    testing/random.cc
    testing/util.cc)

#
# Configure the base Arrow libraries
#

if(ARROW_BUILD_INTEGRATION OR ARROW_BUILD_TESTS)
  arrow_add_object_library(ARROW_INTEGRATION integration/c_data_integration_internal.cc
                           integration/json_integration.cc integration/json_internal.cc)
  foreach(ARROW_INTEGRATION_TARGET ${ARROW_INTEGRATION_TARGETS})
    target_link_libraries(${ARROW_INTEGRATION_TARGET} PRIVATE RapidJSON)
  endforeach()
else()
  set(ARROW_INTEGRATION_TARGET_SHARED)
  set(ARROW_INTEGRATION_TARGET_STATIC)
endif()

if(ARROW_CSV)
  arrow_add_object_library(ARROW_CSV
                           csv/converter.cc
                           csv/chunker.cc
                           csv/column_builder.cc
                           csv/column_decoder.cc
                           csv/options.cc
                           csv/parser.cc
                           csv/reader.cc
                           csv/writer.cc)
  if(ARROW_USE_XSIMD)
    foreach(ARROW_CSV_TARGET ${ARROW_CSV_TARGETS})
      target_link_libraries(${ARROW_CSV_TARGET} PRIVATE ${ARROW_XSIMD})
    endforeach()
  endif()

  list(APPEND ARROW_TESTING_SRCS csv/test_common.cc)
else()
  set(ARROW_CSV_TARGET_SHARED)
  set(ARROW_CSV_TARGET_STATIC)
endif()

# Baseline Compute functionality + scalar casts and a few select kernels
set(ARROW_COMPUTE_SRCS
    compute/api_aggregate.cc
    compute/api_scalar.cc
    compute/api_vector.cc
    compute/cast.cc
    compute/exec.cc
    compute/expression.cc
    compute/function.cc
    compute/function_internal.cc
    compute/kernel.cc
    compute/ordering.cc
    compute/registry.cc
    compute/kernels/chunked_internal.cc
    compute/kernels/codegen_internal.cc
    compute/kernels/ree_util_internal.cc
    compute/kernels/scalar_cast_boolean.cc
    compute/kernels/scalar_cast_dictionary.cc
    compute/kernels/scalar_cast_extension.cc
    compute/kernels/scalar_cast_internal.cc
    compute/kernels/scalar_cast_nested.cc
    compute/kernels/scalar_cast_numeric.cc
    compute/kernels/scalar_cast_string.cc
    compute/kernels/scalar_cast_temporal.cc
    compute/kernels/util_internal.cc
    compute/kernels/vector_hash.cc
    compute/kernels/vector_selection.cc
    compute/kernels/vector_selection_filter_internal.cc
    compute/kernels/vector_selection_internal.cc
    compute/kernels/vector_selection_take_internal.cc)

if(ARROW_COMPUTE)
  # Include the remaining kernels
  list(APPEND
       ARROW_COMPUTE_SRCS
       compute/kernels/aggregate_basic.cc
       compute/kernels/aggregate_mode.cc
       compute/kernels/aggregate_pivot.cc
       compute/kernels/aggregate_quantile.cc
       compute/kernels/aggregate_tdigest.cc
       compute/kernels/aggregate_var_std.cc
       compute/kernels/hash_aggregate.cc
       compute/kernels/hash_aggregate_numeric.cc
       compute/kernels/hash_aggregate_pivot.cc
       compute/kernels/pivot_internal.cc
       compute/kernels/scalar_arithmetic.cc
       compute/kernels/scalar_boolean.cc
       compute/kernels/scalar_compare.cc
       compute/kernels/scalar_if_else.cc
       compute/kernels/scalar_nested.cc
       compute/kernels/scalar_random.cc
       compute/kernels/scalar_round.cc
       compute/kernels/scalar_set_lookup.cc
       compute/kernels/scalar_string_ascii.cc
       compute/kernels/scalar_string_utf8.cc
       compute/kernels/scalar_temporal_binary.cc
       compute/kernels/scalar_temporal_unary.cc
       compute/kernels/scalar_validity.cc
       compute/kernels/vector_array_sort.cc
       compute/kernels/vector_cumulative_ops.cc
       compute/kernels/vector_nested.cc
       compute/kernels/vector_pairwise.cc
       compute/kernels/vector_rank.cc
       compute/kernels/vector_replace.cc
       compute/kernels/vector_run_end_encode.cc
       compute/kernels/vector_select_k.cc
       compute/kernels/vector_sort.cc
       compute/kernels/vector_statistics.cc
       compute/kernels/vector_swizzle.cc
       compute/key_hash_internal.cc
       compute/key_map_internal.cc
       compute/light_array_internal.cc
       compute/row/encode_internal.cc
       compute/row/compare_internal.cc
       compute/row/grouper.cc
       compute/row/row_encoder_internal.cc
       compute/row/row_internal.cc
       compute/util.cc
       compute/util_internal.cc)

  append_runtime_avx2_src(ARROW_COMPUTE_SRCS compute/kernels/aggregate_basic_avx2.cc)
  append_runtime_avx512_src(ARROW_COMPUTE_SRCS compute/kernels/aggregate_basic_avx512.cc)
  append_runtime_avx2_src(ARROW_COMPUTE_SRCS compute/key_hash_internal_avx2.cc)
  append_runtime_avx2_bmi2_src(ARROW_COMPUTE_SRCS compute/key_map_internal_avx2.cc)
  append_runtime_avx2_src(ARROW_COMPUTE_SRCS compute/row/compare_internal_avx2.cc)
  append_runtime_avx2_src(ARROW_COMPUTE_SRCS compute/row/encode_internal_avx2.cc)
  append_runtime_avx2_bmi2_src(ARROW_COMPUTE_SRCS compute/util_avx2.cc)

endif()

arrow_add_object_library(ARROW_COMPUTE ${ARROW_COMPUTE_SRCS})
if(ARROW_USE_BOOST)
  foreach(ARROW_COMPUTE_TARGET ${ARROW_COMPUTE_TARGETS})
    target_link_libraries(${ARROW_COMPUTE_TARGET} PRIVATE Boost::headers)
  endforeach()
endif()
if(ARROW_USE_XSIMD)
  foreach(ARROW_COMPUTE_TARGET ${ARROW_COMPUTE_TARGETS})
    target_link_libraries(${ARROW_COMPUTE_TARGET} PRIVATE ${ARROW_XSIMD})
  endforeach()
endif()
if(ARROW_WITH_OPENTELEMETRY)
  foreach(ARROW_COMPUTE_TARGET ${ARROW_COMPUTE_TARGETS})
    target_link_libraries(${ARROW_COMPUTE_TARGET} PRIVATE ${ARROW_OPENTELEMETRY_LIBS})
  endforeach()
endif()
if(ARROW_WITH_RE2)
  foreach(ARROW_COMPUTE_TARGET ${ARROW_COMPUTE_TARGETS})
    target_link_libraries(${ARROW_COMPUTE_TARGET} PRIVATE re2::re2)
  endforeach()
endif()
if(ARROW_WITH_UTF8PROC)
  foreach(ARROW_COMPUTE_TARGET ${ARROW_COMPUTE_TARGETS})
    target_link_libraries(${ARROW_COMPUTE_TARGET} PRIVATE utf8proc::utf8proc)
  endforeach()
endif()

if(ARROW_FILESYSTEM)
  set(ARROW_FILESYSTEM_SRCS
      filesystem/filesystem.cc
      filesystem/localfs.cc
      filesystem/mockfs.cc
      filesystem/path_util.cc
      filesystem/util_internal.cc)

  if(ARROW_AZURE)
    list(APPEND ARROW_FILESYSTEM_SRCS filesystem/azurefs.cc)
    set_source_files_properties(filesystem/azurefs.cc
                                PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
  endif()
  if(ARROW_GCS)
    list(APPEND ARROW_FILESYSTEM_SRCS filesystem/gcsfs.cc filesystem/gcsfs_internal.cc)
    set_source_files_properties(filesystem/gcsfs.cc filesystem/gcsfs_internal.cc
                                PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
  endif()
  if(ARROW_HDFS)
    list(APPEND ARROW_FILESYSTEM_SRCS filesystem/hdfs.cc)
  endif()
  if(ARROW_S3)
    list(APPEND ARROW_FILESYSTEM_SRCS filesystem/s3fs.cc)
    set_source_files_properties(filesystem/s3fs.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION
                                                              ON)
  endif()

  arrow_add_object_library(ARROW_FILESYSTEM ${ARROW_FILESYSTEM_SRCS})
  if(ARROW_AZURE)
    foreach(ARROW_FILESYSTEM_TARGET ${ARROW_FILESYSTEM_TARGETS})
      target_link_libraries(${ARROW_FILESYSTEM_TARGET}
                            PRIVATE ${AZURE_SDK_LINK_LIBRARIES})
    endforeach()
  endif()
  if(ARROW_GCS)
    foreach(ARROW_FILESYSTEM_TARGET ${ARROW_FILESYSTEM_TARGETS})
      target_link_libraries(${ARROW_FILESYSTEM_TARGET} PRIVATE google-cloud-cpp::storage)
    endforeach()
  endif()
  if(ARROW_HDFS)
    foreach(ARROW_FILESYSTEM_TARGET ${ARROW_FILESYSTEM_TARGETS})
      target_link_libraries(${ARROW_FILESYSTEM_TARGET} PRIVATE arrow::hadoop)
    endforeach()
  endif()
  if(ARROW_S3)
    foreach(ARROW_FILESYSTEM_TARGET ${ARROW_FILESYSTEM_TARGETS})
      target_link_libraries(${ARROW_FILESYSTEM_TARGET} PRIVATE ${AWSSDK_LINK_LIBRARIES})
    endforeach()

    if(ARROW_S3_MODULE)
      if(NOT ARROW_BUILD_SHARED)
        message(FATAL_ERROR "ARROW_S3_MODULE without shared libarrow (-DARROW_BUILD_SHARED=ON) is not supported"
        )
      endif()

      add_library(arrow_s3fs MODULE filesystem/s3fs_module.cc filesystem/s3fs.cc)
      target_link_libraries(arrow_s3fs PRIVATE ${AWSSDK_LINK_LIBRARIES} arrow_shared)
      set_source_files_properties(filesystem/s3fs.cc filesystem/s3fs_module.cc
                                  PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
    endif()
  endif()

  list(APPEND ARROW_TESTING_SHARED_LINK_LIBS ${ARROW_GTEST_GMOCK})
  list(APPEND ARROW_TESTING_STATIC_LINK_LIBS ${ARROW_GTEST_GMOCK})
  list(APPEND ARROW_TESTING_SRCS filesystem/test_util.cc)
else()
  set(ARROW_FILESYSTEM_TARGET_SHARED)
  set(ARROW_FILESYSTEM_TARGET_STATIC)
endif()

if(ARROW_IPC)
  list(APPEND
       ARROW_IPC_SRCS
       ipc/dictionary.cc
       ipc/feather.cc
       ipc/message.cc
       ipc/metadata_internal.cc
       ipc/options.cc
       ipc/reader.cc
       ipc/writer.cc)
  arrow_add_object_library(ARROW_IPC ${ARROW_IPC_SRCS})
  foreach(ARROW_IPC_TARGET ${ARROW_IPC_TARGETS})
    target_link_libraries(${ARROW_IPC_TARGET} PRIVATE arrow::flatbuffers)
  endforeach()
else()
  set(ARROW_IPC_TARGET_SHARED)
  set(ARROW_IPC_TARGET_STATIC)
endif()

if(ARROW_JSON)
  arrow_add_object_library(ARROW_JSON
                           extension/fixed_shape_tensor.cc
                           extension/opaque.cc
                           json/options.cc
                           json/chunked_builder.cc
                           json/chunker.cc
                           json/converter.cc
                           json/from_string.cc
                           json/object_parser.cc
                           json/object_writer.cc
                           json/parser.cc
                           json/reader.cc)
  foreach(ARROW_JSON_TARGET ${ARROW_JSON_TARGETS})
    target_link_libraries(${ARROW_JSON_TARGET} PRIVATE RapidJSON)
  endforeach()
else()
  set(ARROW_JSON_TARGET_SHARED)
  set(ARROW_JSON_TARGET_STATIC)
endif()

if(ARROW_ORC)
  arrow_add_object_library(ARROW_ORC adapters/orc/adapter.cc adapters/orc/options.cc
                           adapters/orc/util.cc)
  foreach(ARROW_ORC_TARGET ${ARROW_ORC_TARGETS})
    target_link_libraries(${ARROW_ORC_TARGET} PRIVATE orc::orc)
    if(ARROW_ORC_VERSION VERSION_LESS "2.0.0")
      target_compile_definitions(${ARROW_ORC_TARGET}
                                 PRIVATE ARROW_ORC_NEED_TIME_ZONE_DATABASE_CHECK)
    endif()
  endforeach()
else()
  set(ARROW_ORC_TARGET_SHARED)
  set(ARROW_ORC_TARGET_STATIC)
endif()

if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
  set(ARROW_VERSION_SCRIPT_FLAGS
      "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
  set(ARROW_SHARED_LINK_FLAGS ${ARROW_VERSION_SCRIPT_FLAGS})
endif()

if(ARROW_BUILD_STATIC AND ARROW_BUNDLED_STATIC_LIBS)
  set(ARROW_BUILD_BUNDLED_DEPENDENCIES TRUE)
else()
  set(ARROW_BUILD_BUNDLED_DEPENDENCIES FALSE)
endif()

if(ARROW_BUILD_BUNDLED_DEPENDENCIES)
  arrow_car(_FIRST_LIB ${ARROW_BUNDLED_STATIC_LIBS})
  arrow_cdr(_OTHER_LIBS ${ARROW_BUNDLED_STATIC_LIBS})
  arrow_create_merged_static_lib(arrow_bundled_dependencies
                                 NAME
                                 arrow_bundled_dependencies
                                 ROOT
                                 ${_FIRST_LIB}
                                 TO_MERGE
                                 ${_OTHER_LIBS})
  # We can't use install(TARGETS) here because
  # arrow_bundled_dependencies is an IMPORTED library.
  get_target_property(arrow_bundled_dependencies_path arrow_bundled_dependencies
                      IMPORTED_LOCATION)
  install(FILES ${arrow_bundled_dependencies_path} ${INSTALL_IS_OPTIONAL}
          DESTINATION ${CMAKE_INSTALL_LIBDIR})
  string(PREPEND ARROW_PC_LIBS_PRIVATE " -larrow_bundled_dependencies")
  list(INSERT ARROW_STATIC_INSTALL_INTERFACE_LIBS 0 "Arrow::arrow_bundled_dependencies")
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(GLIBCXX_USE_CXX11_ABI_SOURCE
      ${CMAKE_CURRENT_BINARY_DIR}/try_compile_glibcxx_use_cxx_abi.cc)
  file(WRITE ${GLIBCXX_USE_CXX11_ABI_SOURCE}
       "#include <string>\n"
       "#if !_GLIBCXX_USE_CXX11_ABI\n"
       "#error Not using CXX11 ABI\n"
       "#endif\n"
       "int main(void) {return 0;}\n")
  try_compile(IS_GLIBCXX_USE_CXX11_ABI ${CMAKE_CURRENT_BINARY_DIR}/try_compile
              SOURCES ${GLIBCXX_USE_CXX11_ABI_SOURCE})
  if(NOT IS_GLIBCXX_USE_CXX11_ABI)
    string(APPEND ARROW_PC_CFLAGS " -D_GLIBCXX_USE_CXX11_ABI=0")
  endif()
endif()

# If libarrow.a is only built, "pkg-config --cflags --libs arrow"
# outputs build flags for static linking not shared
# linking. ARROW_PC_* except ARROW_PC_*_PRIVATE are for the static
# linking case.
if(NOT ARROW_BUILD_SHARED AND ARROW_BUILD_STATIC)
  string(APPEND ARROW_PC_CFLAGS "${ARROW_PC_CFLAGS_PRIVATE}")
  set(ARROW_PC_CFLAGS_PRIVATE "")
  set(ARROW_PC_LIBS "${ARROW_PC_LIBS_PRIVATE}")
  set(ARROW_PC_LIBS_PRIVATE "")
  set(ARROW_PC_REQUIRES "${ARROW_PC_REQUIRES_PRIVATE}")
  set(ARROW_PC_REQUIRES_PRIVATE "")

  string(APPEND ARROW_TESTING_PC_CFLAGS "${ARROW_TESTING_PC_CFLAGS_PRIVATE}")
  set(ARROW_TESTING_PC_CFLAGS_PRIVATE "")
else()
  set(ARROW_PC_LIBS "")
  set(ARROW_PC_REQUIRES "")
endif()

add_arrow_lib(arrow
              CMAKE_PACKAGE_NAME
              Arrow
              PKG_CONFIG_NAME
              arrow
              SOURCES
              ${ARROW_SRCS}
              OUTPUTS
              ARROW_LIBRARIES
              SHARED_LINK_FLAGS
              ${ARROW_SHARED_LINK_FLAGS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_ARRAY_TARGET_SHARED}
              ${ARROW_COMPUTE_TARGET_SHARED}
              ${ARROW_CSV_TARGET_SHARED}
              ${ARROW_FILESYSTEM_TARGET_SHARED}
              ${ARROW_INTEGRATION_TARGET_SHARED}
              ${ARROW_IO_TARGET_SHARED}
              ${ARROW_IPC_TARGET_SHARED}
              ${ARROW_JSON_TARGET_SHARED}
              ${ARROW_MEMORY_POOL_TARGET_SHARED}
              ${ARROW_ORC_TARGET_SHARED}
              ${ARROW_TELEMETRY_TARGET_SHARED}
              ${ARROW_UTIL_TARGET_SHARED}
              ${ARROW_VENDORED_TARGET_SHARED}
              ${ARROW_SHARED_PRIVATE_LINK_LIBS}
              ${ARROW_SYSTEM_LINK_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_ARRAY_TARGET_STATIC}
              ${ARROW_COMPUTE_TARGET_STATIC}
              ${ARROW_CSV_TARGET_STATIC}
              ${ARROW_FILESYSTEM_TARGET_STATIC}
              ${ARROW_INTEGRATION_TARGET_STATIC}
              ${ARROW_IO_TARGET_STATIC}
              ${ARROW_IPC_TARGET_STATIC}
              ${ARROW_JSON_TARGET_STATIC}
              ${ARROW_MEMORY_POOL_TARGET_STATIC}
              ${ARROW_ORC_TARGET_STATIC}
              ${ARROW_TELEMETRY_TARGET_STATIC}
              ${ARROW_UTIL_TARGET_STATIC}
              ${ARROW_VENDORED_TARGET_STATIC}
              ${ARROW_SYSTEM_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_STATIC_INSTALL_INTERFACE_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_SHARED_INSTALL_INTERFACE_LIBS})
# The following block includes a code for MSYS2 but GDB on MSYS2 doesn't
# auto load our plugin. GDB's auto load may not be supported on MSYS2...
# So it's disabled for now.
if(ARROW_BUILD_SHARED AND NOT WIN32)
  configure_file(libarrow_gdb.py.in "${CMAKE_CURRENT_BINARY_DIR}/libarrow_gdb.py" @ONLY)
  if(NOT ARROW_GDB_INSTALL_DIR)
    if(WIN32)
      set(ARROW_GDB_INSTALL_DIR ${CMAKE_INSTALL_FULL_BINDIR})
    else()
      set(ARROW_GDB_INSTALL_DIR ${CMAKE_INSTALL_FULL_LIBDIR})
    endif()
  endif()
  set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_INSTALL FALSE)
  if(WIN32)
    find_program(cygpath "cygpath")
    if(cygpath)
      execute_process(COMMAND cygpath "${ARROW_GDB_INSTALL_DIR}"
                      OUTPUT_VARIABLE MSYS2_ARROW_GDB_INSTALL_DIR
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
      set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_DIR
          "${ARROW_GDB_AUTO_LOAD_DIR}${MSYS2_ARROW_GDB_INSTALL_DIR}")
      set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_INSTALL TRUE)
    endif()
  else()
    set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_DIR
        "${ARROW_GDB_AUTO_LOAD_DIR}/${ARROW_GDB_INSTALL_DIR}")
    set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_INSTALL TRUE)
  endif()
  if(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_INSTALL)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/libarrow_gdb.py"
            DESTINATION "${ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_DIR}"
            RENAME "$<TARGET_FILE_NAME:arrow_shared>-gdb.py")
  endif()
endif()

add_dependencies(arrow ${ARROW_LIBRARIES})

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_static PUBLIC ARROW_STATIC)
endif()

foreach(LIB_TARGET ${ARROW_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_EXPORTING)
  # C++17 is required to compile against Arrow C++ headers and libraries
  target_compile_features(${LIB_TARGET} PUBLIC cxx_std_17)
endforeach()

if(ARROW_WITH_BACKTRACE)
  find_package(Backtrace)

  foreach(LIB_TARGET ${ARROW_LIBRARIES})
    if(Backtrace_FOUND AND ARROW_WITH_BACKTRACE)
      target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_WITH_BACKTRACE)
    endif()
  endforeach()
endif()

if(ARROW_TESTING)
  add_arrow_lib(arrow_testing
                CMAKE_PACKAGE_NAME
                ArrowTesting
                PKG_CONFIG_NAME
                arrow-testing
                SOURCES
                ${ARROW_TESTING_SRCS}
                OUTPUTS
                ARROW_TESTING_LIBRARIES
                SHARED_LINK_LIBS
                ${ARROW_TESTING_SHARED_LINK_LIBS}
                SHARED_PRIVATE_LINK_LIBS
                ${ARROW_TESTING_SHARED_PRIVATE_LINK_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ARROW_TESTING_SHARED_INSTALL_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ARROW_TESTING_STATIC_LINK_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ARROW_TESTING_STATIC_INSTALL_INTERFACE_LIBS})

  add_custom_target(arrow_testing)
  add_dependencies(arrow_testing ${ARROW_TESTING_LIBRARIES})

  if(ARROW_BUILD_STATIC AND WIN32)
    target_compile_definitions(arrow_testing_static PUBLIC ARROW_TESTING_STATIC)
  endif()

  foreach(LIB_TARGET ${ARROW_TESTING_LIBRARIES})
    target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_TESTING_EXPORTING)
  endforeach()
endif()

arrow_install_all_headers("arrow")

config_summary_cmake_setters("${CMAKE_CURRENT_BINARY_DIR}/ArrowOptions.cmake")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/ArrowOptions.cmake
        DESTINATION "${ARROW_CMAKE_DIR}/Arrow")

# For backward compatibility for find_package(arrow)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/arrow-config.cmake
        DESTINATION "${ARROW_CMAKE_DIR}/Arrow")

#
# Unit tests
#
add_arrow_test(array_test
               SOURCES
               array/array_test.cc
               array/array_binary_test.cc
               array/array_dict_test.cc
               array/array_list_test.cc
               array/array_list_view_test.cc
               array/array_run_end_test.cc
               array/array_struct_test.cc
               array/array_union_test.cc
               array/array_view_test.cc
               array/statistics_test.cc)

add_arrow_test(buffer_test)

if(ARROW_IPC)
  # The extension type unit tests require IPC / Flatbuffers support
  add_arrow_test(extension_type_test)
endif()

add_arrow_test(misc_test
               SOURCES
               datum_test.cc
               memory_pool_test.cc
               result_test.cc
               pretty_print_test.cc
               status_test.cc)

add_arrow_test(public_api_test)
set_source_files_properties(public_api_test.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)

add_arrow_test(scalar_test)
add_arrow_test(type_test SOURCES field_ref_test.cc type_test.cc)

add_arrow_test(table_test
               SOURCES
               chunked_array_test.cc
               record_batch_test.cc
               table_test.cc
               table_builder_test.cc)

add_arrow_test(tensor_test)
add_arrow_test(sparse_tensor_test)

add_arrow_test(stl_test SOURCES stl_iterator_test.cc stl_test.cc)

add_arrow_benchmark(builder_benchmark)
add_arrow_benchmark(chunk_resolver_benchmark)
add_arrow_benchmark(compare_benchmark)
add_arrow_benchmark(memory_pool_benchmark)
add_arrow_benchmark(type_benchmark)
add_arrow_benchmark(tensor_benchmark)

#
# Recurse into sub-directories
#

# Unconditionally install testing headers that are also useful for Arrow consumers.
add_subdirectory(testing)

add_subdirectory(array)
add_subdirectory(c)
add_subdirectory(compute)
add_subdirectory(extension)
add_subdirectory(io)
add_subdirectory(tensor)
add_subdirectory(util)
add_subdirectory(vendored)

if(ARROW_BUILD_INTEGRATION OR ARROW_BUILD_TESTS)
  # We build tests for the JSON integration machinery even if integration
  # is not enabled, to ensure it's exercised in more builds than just the
  # integration build.
  add_subdirectory(integration)
endif()

if(ARROW_CSV)
  add_subdirectory(csv)
endif()

if(ARROW_ACERO)
  add_subdirectory(acero)
endif()

if(ARROW_CUDA)
  add_subdirectory(gpu)
endif()

if(ARROW_DATASET)
  add_subdirectory(dataset)
endif()

if(ARROW_FILESYSTEM)
  add_subdirectory(filesystem)
endif()

if(ARROW_FLIGHT)
  add_subdirectory(flight)
endif()

if(ARROW_IPC)
  add_subdirectory(ipc)
endif()

if(ARROW_JSON)
  add_subdirectory(json)
endif()

if(ARROW_ORC)
  add_subdirectory(adapters/orc)
endif()

if(ARROW_SUBSTRAIT)
  add_subdirectory(engine)
endif()

if(ARROW_WITH_OPENTELEMETRY)
  add_subdirectory(telemetry)
endif()

if(ARROW_TENSORFLOW)
  add_subdirectory(adapters/tensorflow)
endif()
