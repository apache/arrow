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

# Adding unit tests part of the "arrow" portion of the test suite
function(ADD_ARROW_TEST REL_TEST_NAME)
  set(options)
  set(one_value_args PREFIX)
  set(multi_value_args LABELS PRECOMPILED_HEADERS)
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

  # Because of https://gitlab.kitware.com/cmake/cmake/issues/20289,
  # we must generate the precompiled header on an executable target.
  # Do that on the first unit test target (here "arrow-array-test")
  # and reuse the PCH for the other tests.
  if(ARG_PRECOMPILED_HEADERS)
    set(PCH_ARGS PRECOMPILED_HEADERS ${ARG_PRECOMPILED_HEADERS})
  else()
    set(PCH_ARGS PRECOMPILED_HEADER_LIB "arrow-array-test")
  endif()

  add_test_case(${REL_TEST_NAME}
                PREFIX
                ${PREFIX}
                LABELS
                ${LABELS}
                ${PCH_ARGS}
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

macro(append_avx2_src SRC)
  if(ARROW_HAVE_RUNTIME_AVX2)
    list(APPEND ARROW_SRCS ${SRC})
    set_source_files_properties(${SRC} PROPERTIES SKIP_PRECOMPILE_HEADERS ON)
    set_source_files_properties(${SRC} PROPERTIES COMPILE_FLAGS ${ARROW_AVX2_FLAG})
  endif()
endmacro()

macro(append_avx512_src SRC)
  if(ARROW_HAVE_RUNTIME_AVX512)
    list(APPEND ARROW_SRCS ${SRC})
    set_source_files_properties(${SRC} PROPERTIES SKIP_PRECOMPILE_HEADERS ON)
    set_source_files_properties(${SRC} PROPERTIES COMPILE_FLAGS ${ARROW_AVX512_FLAG})
  endif()
endmacro()

set(ARROW_SRCS
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
    array/util.cc
    array/validate.cc
    builder.cc
    buffer.cc
    chunked_array.cc
    chunk_resolver.cc
    compare.cc
    config.cc
    datum.cc
    device.cc
    extension_type.cc
    memory_pool.cc
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
    visitor.cc
    c/bridge.cc
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
    io/transform.cc
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
    util/formatting.cc
    util/future.cc
    util/int_util.cc
    util/io_util.cc
    util/logging.cc
    util/key_value_metadata.cc
    util/memory.cc
    util/mutex.cc
    util/ree_util.cc
    util/string.cc
    util/string_builder.cc
    util/task_group.cc
    util/tdigest.cc
    util/thread_pool.cc
    util/time.cc
    util/tracing.cc
    util/trie.cc
    util/unreachable.cc
    util/uri.cc
    util/utf8.cc
    util/value_parsing.cc
    vendored/base64.cpp
    vendored/datetime/tz.cpp
    vendored/double-conversion/bignum.cc
    vendored/double-conversion/double-conversion.cc
    vendored/double-conversion/bignum-dtoa.cc
    vendored/double-conversion/fast-dtoa.cc
    vendored/double-conversion/cached-powers.cc
    vendored/double-conversion/fixed-dtoa.cc
    vendored/double-conversion/diy-fp.cc
    vendored/double-conversion/strtod.cc)

if(ARROW_JEMALLOC)
  list(APPEND ARROW_SRCS memory_pool_jemalloc.cc)
  set_source_files_properties(memory_pool_jemalloc.cc
                              PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
endif()

append_avx2_src(util/bpacking_avx2.cc)
append_avx512_src(util/bpacking_avx512.cc)

if(ARROW_HAVE_NEON)
  list(APPEND ARROW_SRCS util/bpacking_neon.cc)
endif()

if(APPLE)
  list(APPEND ARROW_SRCS vendored/datetime/ios.mm)
endif()

set(ARROW_C_SRCS
    vendored/musl/strptime.c
    vendored/uriparser/UriCommon.c
    vendored/uriparser/UriCompare.c
    vendored/uriparser/UriEscape.c
    vendored/uriparser/UriFile.c
    vendored/uriparser/UriIp4Base.c
    vendored/uriparser/UriIp4.c
    vendored/uriparser/UriMemory.c
    vendored/uriparser/UriNormalizeBase.c
    vendored/uriparser/UriNormalize.c
    vendored/uriparser/UriParseBase.c
    vendored/uriparser/UriParse.c
    vendored/uriparser/UriQuery.c
    vendored/uriparser/UriRecompose.c
    vendored/uriparser/UriResolve.c
    vendored/uriparser/UriShorten.c)

set_source_files_properties(vendored/datetime/tz.cpp
                            PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                       SKIP_UNITY_BUILD_INCLUSION ON)

# Disable DLL exports in vendored uriparser library
add_definitions(-DURI_STATIC_BUILD)

if(ARROW_WITH_BROTLI)
  add_definitions(-DARROW_WITH_BROTLI)
  list(APPEND ARROW_SRCS util/compression_brotli.cc)
endif()

if(ARROW_WITH_BZ2)
  add_definitions(-DARROW_WITH_BZ2)
  list(APPEND ARROW_SRCS util/compression_bz2.cc)
endif()

if(ARROW_WITH_LZ4)
  add_definitions(-DARROW_WITH_LZ4)
  list(APPEND ARROW_SRCS util/compression_lz4.cc)
endif()

if(ARROW_WITH_OPENTELEMETRY)
  list(APPEND ARROW_SRCS util/tracing_internal.cc)
endif()

if(ARROW_WITH_SNAPPY)
  add_definitions(-DARROW_WITH_SNAPPY)
  list(APPEND ARROW_SRCS util/compression_snappy.cc)
endif()

if(ARROW_WITH_ZLIB)
  add_definitions(-DARROW_WITH_ZLIB)
  list(APPEND ARROW_SRCS util/compression_zlib.cc)
endif()

if(ARROW_WITH_ZSTD)
  add_definitions(-DARROW_WITH_ZSTD)
  list(APPEND ARROW_SRCS util/compression_zstd.cc)
endif()

set(ARROW_TESTING_SRCS
    io/test_common.cc
    ipc/test_common.cc
    testing/json_integration.cc
    testing/json_internal.cc
    testing/gtest_util.cc
    testing/random.cc
    testing/generator.cc
    testing/util.cc)

# Add dependencies for third-party allocators.
# If possible we only want memory_pool.cc to wait for allocators to finish building,
# but that only works with Ninja
# (see https://gitlab.kitware.com/cmake/cmake/issues/19677)

set(_allocator_dependencies "") # Empty list
if(jemalloc_VENDORED)
  list(APPEND _allocator_dependencies jemalloc_ep)
endif()
if(mimalloc_VENDORED)
  list(APPEND _allocator_dependencies mimalloc_ep)
endif()

if(_allocator_dependencies)
  if("${CMAKE_GENERATOR}" STREQUAL "Ninja")
    set_source_files_properties(memory_pool.cc PROPERTIES OBJECT_DEPENDS
                                                          "${_allocator_dependencies}")
  else()
    add_dependencies(arrow_dependencies ${_allocator_dependencies})
  endif()
  set_source_files_properties(memory_pool.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                                        SKIP_UNITY_BUILD_INCLUSION ON)
endif()

unset(_allocator_dependencies)

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_property(SOURCE util/io_util.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-unused-macros ")
endif()

#
# Configure the base Arrow libraries
#

if(ARROW_CSV)
  list(APPEND
       ARROW_SRCS
       csv/converter.cc
       csv/chunker.cc
       csv/column_builder.cc
       csv/column_decoder.cc
       csv/options.cc
       csv/parser.cc
       csv/reader.cc
       csv/writer.cc)

  list(APPEND ARROW_TESTING_SRCS csv/test_common.cc)
endif()

# Baseline Compute functionality + scalar casts and a few select kernels
list(APPEND
     ARROW_SRCS
     compute/api_aggregate.cc
     compute/api_scalar.cc
     compute/api_vector.cc
     compute/cast.cc
     compute/exec.cc
     compute/exec/groupby.cc
     compute/exec/accumulation_queue.cc
     compute/exec/aggregate_node.cc
     compute/exec/asof_join_node.cc
     compute/exec/bloom_filter.cc
     compute/exec/exec_plan.cc
     compute/expression.cc
     compute/exec/fetch_node.cc
     compute/exec/filter_node.cc
     compute/exec/hash_join.cc
     compute/exec/hash_join_dict.cc
     compute/exec/hash_join_node.cc
     compute/exec/map_node.cc
     compute/exec/options.cc
     compute/exec/order_by_impl.cc
     compute/exec/partition_util.cc
     compute/exec/pivot_longer_node.cc
     compute/exec/project_node.cc
     compute/exec/query_context.cc
     compute/exec/sink_node.cc
     compute/exec/source_node.cc
     compute/exec/swiss_join.cc
     compute/exec/task_util.cc
     compute/exec/tpch_node.cc
     compute/exec/union_node.cc
     compute/exec/util.cc
     compute/function.cc
     compute/function_internal.cc
     compute/kernel.cc
     compute/key_hash.cc
     compute/key_map.cc
     compute/light_array.cc
     compute/ordering.cc
     compute/registry.cc
     compute/kernels/codegen_internal.cc
     compute/kernels/row_encoder.cc
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
     compute/row/encode_internal.cc
     compute/row/compare_internal.cc
     compute/row/grouper.cc
     compute/row/row_internal.cc
     compute/util.cc)

append_avx2_src(compute/exec/bloom_filter_avx2.cc)
append_avx2_src(compute/key_hash_avx2.cc)
append_avx2_src(compute/key_map_avx2.cc)
append_avx2_src(compute/exec/swiss_join_avx2.cc)
append_avx2_src(compute/exec/util_avx2.cc)
append_avx2_src(compute/row/compare_internal_avx2.cc)
append_avx2_src(compute/row/encode_internal_avx2.cc)

list(APPEND ARROW_TESTING_SRCS compute/exec/test_util.cc)

if(ARROW_COMPUTE)
  # Include the remaining kernels
  list(APPEND
       ARROW_SRCS
       compute/kernels/aggregate_basic.cc
       compute/kernels/aggregate_mode.cc
       compute/kernels/aggregate_quantile.cc
       compute/kernels/aggregate_tdigest.cc
       compute/kernels/aggregate_var_std.cc
       compute/kernels/hash_aggregate.cc
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
       compute/kernels/vector_rank.cc
       compute/kernels/vector_replace.cc
       compute/kernels/vector_run_end_encode.cc
       compute/kernels/vector_select_k.cc
       compute/kernels/vector_sort.cc)

  append_avx2_src(compute/kernels/aggregate_basic_avx2.cc)
  append_avx512_src(compute/kernels/aggregate_basic_avx512.cc)
endif()

if(ARROW_FILESYSTEM)
  if(ARROW_HDFS)
    add_definitions(-DARROW_HDFS)
  endif()

  list(APPEND
       ARROW_SRCS
       filesystem/filesystem.cc
       filesystem/localfs.cc
       filesystem/mockfs.cc
       filesystem/path_util.cc
       filesystem/util_internal.cc)

  if(ARROW_GCS)
    list(APPEND ARROW_SRCS filesystem/gcsfs.cc filesystem/gcsfs_internal.cc)
    set_source_files_properties(filesystem/gcsfs.cc filesystem/gcsfs_internal.cc
                                PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                           SKIP_UNITY_BUILD_INCLUSION ON)
  endif()
  if(ARROW_HDFS)
    list(APPEND ARROW_SRCS filesystem/hdfs.cc)
  endif()
  if(ARROW_S3)
    try_compile(S3_HAS_CRT ${CMAKE_CURRENT_BINARY_DIR}/try_compile
                SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/filesystem/try_compile/check_s3fs_crt.cc"
                CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${CURRENT_INCLUDE_DIRECTORIES}"
                LINK_LIBRARIES ${AWSSDK_LINK_LIBRARIES} CXX_STANDARD 17)

    if(S3_HAS_CRT)
      message(STATUS "AWS SDK is new enough to have CRT support")
      add_definitions(-DARROW_S3_HAS_CRT)
    endif()

    list(APPEND ARROW_SRCS filesystem/s3fs.cc)
    set_source_files_properties(filesystem/s3fs.cc
                                PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                           SKIP_UNITY_BUILD_INCLUSION ON)
  endif()

  list(APPEND ARROW_TESTING_SRCS filesystem/test_util.cc)
endif()

if(ARROW_IPC)
  list(APPEND
       ARROW_SRCS
       ipc/dictionary.cc
       ipc/feather.cc
       ipc/message.cc
       ipc/metadata_internal.cc
       ipc/options.cc
       ipc/reader.cc
       ipc/writer.cc)

  if(ARROW_JSON)
    list(APPEND ARROW_SRCS ipc/json_simple.cc)
  endif()
endif()

if(ARROW_JSON)
  list(APPEND
       ARROW_SRCS
       json/options.cc
       json/chunked_builder.cc
       json/chunker.cc
       json/converter.cc
       json/object_parser.cc
       json/object_writer.cc
       json/parser.cc
       json/reader.cc)
endif()

if(ARROW_ORC)
  list(APPEND
       ARROW_SRCS
       adapters/orc/adapter.cc
       adapters/orc/options.cc
       adapters/orc/util.cc)
endif()

if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
  set(ARROW_VERSION_SCRIPT_FLAGS
      "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
  set(ARROW_SHARED_LINK_FLAGS ${ARROW_VERSION_SCRIPT_FLAGS})
endif()

set(ARROW_ALL_SRCS ${ARROW_SRCS} ${ARROW_C_SRCS})

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
  string(APPEND ARROW_PC_LIBS_PRIVATE " -larrow_bundled_dependencies")
  list(INSERT ARROW_STATIC_INSTALL_INTERFACE_LIBS 0 "Arrow::arrow_bundled_dependencies")
endif()
# Need -latomic on Raspbian.
# See also: https://issues.apache.org/jira/browse/ARROW-12860
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux" AND ${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv7")
  string(APPEND ARROW_PC_LIBS_PRIVATE " -latomic")
  list(APPEND ARROW_SHARED_INSTALL_INTERFACE_LIBS "atomic")
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS "atomic")
endif()

# If libarrow.a is only built, "pkg-config --cflags --libs arrow"
# outputs build flags for static linking not shared
# linking. ARROW_PC_* except ARROW_PC_*_PRIVATE are for the static
# linking case.
if(NOT ARROW_BUILD_SHARED AND ARROW_BUILD_STATIC)
  set(ARROW_PC_CFLAGS "${ARROW_PC_CFLAGS_PRIVATE}")
  set(ARROW_PC_CFLAGS_PRIVATE "")
  set(ARROW_PC_LIBS "${ARROW_PC_LIBS_PRIVATE}")
  set(ARROW_PC_LIBS_PRIVATE "")
  set(ARROW_PC_REQUIRES "${ARROW_PC_REQUIRES_PRIVATE}")
  set(ARROW_PC_REQUIRES_PRIVATE "")

  string(APPEND ARROW_TESTING_PC_CFLAGS "${ARROW_TESTING_PC_CFLAGS_PRIVATE}")
  set(ARROW_TESTING_PC_CFLAGS_PRIVATE "")
else()
  set(ARROW_PC_CFLAGS "")
  set(ARROW_PC_LIBS "")
  set(ARROW_PC_REQUIRES "")
endif()

add_arrow_lib(arrow
              CMAKE_PACKAGE_NAME
              Arrow
              PKG_CONFIG_NAME
              arrow
              SOURCES
              ${ARROW_ALL_SRCS}
              OUTPUTS
              ARROW_LIBRARIES
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/pch.h>"
              DEPENDENCIES
              arrow_dependencies
              SHARED_LINK_FLAGS
              ${ARROW_SHARED_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${ARROW_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_SHARED_PRIVATE_LINK_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_STATIC_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_STATIC_INSTALL_INTERFACE_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_SHARED_INSTALL_INTERFACE_LIBS})
# The following block includes a code for MSYS2 but GDB on MSYS2 doesn't
# auto load our plugin. GDB's auto load may not be supported on MSYS2...
# So it's disabled for now.
if(ARROW_BUILD_SHARED AND NOT WIN32)
  configure_file(libarrow_gdb.py.in "${CMAKE_CURRENT_BINARY_DIR}/libarrow_gdb.py" @ONLY)
  set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_INSTALL FALSE)
  if(WIN32)
    find_program(cygpath "cygpath")
    if(cygpath)
      execute_process(COMMAND cygpath "${CMAKE_INSTALL_FULL_BINDIR}"
                      OUTPUT_VARIABLE MSYS2_CMAKE_INSTALL_FULL_BINDIR
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
      set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_DIR
          "${ARROW_GDB_AUTO_LOAD_DIR}${MSYS2_CMAKE_INSTALL_FULL_BINDIR}")
      set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_INSTALL TRUE)
    endif()
  else()
    set(ARROW_GDB_AUTO_LOAD_LIBARROW_GDB_DIR
        "${ARROW_GDB_AUTO_LOAD_DIR}/${CMAKE_INSTALL_FULL_LIBDIR}")
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
  # that depend on gtest
  add_arrow_lib(arrow_testing
                CMAKE_PACKAGE_NAME
                ArrowTesting
                PKG_CONFIG_NAME
                arrow-testing
                SOURCES
                ${ARROW_TESTING_SRCS}
                OUTPUTS
                ARROW_TESTING_LIBRARIES
                PRECOMPILED_HEADERS
                "$<$<COMPILE_LANGUAGE:CXX>:arrow/pch.h>"
                DEPENDENCIES
                arrow_test_dependencies
                SHARED_LINK_LIBS
                arrow::flatbuffers
                rapidjson::rapidjson
                arrow_shared
                GTest::gtest
                SHARED_INSTALL_INTERFACE_LIBS
                Arrow::arrow_shared
                GTest::gtest
                STATIC_LINK_LIBS
                arrow::flatbuffers
                rapidjson::rapidjson
                arrow_static
                GTest::gtest
                STATIC_INSTALL_INTERFACE_LIBS
                Arrow::arrow_static
                GTest::gtest)

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
               array/array_run_end_test.cc
               array/array_struct_test.cc
               array/array_union_test.cc
               array/array_view_test.cc
               PRECOMPILED_HEADERS
               "$<$<COMPILE_LANGUAGE:CXX>:arrow/testing/pch.h>")

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

set_source_files_properties(public_api_test.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                                          SKIP_UNITY_BUILD_INCLUSION ON)

add_arrow_test(scalar_test)
add_arrow_test(type_test)

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
add_arrow_benchmark(compare_benchmark)
add_arrow_benchmark(memory_pool_benchmark)
add_arrow_benchmark(type_benchmark)

#
# Recurse into sub-directories
#

# Unconditionally install testing headers that are also useful for Arrow consumers.
add_subdirectory(testing)

add_subdirectory(array)
add_subdirectory(c)
add_subdirectory(compute)
add_subdirectory(io)
add_subdirectory(tensor)
add_subdirectory(util)
add_subdirectory(vendored)

if(ARROW_CSV)
  add_subdirectory(csv)
endif()

if(ARROW_SUBSTRAIT)
  add_subdirectory(engine)
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

if(ARROW_TENSORFLOW)
  add_subdirectory(adapters/tensorflow)
endif()
