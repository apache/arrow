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

add_custom_target(arrow_flight_sql)

arrow_install_all_headers("arrow/flight/flight-sql")

set(FLIGHT_SQL_PROTO_PATH "${ARROW_SOURCE_DIR}/../format")
set(FLIGHT_SQL_PROTO ${ARROW_SOURCE_DIR}/../format/FlightSql.proto)

set(FLIGHT_SQL_GENERATED_PROTO_FILES
    "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.pb.h"
    "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.grpc.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.grpc.pb.h")

set(PROTO_DEPENDS ${FLIGHT_SQL_PROTO} ${ARROW_PROTOBUF_LIBPROTOBUF} gRPC::grpc_cpp_plugin)

add_custom_command(OUTPUT ${FLIGHT_SQL_GENERATED_PROTO_FILES}
                   COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_SQL_PROTO_PATH}"
                           "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${FLIGHT_SQL_PROTO}"
                   DEPENDS ${PROTO_DEPENDS} ARGS
                   COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_SQL_PROTO_PATH}"
                           "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
                           "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                           "${FLIGHT_SQL_PROTO}")

set_source_files_properties(${FLIGHT_SQL_GENERATED_PROTO_FILES} PROPERTIES GENERATED TRUE)

add_custom_target(flight_sql_grpc_gen ALL DEPENDS ${FLIGHT_SQL_GENERATED_PROTO_FILES})

# <KLUDGE> -Werror / /WX cause try_compile to fail because there seems to be no
# way to pass -isystem $GRPC_INCLUDE_DIR instead of -I$GRPC_INCLUDE_DIR
set(CMAKE_CXX_FLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
string(REPLACE "/WX" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-Werror " " " CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

# Probe the version of gRPC being used to see if it supports disabling server
# verification when using TLS.
function(test_grpc_version DST_VAR DETECT_VERSION TEST_FILE)
  if(NOT DEFINED ${DST_VAR})
    message(STATUS "Checking support for TlsCredentialsOptions (gRPC >= ${DETECT_VERSION})..."
    )
    get_property(CURRENT_INCLUDE_DIRECTORIES
                 DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                 PROPERTY INCLUDE_DIRECTORIES)
    try_compile(HAS_GRPC_VERSION ${CMAKE_CURRENT_BINARY_DIR}/try_compile
                SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/try_compile/${TEST_FILE}"
                CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${CURRENT_INCLUDE_DIRECTORIES}"
                LINK_LIBRARIES gRPC::grpc++
                OUTPUT_VARIABLE TLS_CREDENTIALS_OPTIONS_CHECK_OUTPUT CXX_STANDARD 11)
    if(HAS_GRPC_VERSION)
      set(${DST_VAR}
          "${DETECT_VERSION}"
          CACHE INTERNAL "The detected (approximate) gRPC version.")
    else()
      message(STATUS "TlsCredentialsOptions (for gRPC ${DETECT_VERSION}) not found in grpc::experimental."
      )
      message(DEBUG "Build output:")
      list(APPEND CMAKE_MESSAGE_INDENT "${TEST_FILE}: ")
      message(DEBUG ${TLS_CREDENTIALS_OPTIONS_CHECK_OUTPUT})
      list(REMOVE_AT CMAKE_MESSAGE_INDENT -1)
    endif()
  endif()
endfunction()

# Note, we do not compile the generated Protobuf sources directly, instead
# compiling then via protocol_internal.cc which contains some gRPC template
# overrides to enable Flight-specific optimizations. See comments in
# protobuf-internal.cc
set(ARROW_FLIGHT_SQL_SRCS
    client.cc)

add_arrow_lib(arrow_flight_sql
              CMAKE_PACKAGE_NAME
              ArrowFlightSql
              PKG_CONFIG_NAME
              arrow-flight-sql
              OUTPUTS
              ARROW_FLIGHT_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_SQL_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/flight/pch.h>"
              DEPENDENCIES
              flight_sql_grpc_gen
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_shared
              ${ARROW_FLIGHT_LINK_LIBS}
              STATIC_LINK_LIBS
              arrow_static
              ${ARROW_FLIGHT_LINK_LIBS})

add_arrow_test(flight_sql_test
               SOURCES
               client_test.cc
               LABELS
               "arrow_flight_sql")

## Build test server for unit tests or benchmarks
#if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
#  add_executable(flight-test-server test_server.cc)
#  target_link_libraries(flight-test-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
#                        ${GFLAGS_LIBRARIES} GTest::gtest)
#
#  if(ARROW_BUILD_TESTS)
#    add_dependencies(arrow-flight-test flight-test-server)
#  endif()
#
#  add_dependencies(arrow_flight flight-test-server)
#endif()
#
#if(ARROW_BUILD_INTEGRATION)
#  add_executable(flight-test-integration-server test_integration_server.cc)
#  target_link_libraries(flight-test-integration-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
#                        ${GFLAGS_LIBRARIES} GTest::gtest)
#
#  add_executable(flight-test-integration-client test_integration_client.cc)
#  target_link_libraries(flight-test-integration-client ${ARROW_FLIGHT_TEST_LINK_LIBS}
#                        ${GFLAGS_LIBRARIES} GTest::gtest)
#
#  add_dependencies(arrow_flight flight-test-integration-client
#                   flight-test-integration-server)
#  add_dependencies(arrow-integration flight-test-integration-client
#                   flight-test-integration-server)
#endif()
#
#if(ARROW_BUILD_BENCHMARKS)
#  # Perf server for benchmarks
#  set(PERF_PROTO_GENERATED_FILES "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.cc"
#                                 "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.h")
#
#  add_custom_command(OUTPUT ${PERF_PROTO_GENERATED_FILES}
#                     COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${CMAKE_CURRENT_SOURCE_DIR}"
#                             "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "perf.proto"
#                     DEPENDS ${PROTO_DEPENDS})
#
#  add_executable(arrow-flight-perf-server perf_server.cc perf.pb.cc)
#  target_link_libraries(arrow-flight-perf-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
#                        ${GFLAGS_LIBRARIES} GTest::gtest)
#
#  add_executable(arrow-flight-benchmark flight_benchmark.cc perf.pb.cc)
#  target_link_libraries(arrow-flight-benchmark ${ARROW_FLIGHT_TEST_LINK_LIBS}
#                        ${GFLAGS_LIBRARIES} GTest::gtest)
#
#  add_dependencies(arrow-flight-benchmark arrow-flight-perf-server)
#
#  add_dependencies(arrow_flight arrow-flight-benchmark)
#endif(ARROW_BUILD_BENCHMARKS)
