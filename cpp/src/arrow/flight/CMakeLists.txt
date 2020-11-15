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

add_custom_target(arrow_flight)

arrow_install_all_headers("arrow/flight")

set(ARROW_FLIGHT_STATIC_LINK_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF} gRPC::grpc++ c-ares::cares
                                  ZLIB::ZLIB)

if(WIN32)
  list(APPEND ARROW_FLIGHT_STATIC_LINK_LIBS ws2_32.lib)
endif()

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_FLIGHT_TEST_LINK_LIBS arrow_flight_static arrow_flight_testing_static
                                  ${ARROW_FLIGHT_STATIC_LINK_LIBS}
                                  ${ARROW_TEST_LINK_LIBS})
else()
  set(ARROW_FLIGHT_TEST_LINK_LIBS arrow_flight_shared arrow_flight_testing_shared
                                  ${ARROW_TEST_LINK_LIBS})
endif()

# TODO(wesm): Protobuf shared vs static linking

set(FLIGHT_PROTO_PATH "${ARROW_SOURCE_DIR}/../format")
set(FLIGHT_PROTO ${ARROW_SOURCE_DIR}/../format/Flight.proto)

set(FLIGHT_GENERATED_PROTO_FILES "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.cc"
                                 "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.h"
                                 "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.cc"
                                 "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.h")

set(PROTO_DEPENDS ${FLIGHT_PROTO} ${ARROW_PROTOBUF_LIBPROTOBUF} gRPC::grpc_cpp_plugin)

add_custom_command(OUTPUT ${FLIGHT_GENERATED_PROTO_FILES}
                   COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_PROTO_PATH}"
                           "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${FLIGHT_PROTO}"
                   DEPENDS ${PROTO_DEPENDS} ARGS
                   COMMAND ${ARROW_PROTOBUF_PROTOC}
                           "-I${FLIGHT_PROTO_PATH}"
                           "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
                           "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                           "${FLIGHT_PROTO}")

set_source_files_properties(${FLIGHT_GENERATED_PROTO_FILES} PROPERTIES GENERATED TRUE)

add_custom_target(flight_grpc_gen ALL DEPENDS ${FLIGHT_GENERATED_PROTO_FILES})

# <KLUDGE> -Werror / /WX cause try_compile to fail because there seems to be no
# way to pass -isystem $GRPC_INCLUDE_DIR instead of -I$GRPC_INCLUDE_DIR
set(CMAKE_CXX_FLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
string(REPLACE "/WX" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-Werror " " " CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

# Probe the version of gRPC being used to see if it supports disabling server
# verification when using TLS.
if(NOT DEFINED HAS_GRPC_132)
  message(STATUS "Checking support for TlsCredentialsOptions...")
  get_property(CURRENT_INCLUDE_DIRECTORIES
               DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
               PROPERTY INCLUDE_DIRECTORIES)
  try_compile(HAS_GRPC_132 ${CMAKE_CURRENT_BINARY_DIR}/try_compile SOURCES
              "${CMAKE_CURRENT_SOURCE_DIR}/try_compile/check_tls_opts_132.cc"
              CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${CURRENT_INCLUDE_DIRECTORIES}"
              LINK_LIBRARIES gRPC::grpc
              OUTPUT_VARIABLE TSL_CREDENTIALS_OPTIONS_CHECK_OUTPUT CXX_STANDARD 11)

  if(HAS_GRPC_132)
    message(STATUS "TlsCredentialsOptions found in grpc::experimental.")
    add_definitions(-DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc::experimental)
  else()
    message(STATUS "TlsCredentialsOptions not found in grpc::experimental.")
    message(DEBUG "Build output:")
    list(APPEND CMAKE_MESSAGE_INDENT "check_tls_opts_132.cc: ")
    message(DEBUG ${TSL_CREDENTIALS_OPTIONS_CHECK_OUTPUT})
    list(REMOVE_AT CMAKE_MESSAGE_INDENT -1)

    try_compile(HAS_GRPC_127 ${CMAKE_CURRENT_BINARY_DIR}/try_compile SOURCES
                "${CMAKE_CURRENT_SOURCE_DIR}/try_compile/check_tls_opts_127.cc"
                CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${CURRENT_INCLUDE_DIRECTORIES}"
                OUTPUT_VARIABLE TSL_CREDENTIALS_OPTIONS_CHECK_OUTPUT CXX_STANDARD 11)

    if(HAS_GRPC_127)
      message(STATUS "TlsCredentialsOptions found in grpc_impl::experimental.")
      add_definitions(
        -DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc_impl::experimental)
    else()
      message(STATUS "TlsCredentialsOptions not found in grpc_impl::experimental.")
      message(DEBUG "Build output:")
      list(APPEND CMAKE_MESSAGE_INDENT "check_tls_opts_127.cc: ")
      message(DEBUG ${TSL_CREDENTIALS_OPTIONS_CHECK_OUTPUT})
      list(REMOVE_AT CMAKE_MESSAGE_INDENT -1)
    endif()
  endif()
endif()

# </KLUDGE> Restore the CXXFLAGS that were modified above
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_BACKUP}")

# Note, we do not compile the generated Protobuf sources directly, instead
# compiling then via protocol_internal.cc which contains some gRPC template
# overrides to enable Flight-specific optimizations. See comments in
# protobuf-internal.cc
set(ARROW_FLIGHT_SRCS
    client.cc
    client_cookie_middleware.cc
    client_header_internal.cc
    internal.cc
    protocol_internal.cc
    serialization_internal.cc
    server.cc
    server_auth.cc
    types.cc)

add_arrow_lib(arrow_flight
              CMAKE_PACKAGE_NAME
              ArrowFlight
              PKG_CONFIG_NAME
              arrow-flight
              OUTPUTS
              ARROW_FLIGHT_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/flight/pch.h>"
              DEPENDENCIES
              flight_grpc_gen
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_shared
              ${ARROW_FLIGHT_STATIC_LINK_LIBS}
              STATIC_LINK_LIBS
              arrow_static
              ${ARROW_FLIGHT_STATIC_LINK_LIBS})

foreach(LIB_TARGET ${ARROW_FLIGHT_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_EXPORTING)
endforeach()

# Define arrow_flight_testing library
if(ARROW_TESTING)
  add_arrow_lib(arrow_flight_testing
                CMAKE_PACKAGE_NAME
                ArrowFlightTesting
                PKG_CONFIG_NAME
                arrow-flight-testing
                OUTPUTS
                ARROW_FLIGHT_TESTING_LIBRARIES
                SOURCES
                test_integration.cc
                test_util.cc
                DEPENDENCIES
                GTest::gtest
                flight_grpc_gen
                arrow_dependencies
                SHARED_LINK_LIBS
                arrow_shared
                arrow_flight_shared
                arrow_testing_shared
                ${BOOST_FILESYSTEM_LIBRARY}
                ${BOOST_SYSTEM_LIBRARY}
                GTest::gtest
                STATIC_LINK_LIBS
                arrow_static
                arrow_flight_static
                arrow_testing_static)
endif()

foreach(LIB_TARGET ${ARROW_FLIGHT_TESTING_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_EXPORTING
                             ${ARROW_BOOST_PROCESS_COMPILE_DEFINITIONS})
endforeach()

add_arrow_test(flight_test
               STATIC_LINK_LIBS
               ${ARROW_FLIGHT_TEST_LINK_LIBS}
               LABELS
               "arrow_flight")

# Build test server for unit tests or benchmarks
if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  add_executable(flight-test-server test_server.cc)
  target_link_libraries(flight-test-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::gtest)

  if(ARROW_BUILD_TESTS)
    add_dependencies(arrow-flight-test flight-test-server)
  endif()

  add_dependencies(arrow_flight flight-test-server)
endif()

if(ARROW_BUILD_INTEGRATION)
  add_executable(flight-test-integration-server test_integration_server.cc)
  target_link_libraries(flight-test-integration-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::gtest)

  add_executable(flight-test-integration-client test_integration_client.cc)
  target_link_libraries(flight-test-integration-client ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::gtest)

  add_dependencies(arrow_flight flight-test-integration-client
                   flight-test-integration-server)
  add_dependencies(arrow-integration flight-test-integration-client
                   flight-test-integration-server)
endif()

if(ARROW_BUILD_BENCHMARKS)
  # Perf server for benchmarks
  set(PERF_PROTO_GENERATED_FILES "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.cc"
                                 "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.h")

  add_custom_command(OUTPUT ${PERF_PROTO_GENERATED_FILES}
                     COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${CMAKE_CURRENT_SOURCE_DIR}"
                             "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "perf.proto"
                     DEPENDS ${PROTO_DEPENDS})

  add_executable(arrow-flight-perf-server perf_server.cc perf.pb.cc)
  target_link_libraries(arrow-flight-perf-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::gtest)

  add_executable(arrow-flight-benchmark flight_benchmark.cc perf.pb.cc)
  target_link_libraries(arrow-flight-benchmark ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::gtest)

  add_dependencies(arrow-flight-benchmark arrow-flight-perf-server)

  add_dependencies(arrow_flight arrow-flight-benchmark)
endif(ARROW_BUILD_BENCHMARKS)
