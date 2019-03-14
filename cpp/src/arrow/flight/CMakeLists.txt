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

# Header files
arrow_install_all_headers("arrow/flight")

set(ARROW_FLIGHT_STATIC_LINK_LIBS
    protobuf::libprotobuf
    gRPC::grpc++
    gRPC::grpc
    gRPC::gpr
    c-ares::cares)

if(GRPC_HAS_ADDRESS_SORTING)
  list(APPEND ARROW_FLIGHT_STATIC_LINK_LIBS gRPC::address_sorting)
endif()

set(ARROW_FLIGHT_TEST_LINK_LIBS
    arrow_flight_shared
    arrow_flight_testing_shared
    ${ARROW_TEST_SHARED_LINK_LIBS}
    ${ARROW_FLIGHT_STATIC_LINK_LIBS})

# TODO(wesm): Protobuf shared vs static linking

set(FLIGHT_PROTO_PATH "${ARROW_SOURCE_DIR}/../format")
set(FLIGHT_PROTO ${ARROW_SOURCE_DIR}/../format/Flight.proto)

set(FLIGHT_GENERATED_PROTO_FILES
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.h"
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.h")

set(PROTO_DEPENDS ${FLIGHT_PROTO} protobuf::libprotobuf gRPC::grpc)

add_custom_command(
  OUTPUT ${FLIGHT_GENERATED_PROTO_FILES}
  COMMAND protobuf::protoc "-I${FLIGHT_PROTO_PATH}"
          "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${FLIGHT_PROTO}"
  DEPENDS ${PROTO_DEPENDS} ARGS
  COMMAND protobuf::protoc "-I${FLIGHT_PROTO_PATH}"
          "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
          "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}" "${FLIGHT_PROTO}")

set_source_files_properties(${FLIGHT_GENERATED_PROTO_FILES} PROPERTIES GENERATED TRUE)

add_custom_target(flight_grpc_gen ALL DEPENDS ${FLIGHT_GENERATED_PROTO_FILES})

# Note, we do not compile the generated Protobuf sources directly, instead
# compiling then via protocol-internal.cc which contains some gRPC template
# overrides to enable Flight-specific optimizations. See comments in
# protobuf-internal.cc
set(ARROW_FLIGHT_SRCS
    client.cc
    internal.cc
    protocol-internal.cc
    serialization-internal.cc
    server.cc
    types.cc)

add_arrow_lib(arrow_flight
              SOURCES
              ${ARROW_FLIGHT_SRCS}
              DEPENDENCIES
              flight_grpc_gen
              metadata_fbs
              SHARED_LINK_LIBS
              arrow_shared
              ${ARROW_FLIGHT_STATIC_LINK_LIBS}
              STATIC_LINK_LIBS
              arrow_static
              ${ARROW_FLIGHT_STATIC_LINK_LIBS})

# Define arrow_flight_testing library
if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  add_arrow_lib(arrow_flight_testing
                SOURCES
                test-util.cc
                DEPENDENCIES
                GTest::GTest
                flight_grpc_gen
                arrow_dependencies
                metadata_fbs
                SHARED_LINK_LIBS
                arrow_shared
                arrow_flight_shared
                ${BOOST_FILESYSTEM_LIBRARY}
                ${BOOST_SYSTEM_LIBRARY}
                GTest::GTest
                STATIC_LINK_LIBS
                arrow_static
                arrow_flight_static)
endif()

add_arrow_test(flight-test
               EXTRA_LINK_LIBS
               ${ARROW_FLIGHT_TEST_LINK_LIBS}
               LABELS
               "arrow_flight")

# Build test server for unit tests or benchmarks
if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  add_executable(flight-test-server test-server.cc)
  target_link_libraries(flight-test-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::GTest)

  add_executable(flight-test-integration-server test-integration-server.cc)
  target_link_libraries(flight-test-integration-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::GTest)

  add_executable(flight-test-integration-client test-integration-client.cc)
  target_link_libraries(flight-test-integration-client ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::GTest)

  # This is needed for the unit tests
  if(ARROW_BUILD_TESTS)
    add_dependencies(arrow-flight-test flight-test-server)
  endif()

  add_dependencies(arrow_flight flight-test-server flight-test-integration-client
                   flight-test-integration-server)
endif()

if(ARROW_BUILD_BENCHMARKS)
  # Perf server for benchmarks
  set(PERF_PROTO_GENERATED_FILES "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.cc"
      "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.h")

  add_custom_command(OUTPUT ${PERF_PROTO_GENERATED_FILES}
                     COMMAND protobuf::protoc "-I${CMAKE_CURRENT_SOURCE_DIR}"
                             "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "perf.proto"
                     DEPENDS ${PROTO_DEPENDS})

  add_executable(arrow-flight-perf-server perf-server.cc perf.pb.cc)
  target_link_libraries(arrow-flight-perf-server
                        arrow_flight_shared
                        arrow_flight_testing_shared
                        ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES}
                        GTest::GTest)

  add_executable(arrow-flight-benchmark flight-benchmark.cc perf.pb.cc)
  target_link_libraries(arrow-flight-benchmark
                        arrow_flight_static
                        arrow_testing_static
                        ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES}
                        GTest::GTest)

  add_dependencies(arrow-flight-benchmark arrow-flight-perf-server)

  add_dependencies(arrow_flight arrow-flight-benchmark)
endif(ARROW_BUILD_BENCHMARKS)
