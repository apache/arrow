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
ARROW_INSTALL_ALL_HEADERS("arrow/flight")

SET(ARROW_FLIGHT_STATIC_LINK_LIBS
  grpc_grpcpp_static
  grpc_grpc_static
  grpc_gpr_static
  grpc_address_sorting_static
  cares_static)

SET(ARROW_FLIGHT_TEST_STATIC_LINK_LIBS
  arrow_static
  arrow_flight_static
  arrow_testing_static
  ${ARROW_FLIGHT_STATIC_LINK_LIBS}
  ${PROTOBUF_LIBRARY})

# TODO(wesm): Protobuf shared vs static linking

set(FLIGHT_PROTO_PATH "${ARROW_SOURCE_DIR}/../format")
set(FLIGHT_PROTO ${ARROW_SOURCE_DIR}/../format/Flight.proto)

set(FLIGHT_GENERATED_PROTO_FILES
  "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.h"
  "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.h")

set(PROTO_DEPENDS ${FLIGHT_PROTO} ${PROTOBUF_LIBRARY})

add_custom_command(
  OUTPUT ${FLIGHT_GENERATED_PROTO_FILES}
  COMMAND ${PROTOBUF_EXECUTABLE}
  "-I${FLIGHT_PROTO_PATH}"
  "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}"
  "${FLIGHT_PROTO}"
  DEPENDS ${PROTO_DEPENDS}
  ARGS
  COMMAND ${PROTOBUF_EXECUTABLE}
  "-I${FLIGHT_PROTO_PATH}"
  "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
  "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}"
  "${FLIGHT_PROTO}")

set_source_files_properties(${FLIGHT_GENERATED_PROTO_FILES}
  PROPERTIES GENERATED TRUE)

set(ARROW_FLIGHT_SRCS
  client.cc
  Flight.pb.cc
  Flight.grpc.pb.cc
  internal.cc
  server.cc
  types.cc
)

ADD_ARROW_LIB(arrow_flight
  SOURCES ${ARROW_FLIGHT_SRCS}
  SHARED_LINK_LIBS arrow_shared ${ARROW_FLIGHT_STATIC_LINK_LIBS}
  STATIC_LINK_LIBS arrow_static ${ARROW_FLIGHT_STATIC_LINK_LIBS})

ADD_ARROW_TEST(flight-test
  EXTRA_LINK_LIBS ${ARROW_FLIGHT_TEST_STATIC_LINK_LIBS}
  LABELS "arrow_flight")

# Build test server for unit tests or benchmarks
if (ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  add_executable(flight-test-server test-server.cc)
  target_link_libraries(flight-test-server
    ${ARROW_FLIGHT_TEST_STATIC_LINK_LIBS}
    gflags_static
    ${GTEST_LIBRARY})

  add_executable(flight-test-integration-server test-integration-server.cc)
  target_link_libraries(flight-test-integration-server
    ${ARROW_FLIGHT_TEST_STATIC_LINK_LIBS}
    gflags_static
    gtest_static)

  add_executable(flight-test-integration-client test-integration-client.cc)
  target_link_libraries(flight-test-integration-client
    ${ARROW_FLIGHT_TEST_STATIC_LINK_LIBS}
    gflags_static
    gtest_static)

  # This is needed for the unit tests
  if (ARROW_BUILD_TESTS)
    add_dependencies(arrow-flight-test flight-test-server)
  endif()
endif()

if (ARROW_BUILD_BENCHMARKS)
  # Perf server for benchmarks
  set(PERF_PROTO_GENERATED_FILES
    "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/perf.pb.h")

  add_custom_command(
    OUTPUT ${PERF_PROTO_GENERATED_FILES}
    COMMAND ${PROTOBUF_EXECUTABLE}
    "-I${CMAKE_CURRENT_SOURCE_DIR}"
    "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}"
    "perf.proto"
    DEPENDS ${PROTO_DEPENDS})

  add_executable(flight-perf-server
    perf-server.cc
    perf.pb.cc)
  target_link_libraries(flight-perf-server
    arrow_flight_static
    ${ARROW_FLIGHT_STATIC_LINK_LIBS}
    gflags_static
    ${GTEST_LIBRARY})

  add_executable(flight-benchmark
    flight-benchmark.cc
    perf.pb.cc)
  target_link_libraries(flight-benchmark
    arrow_flight_static
    ${ARROW_FLIGHT_STATIC_LINK_LIBS}
    gflags_static
    ${GTEST_LIBRARY})

  add_dependencies(flight-benchmark flight-perf-server)
endif(ARROW_BUILD_BENCHMARKS)
