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

set(ARROW_FLIGHT_LINK_LIBS gRPC::grpc++ ${ARROW_PROTOBUF_LIBPROTOBUF})

if(WIN32)
  list(APPEND ARROW_FLIGHT_LINK_LIBS ws2_32.lib)
endif()

set(ARROW_FLIGHT_TEST_LINKAGE "${ARROW_TEST_LINKAGE}")
if(Protobuf_USE_STATIC_LIBS)
  message(STATUS "Linking Arrow Flight tests statically due to static Protobuf")
  set(ARROW_FLIGHT_TEST_LINKAGE "static")
endif()
if(NOT ARROW_GRPC_USE_SHARED)
  message(STATUS "Linking Arrow Flight tests statically due to static gRPC")
  set(ARROW_FLIGHT_TEST_LINKAGE "static")
endif()

set(ARROW_FLIGHT_TEST_INTERFACE_LIBS)
if(ARROW_BUILD_BENCHMARKS
   OR ARROW_BUILD_INTEGRATION
   OR ARROW_BUILD_TESTS)
  if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static")
    if(NOT ARROW_BUILD_STATIC)
      message(STATUS "If static Protobuf or gRPC are used, Arrow must be built statically"
      )
      message(STATUS "(These libraries have global state, and linkage must be consistent)"
      )
      message(FATAL_ERROR "Must build Arrow statically to link Flight tests statically")
    endif()
    set(ARROW_FLIGHT_TEST_LINK_LIBS arrow_flight_static arrow_flight_testing_static)
    list(APPEND ARROW_FLIGHT_TEST_LINK_LIBS ${ARROW_TEST_STATIC_LINK_LIBS})
    if(ARROW_CUDA)
      list(APPEND ARROW_FLIGHT_TEST_INTERFACE_LIBS arrow_cuda_static)
      list(APPEND ARROW_FLIGHT_TEST_LINK_LIBS arrow_cuda_static)
    endif()
  else()
    set(ARROW_FLIGHT_TEST_LINK_LIBS arrow_flight_shared arrow_flight_testing_shared
                                    ${ARROW_TEST_SHARED_LINK_LIBS})
    if(ARROW_CUDA)
      list(APPEND ARROW_FLIGHT_TEST_INTERFACE_LIBS arrow_cuda_shared)
      list(APPEND ARROW_FLIGHT_TEST_LINK_LIBS arrow_cuda_shared)
    endif()
  endif()
endif()
list(APPEND
     ARROW_FLIGHT_TEST_INTERFACE_LIBS
     Boost::headers
     Boost::filesystem
     Boost::system
     GTest::gtest
     GTest::gmock)
list(APPEND ARROW_FLIGHT_TEST_LINK_LIBS gRPC::grpc++)

# TODO(wesm): Protobuf shared vs static linking

set(FLIGHT_PROTO_PATH "${ARROW_SOURCE_DIR}/../format")
set(FLIGHT_PROTO "${ARROW_SOURCE_DIR}/../format/Flight.proto")

set(FLIGHT_GENERATED_PROTO_FILES
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.cc" "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.h"
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.grpc.pb.h")

set(PROTO_DEPENDS ${FLIGHT_PROTO} ${ARROW_PROTOBUF_LIBPROTOBUF} gRPC::grpc_cpp_plugin)

add_custom_command(OUTPUT ${FLIGHT_GENERATED_PROTO_FILES}
                   COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_PROTO_PATH}"
                           "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${FLIGHT_PROTO}"
                   DEPENDS ${PROTO_DEPENDS} ARGS
                   COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_PROTO_PATH}"
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
# gRPC's pkg-config file neglects to specify pthreads.
find_package(Threads REQUIRED)
function(test_grpc_version DST_VAR DETECT_VERSION TEST_FILE)
  if(NOT DEFINED ${DST_VAR})
    message(STATUS "Checking support for TlsCredentialsOptions (gRPC >= ${DETECT_VERSION})..."
    )
    get_property(CURRENT_INCLUDE_DIRECTORIES
                 DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                 PROPERTY INCLUDE_DIRECTORIES)
    # ARROW-13881: when detecting support, avoid mismatch between
    # debug flags of gRPC and our probe (which results in LNK2038)
    set(CMAKE_TRY_COMPILE_CONFIGURATION ${CMAKE_BUILD_TYPE})
    try_compile(HAS_GRPC_VERSION ${CMAKE_CURRENT_BINARY_DIR}/try_compile
                SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/try_compile/${TEST_FILE}"
                CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${CURRENT_INCLUDE_DIRECTORIES}"
                LINK_LIBRARIES gRPC::grpc++ Threads::Threads
                OUTPUT_VARIABLE TLS_CREDENTIALS_OPTIONS_CHECK_OUTPUT CXX_STANDARD 11)
    if(HAS_GRPC_VERSION)
      set(${DST_VAR}
          "${DETECT_VERSION}"
          CACHE INTERNAL "The detected (approximate) gRPC version.")
    else()
      message(STATUS "TlsCredentialsOptions (for gRPC ${DETECT_VERSION}) not found in grpc::experimental."
      )
      if(ARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS)
        message(WARNING "Build output:")
        list(APPEND CMAKE_MESSAGE_INDENT "${TEST_FILE}: ")
        message(WARNING ${TLS_CREDENTIALS_OPTIONS_CHECK_OUTPUT})
        list(REMOVE_AT CMAKE_MESSAGE_INDENT -1)
      else()
        message(DEBUG "Build output:")
        list(APPEND CMAKE_MESSAGE_INDENT "${TEST_FILE}: ")
        message(DEBUG ${TLS_CREDENTIALS_OPTIONS_CHECK_OUTPUT})
        list(REMOVE_AT CMAKE_MESSAGE_INDENT -1)
      endif()
    endif()
  endif()
endfunction()

if(GRPC_VENDORED)
  # v1.35.0 -> 1.35
  string(REGEX MATCH "[0-9]+\\.[0-9]+" GRPC_VERSION "${ARROW_GRPC_BUILD_VERSION}")
else()
  test_grpc_version(GRPC_VERSION "1.43" "check_tls_opts_143.cc")
  test_grpc_version(GRPC_VERSION "1.36" "check_tls_opts_136.cc")
  test_grpc_version(GRPC_VERSION "1.34" "check_tls_opts_134.cc")
  test_grpc_version(GRPC_VERSION "1.32" "check_tls_opts_132.cc")
  test_grpc_version(GRPC_VERSION "1.27" "check_tls_opts_127.cc")
  message(STATUS "Found approximate gRPC version: ${GRPC_VERSION} (ARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS=${ARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS})"
  )
endif()
if(GRPC_VERSION EQUAL "1.27")
  add_definitions(-DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc_impl::experimental)
elseif(GRPC_VERSION EQUAL "1.32")
  add_definitions(-DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc::experimental)
elseif(GRPC_VERSION EQUAL "1.34" OR GRPC_VERSION EQUAL "1.35")
  add_definitions(-DGRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS
                  -DGRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS
                  -DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc::experimental)
elseif(GRPC_VERSION EQUAL "1.36")
  add_definitions(-DGRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS
                  -DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc::experimental)
elseif((GRPC_VERSION EQUAL "1.43") OR (GRPC_VERSION EQUAL "1.46"))
  # 1.46 is the bundled version
  add_definitions(-DGRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS
                  -DGRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS=grpc::experimental
                  -DGRPC_USE_CERTIFICATE_VERIFIER)
else()
  message(STATUS "A proper version of gRPC could not be found to support TlsCredentialsOptions in Arrow Flight."
  )
  message(STATUS "You may need a newer version of gRPC (>= 1.27), or the gRPC API has changed and Flight must be updated to match."
  )
  if(ARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS)
    message(FATAL_ERROR "Halting build since ARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS is set."
    )
  endif()
endif()

# </KLUDGE> Restore the CXXFLAGS that were modified above
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_BACKUP}")

# Note, we do not compile the generated gRPC sources directly, instead
# compiling them via protocol_internal.cc which contains some gRPC template
# overrides to enable Flight-specific optimizations. See comments in
# protocol_internal.cc
set(ARROW_FLIGHT_SRCS
    "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.cc"
    client.cc
    client_cookie_middleware.cc
    client_tracing_middleware.cc
    cookie_internal.cc
    middleware.cc
    serialization_internal.cc
    server.cc
    server_auth.cc
    server_tracing_middleware.cc
    transport.cc
    transport_server.cc
    # Bundle the gRPC impl with libarrow_flight
    transport/grpc/grpc_client.cc
    transport/grpc/grpc_server.cc
    transport/grpc/serialization_internal.cc
    transport/grpc/protocol_grpc_internal.cc
    transport/grpc/util_internal.cc
    types.cc)

if(MSVC)
  # Protobuf generated files trigger spurious warnings on MSVC.
  foreach(GENERATED_SOURCE "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.cc"
                           "${CMAKE_CURRENT_BINARY_DIR}/Flight.pb.h")
    # Suppress missing dll-interface warning
    set_source_files_properties("${GENERATED_SOURCE}"
                                PROPERTIES COMPILE_OPTIONS "/wd4251"
                                           GENERATED TRUE
                                           SKIP_UNITY_BUILD_INCLUSION TRUE)
  endforeach()
endif()

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
              ${ARROW_FLIGHT_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              Arrow::arrow_shared
              STATIC_LINK_LIBS
              arrow_static
              ${ARROW_FLIGHT_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              Arrow::arrow_static)

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_flight_static PUBLIC ARROW_FLIGHT_STATIC)
endif()

foreach(LIB_TARGET ${ARROW_FLIGHT_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_EXPORTING)
endforeach()

# Define arrow_flight_testing library
if(ARROW_TESTING)
  if(ARROW_BUILD_SHARED AND ARROW_BUILD_STATIC)
    set(ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS arrow_flight_shared)
    set(ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS arrow_flight_static)
    set(ARROW_FLIGHT_TESTING_SHARED_INSTALL_INTERFACE_LIBS
        ArrowFlight::arrow_flight_shared)
    set(ARROW_FLIGHT_TESTING_STATIC_INSTALL_INTERFACE_LIBS
        ArrowFlight::arrow_flight_static)
  elseif(ARROW_BUILD_SHARED)
    set(ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS arrow_flight_shared)
    set(ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS arrow_flight_shared)
    set(ARROW_FLIGHT_TESTING_SHARED_INSTALL_INTERFACE_LIBS
        ArrowFlight::arrow_flight_shared)
    set(ARROW_FLIGHT_TESTING_STATIC_INSTALL_INTERFACE_LIBS
        ArrowFlight::arrow_flight_shared)
  else()
    set(ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS arrow_flight_static)
    set(ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS arrow_flight_static)
    set(ARROW_FLIGHT_TESTING_SHARED_INSTALL_INTERFACE_LIBS
        ArrowFlight::arrow_flight_static)
    set(ARROW_FLIGHT_TESTING_STATIC_INSTALL_INTERFACE_LIBS
        ArrowFlight::arrow_flight_static)
  endif()
  if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "shared")
    list(APPEND ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS arrow_testing_shared)
    list(APPEND ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS arrow_testing_shared)
    list(APPEND ARROW_FLIGHT_TESTING_SHARED_INSTALL_INTERFACE_LIBS
         ArrowTesting::arrow_testing_shared)
    list(APPEND ARROW_FLIGHT_TESTING_STATIC_INSTALL_INTERFACE_LIBS
         ArrowTesting::arrow_testing_shared)
  else()
    list(APPEND ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS arrow_testing_static)
    list(APPEND ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS arrow_testing_static)
    list(APPEND ARROW_FLIGHT_TESTING_SHARED_INSTALL_INTERFACE_LIBS
         ArrowTesting::arrow_testing_static)
    list(APPEND ARROW_FLIGHT_TESTING_STATIC_INSTALL_INTERFACE_LIBS
         ArrowTesting::arrow_testing_static)
  endif()
  list(APPEND ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS ${ARROW_FLIGHT_TEST_INTERFACE_LIBS})
  list(APPEND ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS ${ARROW_FLIGHT_TEST_INTERFACE_LIBS})
  add_arrow_lib(arrow_flight_testing
                CMAKE_PACKAGE_NAME
                ArrowFlightTesting
                PKG_CONFIG_NAME
                arrow-flight-testing
                OUTPUTS
                ARROW_FLIGHT_TESTING_LIBRARIES
                SOURCES
                test_definitions.cc
                test_util.cc
                DEPENDENCIES
                GTest::gtest
                flight_grpc_gen
                arrow_dependencies
                SHARED_LINK_LIBS
                ${ARROW_FLIGHT_TESTING_SHARED_LINK_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ARROW_FLIGHT_TESTING_SHARED_INSTALL_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ARROW_FLIGHT_TESTING_STATIC_LINK_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ARROW_FLIGHT_TESTING_STATIC_INSTALL_INTERFACE_LIBS}
                PRIVATE_INCLUDES
                "${Protobuf_INCLUDE_DIRS}")

  foreach(LIB_TARGET ${ARROW_FLIGHT_TESTING_LIBRARIES})
    target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_EXPORTING)
  endforeach()
endif()

add_arrow_test(flight_internals_test
               STATIC_LINK_LIBS
               ${ARROW_FLIGHT_TEST_LINK_LIBS}
               LABELS
               "arrow_flight")

add_arrow_test(flight_test
               STATIC_LINK_LIBS
               ${ARROW_FLIGHT_TEST_LINK_LIBS}
               LABELS
               "arrow_flight")

# Build test server for unit tests or benchmarks
if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  add_executable(flight-test-server test_server.cc)
  target_link_libraries(flight-test-server ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES})

  if(ARROW_BUILD_TESTS)
    add_dependencies(arrow-flight-test flight-test-server)
  endif()

  add_dependencies(arrow_flight flight-test-server)
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
                        ${GFLAGS_LIBRARIES})

  add_executable(arrow-flight-benchmark flight_benchmark.cc perf.pb.cc)
  target_link_libraries(arrow-flight-benchmark ${ARROW_FLIGHT_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES})

  add_dependencies(arrow-flight-benchmark arrow-flight-perf-server)

  add_dependencies(arrow_flight arrow-flight-benchmark)

  if(ARROW_WITH_UCX)
    if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static")
      target_link_libraries(arrow-flight-benchmark arrow_flight_transport_ucx_static)
      target_link_libraries(arrow-flight-perf-server arrow_flight_transport_ucx_static)
    else()
      target_link_libraries(arrow-flight-benchmark arrow_flight_transport_ucx_shared)
      target_link_libraries(arrow-flight-perf-server arrow_flight_transport_ucx_shared)
    endif()
  endif()
endif(ARROW_BUILD_BENCHMARKS)

if(ARROW_WITH_UCX)
  add_subdirectory(transport/ucx)
endif()

if(ARROW_FLIGHT_SQL)
  add_subdirectory(sql)

  if(ARROW_BUILD_INTEGRATION)
    add_subdirectory(integration_tests)
  endif()
endif()
