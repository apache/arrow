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

set(ARROW_FLIGHT_SQL_LINK_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF})

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

set(ARROW_FLIGHT_SQL_SRCS protocol_internal.cc sql_server.cpp)

add_arrow_lib(arrow_flight_sql
              CMAKE_PACKAGE_NAME
              ArrowFlightSql
              PKG_CONFIG_NAME
              arrow-flight-sql
              OUTPUTS
              ARROW_FLIGHT_SQL_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_SQL_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/flight/flight-sql/pch.h>"
              DEPENDENCIES
              flight_sql_grpc_gen
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_flight_shared
              ${ARROW_FLIGHT_SQL_LINK_LIBS}
              STATIC_LINK_LIBS
              arrow_flight_static
              ${ARROW_FLIGHT_SQL_LINK_LIBS})

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_FLIGHT_SQL_TEST_LINK_LIBS
      arrow_flight_sql_static arrow_flight_testing_static
      ${ARROW_FLIGHT_STATIC_LINK_LIBS} ${ARROW_TEST_LINK_LIBS})
else()
  set(ARROW_FLIGHT_SQL_TEST_LINK_LIBS arrow_flight_sql_shared arrow_flight_testing_shared
                                      ${ARROW_TEST_LINK_LIBS})
endif()

add_arrow_test(flight_sql_test
               SOURCES
               client_test.cc
               sql_server_test.cc
               STATIC_LINK_LIBS
               ${ARROW_FLIGHT_SQL_LINK_LIBS}
               ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS}
               GTest::gtest
               GTest::gmock
               LABELS
               "arrow_flight_sql")

add_executable(flight_sql_test_app test_app.cc)
target_link_libraries(flight_sql_test_app PRIVATE arrow_flight_sql_static
                                                  ${GFLAGS_LIBRARIES})

find_package(SQLite3)
include_directories(${SQLite3_INCLUDE_DIRS})

add_executable(flight_sql_test_server
               sql_server.cpp
               test_server.cc
               example/sqlite_statement.cc
               example/sqlite_statement_batch_reader.cc
               example/sqlite_server.cc)
target_link_libraries(flight_sql_test_server
                      PRIVATE arrow_flight_sql_static ${GFLAGS_LIBRARIES}
                              ${SQLite3_LIBRARIES})
