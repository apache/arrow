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

arrow_install_all_headers("arrow/flight/sql")

set(FLIGHT_SQL_PROTO_PATH "${ARROW_SOURCE_DIR}/../format")
set(FLIGHT_SQL_PROTO ${ARROW_SOURCE_DIR}/../format/FlightSql.proto)

set(FLIGHT_SQL_GENERATED_PROTO_FILES "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.pb.cc"
                                     "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.pb.h")

set(PROTO_DEPENDS ${FLIGHT_SQL_PROTO} ${ARROW_PROTOBUF_LIBPROTOBUF})

add_custom_command(OUTPUT ${FLIGHT_SQL_GENERATED_PROTO_FILES}
                   COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_SQL_PROTO_PATH}"
                           "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${FLIGHT_SQL_PROTO}"
                   DEPENDS ${PROTO_DEPENDS})

set_source_files_properties(${FLIGHT_SQL_GENERATED_PROTO_FILES} PROPERTIES GENERATED TRUE)

add_custom_target(flight_sql_protobuf_gen ALL DEPENDS ${FLIGHT_SQL_GENERATED_PROTO_FILES})

set(ARROW_FLIGHT_SQL_SRCS server.cc sql_info_internal.cc client.cc
                          "${CMAKE_CURRENT_BINARY_DIR}/FlightSql.pb.cc")

add_arrow_lib(arrow_flight_sql
              CMAKE_PACKAGE_NAME
              ArrowFlightSql
              PKG_CONFIG_NAME
              arrow-flight-sql
              OUTPUTS
              ARROW_FLIGHT_SQL_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_SQL_SRCS}
              DEPENDENCIES
              flight_sql_protobuf_gen
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_flight_shared
              STATIC_LINK_LIBS
              arrow_flight_static)

if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static")
  set(ARROW_FLIGHT_SQL_TEST_LINK_LIBS
      arrow_flight_sql_static arrow_flight_testing_static
      ${ARROW_FLIGHT_STATIC_LINK_LIBS} ${ARROW_TEST_LINK_LIBS})
else()
  set(ARROW_FLIGHT_SQL_TEST_LINK_LIBS arrow_flight_sql_shared arrow_flight_testing_shared
                                      ${ARROW_TEST_LINK_LIBS})
endif()

# Build test server for unit tests
if(ARROW_BUILD_TESTS OR ARROW_BUILD_EXAMPLES)
  find_package(SQLite3Alt REQUIRED)

  set(ARROW_FLIGHT_SQL_TEST_SERVER_SRCS
      example/sqlite_sql_info.cc
      example/sqlite_statement.cc
      example/sqlite_statement_batch_reader.cc
      example/sqlite_server.cc
      example/sqlite_tables_schema_batch_reader.cc)

  add_arrow_test(flight_sql_test
                 SOURCES
                 client_test.cc
                 server_test.cc
                 ${ARROW_FLIGHT_SQL_TEST_SERVER_SRCS}
                 STATIC_LINK_LIBS
                 ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS}
                 ${SQLite3_LIBRARIES}
                 LABELS
                 "arrow_flight_sql")

  add_executable(flight_sql_test_server test_server_cli.cc
                                        ${ARROW_FLIGHT_SQL_TEST_SERVER_SRCS})
  target_link_libraries(flight_sql_test_server
                        PRIVATE ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS} ${GFLAGS_LIBRARIES}
                                ${SQLite3_LIBRARIES})

  add_executable(flight_sql_test_app test_app_cli.cc)
  target_link_libraries(flight_sql_test_app PRIVATE ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS}
                                                    ${GFLAGS_LIBRARIES})
endif()
