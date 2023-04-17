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
                           "--cpp_out=dllexport_decl=ARROW_FLIGHT_SQL_EXPORT:${CMAKE_CURRENT_BINARY_DIR}"
                           "${FLIGHT_SQL_PROTO}"
                   DEPENDS ${PROTO_DEPENDS})

set_source_files_properties(${FLIGHT_SQL_GENERATED_PROTO_FILES} PROPERTIES GENERATED TRUE)
add_custom_target(flight_sql_protobuf_gen ALL DEPENDS ${FLIGHT_SQL_GENERATED_PROTO_FILES})

set(ARROW_FLIGHT_SQL_SRCS
    server.cc
    sql_info_internal.cc
    column_metadata.cc
    client.cc
    protocol_internal.cc)

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
              arrow_flight_static
              PRIVATE_INCLUDES
              "${Protobuf_INCLUDE_DIRS}")

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_flight_sql_static PUBLIC ARROW_FLIGHT_SQL_STATIC)
endif()

if(MSVC)
  # Suppress warnings caused by Protobuf (casts)
  set_source_files_properties(protocol_internal.cc PROPERTIES COMPILE_FLAGS "/wd4267")
endif()
foreach(LIB_TARGET ${ARROW_FLIGHT_SQL_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_SQL_EXPORTING)
endforeach()

if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static" AND ARROW_BUILD_STATIC)
  set(ARROW_FLIGHT_SQL_TEST_LINK_LIBS arrow_flight_sql_static)
else()
  set(ARROW_FLIGHT_SQL_TEST_LINK_LIBS arrow_flight_sql_shared)
endif()
list(APPEND ARROW_FLIGHT_SQL_TEST_LINK_LIBS ${ARROW_FLIGHT_TEST_LINK_LIBS})

# Build test server for unit tests
if(ARROW_BUILD_TESTS OR ARROW_BUILD_EXAMPLES)
  find_package(SQLite3Alt REQUIRED)

  set(ARROW_FLIGHT_SQL_TEST_SERVER_SRCS
      example/sqlite_sql_info.cc
      example/sqlite_type_info.cc
      example/sqlite_statement.cc
      example/sqlite_statement_batch_reader.cc
      example/sqlite_server.cc
      example/sqlite_tables_schema_batch_reader.cc)

  set(ARROW_FLIGHT_SQL_TEST_SRCS server_test.cc)
  set(ARROW_FLIGHT_SQL_TEST_LIBS ${SQLite3_LIBRARIES})
  set(ARROW_FLIGHT_SQL_ACERO_SRCS example/acero_server.cc)

  if(ARROW_COMPUTE
     AND ARROW_PARQUET
     AND ARROW_SUBSTRAIT)
    list(APPEND ARROW_FLIGHT_SQL_TEST_SRCS ${ARROW_FLIGHT_SQL_ACERO_SRCS} acero_test.cc)
    if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static")
      list(APPEND ARROW_FLIGHT_SQL_TEST_LIBS arrow_substrait_static)
    else()
      list(APPEND ARROW_FLIGHT_SQL_TEST_LIBS arrow_substrait_shared)
    endif()

    if(ARROW_BUILD_EXAMPLES)
      add_executable(acero-flight-sql-server ${ARROW_FLIGHT_SQL_ACERO_SRCS}
                                             example/acero_main.cc)
      target_link_libraries(acero-flight-sql-server
                            PRIVATE ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS}
                                    ${ARROW_FLIGHT_SQL_TEST_LIBS} ${GFLAGS_LIBRARIES})
    endif()
  endif()

  add_arrow_test(flight_sql_test
                 SOURCES
                 ${ARROW_FLIGHT_SQL_TEST_SRCS}
                 ${ARROW_FLIGHT_SQL_TEST_SERVER_SRCS}
                 STATIC_LINK_LIBS
                 ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS}
                 ${ARROW_FLIGHT_SQL_TEST_LIBS}
                 EXTRA_INCLUDES
                 "${CMAKE_CURRENT_BINARY_DIR}/../"
                 LABELS
                 "arrow_flight_sql")

  add_executable(flight-sql-test-server test_server_cli.cc
                                        ${ARROW_FLIGHT_SQL_TEST_SERVER_SRCS})
  target_link_libraries(flight-sql-test-server
                        PRIVATE ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS} ${GFLAGS_LIBRARIES}
                                ${SQLite3_LIBRARIES})

  add_executable(flight-sql-test-app test_app_cli.cc)
  target_link_libraries(flight-sql-test-app PRIVATE ${ARROW_FLIGHT_SQL_TEST_LINK_LIBS}
                                                    ${GFLAGS_LIBRARIES})

  if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static" AND ARROW_BUILD_STATIC)
    foreach(TEST_TARGET arrow-flight-sql-test flight-sql-test-server flight-sql-test-app)
      target_compile_definitions(${TEST_TARGET} PUBLIC ARROW_FLIGHT_STATIC
                                                       ARROW_FLIGHT_SQL_STATIC)
    endforeach()
  endif()
endif()
