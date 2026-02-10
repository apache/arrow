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

if(WIN32)
  include_directories(${ODBC_INCLUDE_DIRS})
else()
  include_directories(${ODBC_INCLUDE_DIR})
endif()

find_package(SQLite3Alt REQUIRED)

set(ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS
    ../../example/sqlite_sql_info.cc
    ../../example/sqlite_type_info.cc
    ../../example/sqlite_statement.cc
    ../../example/sqlite_statement_batch_reader.cc
    ../../example/sqlite_server.cc
    ../../example/sqlite_tables_schema_batch_reader.cc)

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS arrow_flight_sql_odbc_static
                                           ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS arrow_flight_sql_odbc_shared
                                           ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

set(ARROW_FLIGHT_SQL_ODBC_TEST_SRCS
    odbc_test_suite.cc
    odbc_test_suite.h
    columns_test.cc
    connection_attr_test.cc
    connection_info_test.cc
    connection_test.cc
    errors_test.cc
    get_functions_test.cc
    statement_attr_test.cc
    statement_test.cc
    tables_test.cc
    type_info_test.cc
    # Enable Protobuf cleanup after test execution
    # GH-46889: move protobuf_test_util to a more common location
    ../../../../engine/substrait/protobuf_test_util.cc)

# On Windows, dynamic linking ODBC is supported, tests link libraries dynamically.
# On unix systems, static linking ODBC is supported, thus tests link libraries statically.
if(WIN32)
  add_arrow_test(flight_sql_odbc_test
                 SOURCES
                 ${ARROW_FLIGHT_SQL_ODBC_TEST_SRCS}
                 ${ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS}
                 DEFINITIONS
                 UNICODE
                 EXTRA_LINK_LIBS
                 ${ODBC_LIBRARIES}
                 ${ODBCINST}
                 ${SQLite3_LIBRARIES}
                 ${ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS})
elseif(APPLE)
  # macOS
  add_arrow_test(flight_sql_odbc_test
                 SOURCES
                 ${ARROW_FLIGHT_SQL_ODBC_TEST_SRCS}
                 ${ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS}
                 DEFINITIONS
                 UNICODE
                 STATIC_LINK_LIBS
                 iodbc
                 ${ODBC_LIBRARIES}
                 ${ODBCINST}
                 ${SQLite3_LIBRARIES}
                 ${ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS})
endif()

find_package(ODBC REQUIRED)
target_link_libraries(arrow-flight-sql-odbc-test PRIVATE ODBC::ODBC)

# Disable unity build due to sqlite_sql_info.cc conflict with sql.h and sqlext.h headers.
set_target_properties(arrow-flight-sql-odbc-test PROPERTIES UNITY_BUILD OFF)
