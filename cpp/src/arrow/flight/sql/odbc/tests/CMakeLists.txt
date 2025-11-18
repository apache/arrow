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

add_custom_target(tests)

find_package(ODBC REQUIRED)
include_directories(${ODBC_INCLUDE_DIRS})

find_package(SQLite3Alt REQUIRED)

set(ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS
    ../../example/sqlite_sql_info.cc
    ../../example/sqlite_type_info.cc
    ../../example/sqlite_statement.cc
    ../../example/sqlite_statement_batch_reader.cc
    ../../example/sqlite_server.cc
    ../../example/sqlite_tables_schema_batch_reader.cc)

add_arrow_test(flight_sql_odbc_test
               SOURCES
               odbc_test_suite.cc
               odbc_test_suite.h
               odbc_test_util.h
               connection_test.cc
               # Enable Protobuf cleanup after test execution
               # GH-46889: move protobuf_test_util to a more common location
               ../../../../engine/substrait/protobuf_test_util.cc
               ${ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS}
               EXTRA_LINK_LIBS
               ${ODBC_LIBRARIES}
               ${ODBCINST}
               ${SQLite3_LIBRARIES}
               arrow_odbc_spi_impl)

# Disable unity build due to sqlite_sql_info.cc conflict with sql.h and sqlext.h headers.
set_target_properties(arrow-flight-sql-odbc-test PROPERTIES UNITY_BUILD OFF)
