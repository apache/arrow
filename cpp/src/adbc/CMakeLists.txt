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

add_custom_target(adbc)

set(ADBC_LINK_SHARED arrow_shared)
set(ADBC_LINK_STATIC arrow_static)

set(ADBC_CLIENT_SOURCES client/driver.cc)
set(ADBC_DRIVER_SOURCES driver/util.cc)

# Driver Manager
# TODO: this should be buildable/distributable separately
add_arrow_lib(adbc_driver_manager
              SOURCES
              adbc_driver_manager.cc
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS})

# C++ library to implement drivers
add_arrow_lib(adbc_driver
              PKG_CONFIG_NAME
              adbc-driver
              SOURCES
              ${ADBC_DRIVER_SOURCES}
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS}
              SHARED_LINK_LIBS
              ${ADBC_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ADBC_LINK_STATIC})

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ADBC_TEST_LINK_LIBS adbc_driver_manager_static)
else()
  set(ADBC_TEST_LINK_LIBS adbc_driver_manager_shared)
endif()

add_arrow_test(adbc_test
               SOURCES
               adbc_test.cc
               adbc_driver_manager_test.cc
               test_sqlite_internal.cc
               EXTRA_LINK_LIBS
               ${ADBC_TEST_LINK_LIBS}
               PREFIX
               "adbc")

if(ARROW_BUILD_TESTS)
  find_package(SQLite3Alt REQUIRED)

  add_arrow_lib(adbc_driver_sqlite
                SOURCES
                driver/example/sqlite.cc
                BUILD_STATIC
                OFF
                SHARED_LINK_LIBS
                ${ADBC_LINK_SHARED}
                ${SQLite3_LIBRARIES})
  add_dependencies(adbc-adbc-test adbc_driver_sqlite_shared)

  if(ARROW_FLIGHT_SQL)
    add_arrow_lib(adbc_driver_flight_sql
                  SOURCES
                  driver/example/flight_sql.cc
                  BUILD_STATIC
                  OFF
                  SHARED_LINK_LIBS
                  ${ADBC_LINK_SHARED}
                  arrow_flight_shared
                  arrow_flight_sql_shared
                  adbc_driver_shared)

    if(ARROW_TEST_LINKAGE STREQUAL "static")
      set(ADBC_FLIGHT_SQL_TEST_LINK_LIBS arrow_flight_static arrow_flight_sql_static
                                         arrow_flight_sql_sqlite_static)
    else()
      set(ADBC_FLIGHT_SQL_TEST_LINK_LIBS arrow_flight_shared arrow_flight_sql_shared
                                         arrow_flight_sql_sqlite_shared)
    endif()

    add_arrow_test(adbc_flight_sql_test
                   SOURCES
                   driver/example/flight_sql_test.cc
                   EXTRA_LINK_LIBS
                   ${ADBC_TEST_LINK_LIBS}
                   ${ADBC_FLIGHT_SQL_TEST_LINK_LIBS}
                   PREFIX
                   "adbc")

    add_dependencies(adbc-adbc-flight-sql-test adbc_driver_flight_sql_shared)
  endif()
endif()
