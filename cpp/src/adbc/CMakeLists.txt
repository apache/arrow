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

# define the dependencies
set(ARROW_SKYHOOK_LINK_STATIC arrow_static)
set(ARROW_SKYHOOK_LINK_SHARED arrow_shared)

# define the client and cls sources
set(ADBC_SOURCES adbc.cc)

# define the client library
add_arrow_lib(adbc
              PKG_CONFIG_NAME
              adbc
              SOURCES
              ${ADBC_SOURCES}
              SHARED_LINK_LIBS
              ${ADBC_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ADBC_LINK_STATIC})

# define the test builds
if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ADBC_TEST_LINK_LIBS adbc_static)
else()
  set(ADBC_TEST_LINK_LIBS adbc_shared)
endif()

add_arrow_test(adbc_test
               SOURCES
               adbc_test.cc
               EXTRA_LINK_LIBS
               ${ADBC_TEST_LINK_LIBS}
               PREFIX
               "adbc")

             find_package(SQLite3Alt REQUIRED)
add_arrow_lib(adbc_driver_sqlite
              SOURCES
              example/sqlite.cc
              SHARED_LINK_LIBS
              ${ADBC_LINK_SHARED} ${SQLite3_LIBRARIES}
              STATIC_LINK_LIBS
              ${ADBC_LINK_STATIC})

            add_dependencies(adbc-adbc-test adbc_driver_sqlite_shared)
