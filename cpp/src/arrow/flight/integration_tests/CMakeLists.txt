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

add_custom_target(arrow_flight_integration_tests)

if(ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static" AND ARROW_BUILD_STATIC)
  set(ARROW_FLIGHT_INTEGRATION_TEST_LINK_LIBS arrow_flight_sql_static)
else()
  set(ARROW_FLIGHT_INTEGRATION_TEST_LINK_LIBS arrow_flight_sql_shared)
endif()
list(APPEND
     ARROW_FLIGHT_INTEGRATION_TEST_LINK_LIBS
     ${ARROW_FLIGHT_TEST_LINK_LIBS}
     ${GFLAGS_LIBRARIES}
     ${ARROW_GTEST_GTEST})

add_executable(flight-test-integration-server test_integration_server.cc
                                              test_integration.cc)
target_link_libraries(flight-test-integration-server
                      ${ARROW_FLIGHT_INTEGRATION_TEST_LINK_LIBS})

add_executable(flight-test-integration-client test_integration_client.cc
                                              test_integration.cc)
target_link_libraries(flight-test-integration-client
                      ${ARROW_FLIGHT_INTEGRATION_TEST_LINK_LIBS})

add_dependencies(arrow-integration flight-test-integration-client
                 flight-test-integration-server)

if(ARROW_BUILD_TESTS)
  add_arrow_test(flight_integration_test
                 SOURCES
                 flight_integration_test.cc
                 test_integration.cc
                 STATIC_LINK_LIBS
                 ${ARROW_FLIGHT_INTEGRATION_TEST_LINK_LIBS}
                 LABELS
                 "arrow_flight")
endif()
