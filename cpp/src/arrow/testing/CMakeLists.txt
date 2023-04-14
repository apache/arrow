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

arrow_install_all_headers("arrow/testing")

if(ARROW_BUILD_TESTS)
  add_arrow_test(random_test)
endif()

# json_integration_test is two things at the same time:
# - an executable that can be called to answer integration test requests
# - a self-(unit)test for the C++ side of integration testing
if(ARROW_BUILD_TESTS)
  add_arrow_test(json_integration_test EXTRA_LINK_LIBS ${GFLAGS_LIBRARIES})
  add_dependencies(arrow-integration arrow-json-integration-test)
elseif(ARROW_BUILD_INTEGRATION)
  add_executable(arrow-json-integration-test json_integration_test.cc)
  target_link_libraries(arrow-json-integration-test ${ARROW_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} ${ARROW_GTEST_GTEST})

  add_dependencies(arrow-json-integration-test arrow arrow_testing)
  add_dependencies(arrow-integration arrow-json-integration-test)
endif()
