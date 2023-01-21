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

# two parts here, the common test code for other parts to use,
# and an integration test using the other parts of the library.

# common dependencies for tests
add_library(common_test_lib OBJECT "")

include(FetchContent)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2
  GIT_TAG        v2.13.9
)

FetchContent_MakeAvailable(Catch2)

target_link_libraries(common_test_lib PUBLIC Catch2::Catch2)

set_target_properties(common_test_lib PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

target_include_directories(common_test_lib
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_sources(common_test_lib
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/test_allocator.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test_type.hpp
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/catch_runner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test_allocator.cpp
)

add_executable(common_test)

target_link_libraries(common_test common common_test_lib)

set_target_properties(common_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

add_test(
  NAME common_test
  COMMAND common_test
)

target_sources(common_test
  PRIVATE
    quantiles_sorted_view_test.cpp
)

# now the integration test part
add_executable(integration_test)

target_link_libraries(integration_test cpc fi hll kll req sampling theta tuple common_test_lib)

set_target_properties(integration_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

add_test(
  NAME integration_test
  COMMAND integration_test
)

target_sources(integration_test
  PRIVATE
    integration_test.cpp
)
