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

cmake_minimum_required(VERSION 3.20)

# This function uses the ThirdpartyToolchain CMake module
# in cpp/cmake_modules to automatically download and build GoogleTest.
function(download_and_build_google_test)
  # The ExternalProject module is required for downloading and building external dependencies.
  include(ExternalProject)
  # The ThirdpartyToolchain module automatically builds xsimd by default,
  # so we explicitly disable building of xsimd here.
  set(ARROW_SIMD_LEVEL "NONE")
  set(ARROW_RUNTIME_SIMD_LEVEL "NONE")
  # The ThirdpartyToolchain module must know the location of the Arrow C++ libraries source code.
  set(arrow_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../cpp)
  # The ThirdpartyToolchain module uses ExternalProject to download GoogleTest.
  include(ThirdpartyToolchain)
  # Download and build GoogleTest.
  build_dependency(GTest)
endfunction()

set(CMAKE_CXX_STANDARD 11)

set(MLARROW_VERSION "5.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" MLARROW_BASE_VERSION "${MLARROW_VERSION}")

project(mlarrow VERSION "${MLARROW_BASE_VERSION}")

# Grab CMAKE Modules from the CPP interface
set(CPP_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")
if(EXISTS "${CPP_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CPP_CMAKE_MODULES})
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake_modules)

# Arrow is Required
find_package(Arrow REQUIRED)

# MATLAB is Required
find_package(Matlab REQUIRED)

# Construct the absolute path to featherread's source files
set(featherread_sources featherreadmex.cc feather_reader.cc util/handle_status.cc
                        util/unicode_conversion.cc)
list(TRANSFORM featherread_sources PREPEND ${CMAKE_SOURCE_DIR}/src/)

# Build featherreadmex MEX binary
matlab_add_mex(R2018a
               NAME featherreadmex
               SRC ${featherread_sources}
               LINK_TO arrow_shared)

# Construct the absolute path to featherwrite's source files
set(featherwrite_sources featherwritemex.cc feather_writer.cc util/handle_status.cc
                         util/unicode_conversion.cc)
list(TRANSFORM featherwrite_sources PREPEND ${CMAKE_SOURCE_DIR}/src/)

# Build featherwritemex MEX binary
matlab_add_mex(R2018a
               NAME featherwritemex
               SRC ${featherwrite_sources}
               LINK_TO arrow_shared)

# Ensure the MEX binaries are placed in the src directory on all platforms
if(WIN32)
  set_target_properties(featherreadmex PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                                  $<1:${CMAKE_SOURCE_DIR}/src>)
  set_target_properties(featherwritemex PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                                   $<1:${CMAKE_SOURCE_DIR}/src>)
else()
  set_target_properties(featherreadmex PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                                  $<1:${CMAKE_SOURCE_DIR}/src>)
  set_target_properties(featherwritemex PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                                   $<1:${CMAKE_SOURCE_DIR}/src>)
endif()

# ######################
# GoogleTest Integration
# ######################
# If the user has specified a GTEST_ROOT value, use their pre-built GoogleTest binaries.
# Otherwise, download and build GoogleTest automatically.
if(GTEST_ROOT)
  find_package(GTest REQUIRED)
else()
  download_and_build_google_test()
endif()

# #########
# C++ Tests
# #########
enable_testing()

# Define a test executable target.
# TODO: Remove the placeholder test. This is just for testing GoogleTest integration.
add_executable(placeholder_test ${CMAKE_SOURCE_DIR}/src/placeholder_test.cc)
# Declare a dependency on the GTest::gtest and GTest::gtest_main IMPORTED targets.
target_link_libraries(placeholder_test GTest::gtest GTest::gtest_main)

# Add a test target.
add_test(PlaceholderTestTarget placeholder_test)
