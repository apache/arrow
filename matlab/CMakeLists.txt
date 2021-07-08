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

# Build the Arrow C++ libraries WITHOUT bundled GoogleTest binaries.
function(BUILD_ARROW)
  include(ExternalProject)

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
  set(ARROW_LIBRARY_DIR "${ARROW_PREFIX}/lib")
  set(ARROW_SHARED_LIB "${ARROW_LIBRARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")

  externalproject_add(arrow_ep
                      SOURCE_DIR "${CMAKE_SOURCE_DIR}/../cpp"
                      BINARY_DIR "${ARROW_BINARY_DIR}"
                      CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX} -DCMAKE_INSTALL_LIBDIR=lib
                      BUILD_BYPRODUCTS ${ARROW_SHARED_LIB})

  file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")

  add_library(arrow_shared SHARED IMPORTED)
  set_target_properties(arrow_shared
                        PROPERTIES IMPORTED_LOCATION ${ARROW_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})

  add_dependencies(arrow_shared arrow_ep)
endfunction()

# Build the Arrow C++ libraries WITH bundled GoogleTest binaries.
function(BUILD_ARROW_AND_GTEST)
  include(ExternalProject)

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
  set(ARROW_LIBRARY_DIR "${ARROW_PREFIX}/lib")
  set(ARROW_SHARED_LIB "${ARROW_LIBRARY_DIR}/libarrow.so")
  set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep_build")

  set(ARROW_GTEST_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")
  set(ARROW_GTEST_INCLUDE_DIR "${ARROW_GTEST_PREFIX}/include")
  set(ARROW_GTEST_LIBRARY_DIR "${ARROW_GTEST_PREFIX}/lib")
  set(ARROW_GTEST_SHARED_LIB "${ARROW_GTEST_LIBRARY_DIR}/libgtest.so")

  set(ARROW_GTEST_MAIN_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")
  set(ARROW_GTEST_MAIN_INCLUDE_DIR "${ARROW_GTEST_MAIN_PREFIX}/include")
  set(ARROW_GTEST_MAIN_LIBRARY_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
  set(ARROW_GTEST_MAIN_SHARED_LIB "${ARROW_GTEST_MAIN_LIBRARY_DIR}/libgtest_main.so")

  externalproject_add(arrow_ep
                      SOURCE_DIR "${CMAKE_SOURCE_DIR}/../cpp"
                      BINARY_DIR "${ARROW_BINARY_DIR}"
                      CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
                                 -DARROW_BUILD_TESTS=ON
                      BUILD_BYPRODUCTS ${ARROW_SHARED_LIB} ${ARROW_GTEST_SHARED_LIB}
                                       ${ARROW_GTEST_MAIN_SHARED_LIB})

  file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
  file(MAKE_DIRECTORY "${ARROW_GTEST_INCLUDE_DIR}")

  add_library(arrow_shared SHARED IMPORTED)
  set_target_properties(arrow_shared
                        PROPERTIES IMPORTED_LOCATION ${ARROW_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})

  add_library(GTest::gtest SHARED IMPORTED)
  set_target_properties(GTest::gtest
                        PROPERTIES IMPORTED_LOCATION ${ARROW_GTEST_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_INCLUDE_DIR})

  add_library(GTest::gtest_main SHARED IMPORTED)
  set_target_properties(GTest::gtest_main
                        PROPERTIES IMPORTED_LOCATION ${ARROW_GTEST_MAIN_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_MAIN_INCLUDE_DIR})
  add_dependencies(arrow_shared arrow_ep)
  add_dependencies(GTest::gtest arrow_ep)
  add_dependencies(GTest::gtest_main arrow_ep)
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

# Only build the MATLAB interface C++ tests if MATLAB_BUILD_TESTS=ON.
if(MATLAB_BUILD_TESTS)
  # Should support custom GTEST_ROOT as well as package managers.
  find_package(GTest)
  if(NOT GTest_FOUND)
    # Trigger an automatic build of the Arrow C++ libraries and bundled GoogleTest binaries.
    build_arrow_and_gtest()
  else()
    # Should support custom ARROW_HOME as well as package managers.
    find_package(Arrow)
    if(NOT Arrow_FOUND)
      # Trigger an automatic build of the Arrow C++ libraries.
      build_arrow()
    endif()
  endif()
else()
  # Should support custom ARROW_HOME as well as package managers.
  find_package(Arrow)
  if(NOT Arrow_FOUND)
    # Trigger an automatic build of the Arrow C++ libraries.
    build_arrow()
  endif()
endif()

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

# #########
# C++ Tests
# #########
# Only build the C++ tests if MATLAB_BUILD_TESTS=ON.
if(MATLAB_BUILD_TESTS)
  enable_testing()

  # Define a test executable target.
  # TODO: Remove the placeholder test. This is just for testing GoogleTest integration.
  add_executable(placeholder_test ${CMAKE_SOURCE_DIR}/src/placeholder_test.cc)
  # Declare a dependency on the GTest::gtest and GTest::gtest_main IMPORTED targets.
  target_link_libraries(placeholder_test GTest::gtest GTest::gtest_main)

  # Add a test target.
  add_test(PlaceholderTestTarget placeholder_test)
endif()
