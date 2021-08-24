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

# Build the Arrow C++ libraries.
function(build_arrow)
  set(options BUILD_GTEST)
  set(one_value_args)
  set(multi_value_args)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(WIN32)
    set(ARROW_IMPORTED_TYPE IMPORTED_IMPLIB)
    set(ARROW_LIBRARY_SUFFIX ${CMAKE_IMPORT_LIBRARY_SUFFIX})
  else()
    set(ARROW_IMPORTED_TYPE IMPORTED_LOCATION)
    set(ARROW_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
  set(ARROW_LIBRARY_DIR "${ARROW_PREFIX}/lib")
  set(ARROW_SHARED_LIB
      "${ARROW_LIBRARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${ARROW_LIBRARY_SUFFIX}")
  set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")
  set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
                       "-DCMAKE_INSTALL_LIBDIR=lib" "-DARROW_BUILD_STATIC=OFF")
  set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}")

  # Building the Arrow C++ libraries and bundled GoogleTest binaries requires ExternalProject.
  include(ExternalProject)

  if(ARG_BUILD_GTEST)
    enable_gtest()
  endif()

  externalproject_add(arrow_ep
                      SOURCE_DIR "${CMAKE_SOURCE_DIR}/../cpp"
                      BINARY_DIR "${ARROW_BINARY_DIR}"
                      CMAKE_ARGS ${ARROW_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${ARROW_BUILD_BYPRODUCTS})

  set(ARROW_LIBRARY_TARGET arrow_shared)

  # If find_package has already found a valid Arrow installation, then
  # we don't want to link against the newly built arrow_shared library.
  # However, we still need create a library target to trigger building
  # of the arrow_ep target, which will ultimately build the bundled
  # GoogleTest binaries.
  if(Arrow_FOUND)
    set(ARROW_LIBRARY_TARGET arrow_shared_for_gtest)
  endif()

  file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
  add_library(${ARROW_LIBRARY_TARGET} SHARED IMPORTED)
  set_target_properties(${ARROW_LIBRARY_TARGET}
                        PROPERTIES ${ARROW_IMPORTED_TYPE} ${ARROW_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})

  add_dependencies(${ARROW_LIBRARY_TARGET} arrow_ep)

  if(ARG_BUILD_GTEST)
    build_gtest()
  endif()

endfunction()

macro(enable_gtest)
  if(WIN32)
    set(ARROW_GTEST_IMPORTED_TYPE IMPORTED_IMPLIB)
    set(ARROW_GTEST_MAIN_IMPORTED_TYPE IMPORTED_IMPLIB)

    set(ARROW_GTEST_LIBRARY_SUFFIX ${CMAKE_IMPORT_LIBRARY_SUFFIX})
    set(ARROW_GTEST_MAIN_LIBRARY_SUFFIX ${CMAKE_IMPORT_LIBRARY_SUFFIX})
  else()
    set(ARROW_GTEST_IMPORTED_TYPE IMPORTED_LOCATION)
    set(ARROW_GTEST_MAIN_IMPORTED_TYPE IMPORTED_LOCATION)

    set(ARROW_GTEST_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_GTEST_MAIN_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()

  set(ARROW_GTEST_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")
  set(ARROW_GTEST_INCLUDE_DIR "${ARROW_GTEST_PREFIX}/include")
  set(ARROW_GTEST_LIBRARY_DIR "${ARROW_GTEST_PREFIX}/lib")
  set(ARROW_GTEST_SHARED_LIB
      "${ARROW_GTEST_LIBRARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${ARROW_GTEST_LIBRARY_SUFFIX}"
  )

  set(ARROW_GTEST_MAIN_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")
  set(ARROW_GTEST_MAIN_INCLUDE_DIR "${ARROW_GTEST_MAIN_PREFIX}/include")
  set(ARROW_GTEST_MAIN_LIBRARY_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
  set(ARROW_GTEST_MAIN_SHARED_LIB
      "${ARROW_GTEST_MAIN_LIBRARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${ARROW_GTEST_MAIN_LIBRARY_SUFFIX}"
  )

  list(APPEND ARROW_CMAKE_ARGS "-DARROW_BUILD_TESTS=ON")
  list(APPEND ARROW_BUILD_BYPRODUCTS "${ARROW_GTEST_SHARED_LIB}"
       "${ARROW_GTEST_MAIN_SHARED_LIB}")
endmacro()

# Build the GoogleTest binaries that are bundled with the Arrow C++ libraries.
macro(build_gtest)
  set(ARROW_GTEST_INCLUDE_DIR "${ARROW_GTEST_PREFIX}/include")
  set(ARROW_GTEST_MAIN_INCLUDE_DIR "${ARROW_GTEST_MAIN_PREFIX}/include")

  file(MAKE_DIRECTORY "${ARROW_GTEST_INCLUDE_DIR}")

  if(WIN32)
    set(ARROW_GTEST_RUNTIME_DIR "${ARROW_GTEST_PREFIX}/bin")
    set(ARROW_GTEST_MAIN_RUNTIME_DIR "${ARROW_GTEST_MAIN_PREFIX}/bin")
    set(ARROW_GTEST_RUNTIME_SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ARROW_GTEST_MAIN_RUNTIME_SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ARROW_GTEST_RUNTIME_LIB
        "${ARROW_GTEST_RUNTIME_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${ARROW_GTEST_RUNTIME_SUFFIX}"
    )
    set(ARROW_GTEST_MAIN_RUNTIME_LIB
        "${ARROW_GTEST_MAIN_RUNTIME_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${ARROW_GTEST_MAIN_RUNTIME_SUFFIX}"
    )

    # Multi-Configuration generators (e.g. Visual Studio or XCode) place their build artifacts
    # in a subdirectory named ${CMAKE_BUILD_TYPE} by default, where ${CMAKE_BUILD_TYPE} varies
    # depending on the chosen build configuration (e.g. Release or Debug).
    get_property(GENERATOR_IS_MULTI_CONFIG_VALUE GLOBAL
                 PROPERTY GENERATOR_IS_MULTI_CONFIG)
    if(GENERATOR_IS_MULTI_CONFIG_VALUE)
      set(MATLAB_TESTS_DIR "${CMAKE_BINARY_DIR}/$<CONFIG>")
    else()
      set(MATLAB_TESTS_DIR "${CMAKE_BINARY_DIR}")
    endif()

    # We need to copy the gtest and gtest_main runtime DLLs into the directory where the
    # MATLAB C++ tests reside, since Windows requires that runtime DLLs are in the same
    # directory as the executables that depend on them (or on the %PATH%).
    externalproject_add_step(arrow_ep copy
                             COMMAND ${CMAKE_COMMAND} -E make_directory
                                     ${MATLAB_TESTS_DIR}
                             COMMAND ${CMAKE_COMMAND} -E copy ${ARROW_GTEST_RUNTIME_LIB}
                                     ${MATLAB_TESTS_DIR}
                             COMMAND ${CMAKE_COMMAND} -E copy
                                     ${ARROW_GTEST_MAIN_RUNTIME_LIB} ${MATLAB_TESTS_DIR}
                             DEPENDEES install)
  endif()

  add_library(GTest::gtest SHARED IMPORTED)
  set_target_properties(GTest::gtest
                        PROPERTIES ${ARROW_GTEST_IMPORTED_TYPE} ${ARROW_GTEST_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_INCLUDE_DIR})

  add_library(GTest::gtest_main SHARED IMPORTED)
  set_target_properties(GTest::gtest_main
                        PROPERTIES ${ARROW_GTEST_MAIN_IMPORTED_TYPE}
                                   ${ARROW_GTEST_MAIN_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_MAIN_INCLUDE_DIR})

  add_dependencies(GTest::gtest arrow_ep)
  add_dependencies(GTest::gtest_main arrow_ep)
endmacro()

set(CMAKE_CXX_STANDARD 11)

set(MLARROW_VERSION "6.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" MLARROW_BASE_VERSION "${MLARROW_VERSION}")

project(mlarrow VERSION "${MLARROW_BASE_VERSION}")

option(MATLAB_BUILD_TESTS "Build the C++ tests for the MATLAB interface" OFF)

# Grab CMAKE Modules from the CPP interface
set(CPP_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")
if(EXISTS "${CPP_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CPP_CMAKE_MODULES})
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake_modules)

# Only build the MATLAB interface C++ tests if MATLAB_BUILD_TESTS=ON.
if(MATLAB_BUILD_TESTS)
  # find_package(GTest) supports custom GTEST_ROOT as well as package managers.
  find_package(GTest)
  if(NOT GTest_FOUND)
    # find_package(Arrow) supports custom ARROW_HOME as well as package
    # managers.
    find_package(Arrow)
    # Trigger an automatic build of the Arrow C++ libraries and bundled
    # GoogleTest binaries. If a valid Arrow installation was not already
    # found by find_package, then build_arrow will use the Arrow
    # C++ libraries that are built from source.
    build_arrow(BUILD_GTEST)
  else()
    find_package(Arrow)
    if(NOT Arrow_FOUND)
      # Trigger an automatic build of the Arrow C++ libraries.
      build_arrow()
    endif()
  endif()
else()
  find_package(Arrow)
  if(NOT Arrow_FOUND)
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

# ##############################################################################
# C++ Tests
# ##############################################################################
# Only build the C++ tests if MATLAB_BUILD_TESTS=ON.
if(MATLAB_BUILD_TESTS)
  enable_testing()

  # Define a test executable target. TODO: Remove the placeholder test. This is
  # just for testing GoogleTest integration.
  add_executable(placeholder_test ${CMAKE_SOURCE_DIR}/src/placeholder_test.cc)
  # Declare a dependency on the GTest::gtest and GTest::gtest_main IMPORTED
  # targets.
  target_link_libraries(placeholder_test GTest::gtest GTest::gtest_main)

  # Add a test target.
  add_test(PlaceholderTestTarget placeholder_test)
endif()
