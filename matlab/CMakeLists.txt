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

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")

  if(WIN32)
    # On Windows, the link time suffix (.lib) and runtime library suffixes (.dll) are different.
    set(ARROW_LINK_SUFFIX ${CMAKE_IMPORT_LIBRARY_SUFFIX})
    # The shared library is located in the "bin" directory.
    set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/bin")
  else()
    # On Linux and macOS, the link time suffix and runtime suffixes are consistent, (.so and .dylib).
    set(ARROW_LINK_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
    # The shared library is located in the "lib" directory.
    set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/lib")
  endif()

  set(ARROW_LINK_LIB_FILENAME "${CMAKE_IMPORT_LIBRARY_PREFIX}arrow${ARROW_LINK_SUFFIX}")
  set(ARROW_LINK_LIB "${ARROW_PREFIX}/lib/${ARROW_LINK_LIB_FILENAME}")

  set(ARROW_SHARED_LIB_FILENAME
      "${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${ARROW_SHARED_LIB_FILENAME}")

  set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")
  set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
                       "-DCMAKE_INSTALL_LIBDIR=lib" "-DARROW_BUILD_STATIC=OFF")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

  # On Windows, add the Arrow link library as a BUILD_BYPRODUCTS for arrow_ep.
  # On Linux and macOS, add the Arrow shared library as a BUILD_BYPRODUCTS for arrow_ep.
  # When building with Ninja, the link/shared libraries need to be guaranteed to be available for linking the test
  # executables.
  if(WIN32)
    set(ARROW_BUILD_BYPRODUCTS "${ARROW_LINK_LIB}")
  else()
    set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}")
  endif()

  # Building the Arrow C++ libraries and bundled GoogleTest binaries requires ExternalProject.
  include(ExternalProject)

  if(ARG_BUILD_GTEST)
    enable_gtest()
  endif()

  externalproject_add(arrow_ep
                      SOURCE_DIR "${CMAKE_SOURCE_DIR}/../cpp"
                      BINARY_DIR "${ARROW_BINARY_DIR}"
                      CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
                      BUILD_BYPRODUCTS "${ARROW_BUILD_BYPRODUCTS}")

  set(ARROW_LIBRARY_TARGET arrow_shared)

  # If find_package has already found a valid Arrow installation, then
  # we don't want to link against the newly built arrow_shared library.
  # However, we still need to create a library target to trigger building
  # of the arrow_ep target, which will ultimately build the bundled
  # GoogleTest binaries.
  if(Arrow_FOUND)
    set(ARROW_LIBRARY_TARGET arrow_shared_for_gtest)
  endif()

  file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
  add_library(${ARROW_LIBRARY_TARGET} SHARED IMPORTED)
  set_target_properties(${ARROW_LIBRARY_TARGET}
                        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
                                   IMPORTED_LOCATION ${ARROW_SHARED_LIB})
  if(WIN32)
    set_target_properties(${ARROW_LIBRARY_TARGET} PROPERTIES IMPORTED_IMPLIB
                                                             ${ARROW_LINK_LIB})
  endif()

  add_dependencies(${ARROW_LIBRARY_TARGET} arrow_ep)

  if(ARG_BUILD_GTEST)
    build_gtest()
  endif()
endfunction()

macro(enable_gtest)
  set(ARROW_GTEST_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")
  set(ARROW_GTEST_MAIN_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")

  if(WIN32)
    set(ARROW_GTEST_LINK_SUFFIX ${CMAKE_IMPORT_LIBRARY_SUFFIX})
    set(ARROW_GTEST_SHARED_LIB_DIR "${ARROW_GTEST_PREFIX}/bin")
    set(ARROW_GTEST_MAIN_LINK_SUFFIX ${CMAKE_IMPORT_LIBRARY_SUFFIX})
    set(ARROW_GTEST_MAIN_SHARED_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/bin")
  else()
    set(ARROW_GTEST_LINK_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_GTEST_SHARED_LIB_DIR "${ARROW_GTEST_PREFIX}/lib")
    set(ARROW_GTEST_MAIN_LINK_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_GTEST_MAIN_SHARED_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
  endif()

  set(ARROW_GTEST_INCLUDE_DIR "${ARROW_GTEST_PREFIX}/include")
  set(ARROW_GTEST_LINK_LIB_DIR "${ARROW_GTEST_PREFIX}/lib")
  set(ARROW_GTEST_LINK_LIB
      "${ARROW_GTEST_LINK_LIB_DIR}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest${ARROW_GTEST_LINK_SUFFIX}"
  )
  set(ARROW_GTEST_SHARED_LIB
      "${ARROW_GTEST_SHARED_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )

  set(ARROW_GTEST_MAIN_INCLUDE_DIR "${ARROW_GTEST_MAIN_PREFIX}/include")
  set(ARROW_GTEST_MAIN_LINK_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
  set(ARROW_GTEST_MAIN_LINK_LIB
      "${ARROW_GTEST_MAIN_LINK_LIB_DIR}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest_main${ARROW_GTEST_MAIN_LINK_SUFFIX}"
  )
  set(ARROW_GTEST_MAIN_SHARED_LIB
      "${ARROW_GTEST_MAIN_SHARED_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )

  list(APPEND ARROW_CMAKE_ARGS "-DARROW_BUILD_TESTS=ON")

  # On Windows, add the gtest link libraries as BUILD_BYPRODUCTS for arrow_ep.
  # On Linux and macOS, add the gtest shared libraries as BUILD_BYPRODUCTS for arrow_ep.
  # When building with Ninja, The link/shared libraries need to be guaranteed to be available for linking the test
  # executables.
  if(WIN32)
    list(APPEND ARROW_BUILD_BYPRODUCTS "${ARROW_GTEST_LINK_LIB}"
         "${ARROW_GTEST_MAIN_LINK_LIB}")
  else()
    list(APPEND ARROW_BUILD_BYPRODUCTS "${ARROW_GTEST_SHARED_LIB}"
         "${ARROW_GTEST_MAIN_SHARED_LIB}")
  endif()
endmacro()

# Build the GoogleTest binaries that are bundled with the Arrow C++ libraries.
macro(build_gtest)
  file(MAKE_DIRECTORY "${ARROW_GTEST_INCLUDE_DIR}")

  add_library(GTest::gtest SHARED IMPORTED)
  set_target_properties(GTest::gtest
                        PROPERTIES IMPORTED_LOCATION ${ARROW_GTEST_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_INCLUDE_DIR})
  if(WIN32)
    set_target_properties(GTest::gtest PROPERTIES IMPORTED_IMPLIB ${ARROW_GTEST_LINK_LIB})
  endif()

  add_dependencies(GTest::gtest arrow_ep)

  add_library(GTest::gtest_main SHARED IMPORTED)
  set_target_properties(GTest::gtest_main
                        PROPERTIES IMPORTED_LOCATION ${ARROW_GTEST_MAIN_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_MAIN_INCLUDE_DIR})
  if(WIN32)
    set_target_properties(GTest::gtest_main PROPERTIES IMPORTED_IMPLIB
                                                       ${ARROW_GTEST_MAIN_LINK_LIB})
  endif()

  add_dependencies(GTest::gtest_main arrow_ep)
endmacro()

set(CMAKE_CXX_STANDARD 11)

set(MLARROW_VERSION "7.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" MLARROW_BASE_VERSION "${MLARROW_VERSION}")

project(mlarrow VERSION "${MLARROW_BASE_VERSION}")

# On Windows, set the global variable that determines the MSVC runtime library that is
# used by targets created in this CMakeLists.txt file.
if(WIN32)
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
endif()

option(MATLAB_BUILD_TESTS "Build the C++ tests for the MATLAB interface" OFF)

# Grab CMAKE Modules from the CPP interface
set(CPP_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")
if(EXISTS "${CPP_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CPP_CMAKE_MODULES})
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake_modules)

# Multi-Configuration generators (e.g. Visual Studio or XCode) place their build artifacts
# in a subdirectory named ${CMAKE_BUILD_TYPE} by default, where ${CMAKE_BUILD_TYPE} varies
# depending on the chosen build configuration (e.g. Release or Debug).
get_property(GENERATOR_IS_MULTI_CONFIG_VALUE GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if(GENERATOR_IS_MULTI_CONFIG_VALUE)
  set(MATLAB_BUILD_OUTPUT_DIR "${CMAKE_BINARY_DIR}/$<CONFIG>")
else()
  set(MATLAB_BUILD_OUTPUT_DIR "${CMAKE_BINARY_DIR}")
endif()

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
    # GTest is found, on Windows, IMPORTED_LOCATION needs to be set to indicate where the shared
    # libraries live
    if(WIN32)
      set(GTEST_SHARED_LIB_DIR "${GTEST_ROOT}/bin")
      set(GTEST_SHARED_LIBRARY_FILENAME
          "${CMAKE_SHARED_LIBRARY_PREFIX}gtest${CMAKE_SHARED_LIBRARY_SUFFIX}")
      set(GTEST_SHARED_LIBRARY_LIB
          "${GTEST_SHARED_LIB_DIR}/${GTEST_SHARED_LIBRARY_FILENAME}")

      set(GTEST_MAIN_SHARED_LIB_DIR "${GTEST_ROOT}/bin")
      set(GTEST_MAIN_SHARED_LIBRARY_FILENAME
          "${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${CMAKE_SHARED_LIBRARY_SUFFIX}")
      set(GTEST_MAIN_SHARED_LIBRARY_LIB
          "${GTEST_MAIN_SHARED_LIB_DIR}/${GTEST_MAIN_SHARED_LIBRARY_FILENAME}")

      set_target_properties(GTest::gtest PROPERTIES IMPORTED_LOCATION
                                                    "${GTEST_SHARED_LIBRARY_LIB}")

      set_target_properties(GTest::gtest_main
                            PROPERTIES IMPORTED_LOCATION
                                       "${GTEST_MAIN_SHARED_LIBRARY_LIB}")
    endif()

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

# On Windows: copy arrow.dll into the directory where the MATLAB C++ tests reside,
# since Windows require runtime DLLs that are depended on by executables to be in
# the same directory (or on the %PATH%).
if(WIN32)
  get_property(ARROW_SHARED_LIB
               TARGET arrow_shared
               PROPERTY IMPORTED_LOCATION)

  add_custom_target(copy_arrow_to_build_output_dir ALL
                    COMMAND ${CMAKE_COMMAND} -E make_directory ${MATLAB_BUILD_OUTPUT_DIR}
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${ARROW_SHARED_LIB}
                            ${MATLAB_BUILD_OUTPUT_DIR}
                    COMMENT "Start copying arrow.dll from ${ARROW_SHARED_LIB} to ${MATLAB_BUILD_OUTPUT_DIR}"
  )
  add_dependencies(copy_arrow_to_build_output_dir arrow_shared)
endif()

# MATLAB is Required
find_package(Matlab REQUIRED)

message(STATUS "Mex Library: ${Matlab_MEX_LIBRARY}")
message(STATUS "Mex Include Folder: ${Matlab_INCLUDE_DIRS}")

set(CPP_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/cpp)
set(MATLAB_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab)

set(arrow_matlab_sources
    mex/mex_util.cc
    feather/feather_reader.cc
    feather/feather_writer.cc
    feather/feather_functions.cc
    feather/util/handle_status.cc
    feather/util/unicode_conversion.cc)
list(TRANSFORM arrow_matlab_sources PREPEND ${CPP_SOURCE_DIR}/arrow/matlab/)

add_library(arrow_matlab SHARED ${arrow_matlab_sources})

# Declare a dependency on arrow_shared (libarrow.so/dylib/dll).
target_link_libraries(arrow_matlab arrow_shared)

# On macOS, rpaths for linking imported targets are not automatically added to linked libraries
if(APPLE)
  get_property(ARROW_SHARED_IMPORTED_LOCATION
               TARGET arrow_shared
               PROPERTY IMPORTED_LOCATION)
  get_filename_component(ARROW_SHARED_LIBRARY_DIR ${ARROW_SHARED_IMPORTED_LOCATION}
                         DIRECTORY)
  target_link_options(arrow_matlab PUBLIC -Wl,-rpath,${ARROW_SHARED_LIBRARY_DIR})
endif()

# Declare a dependency on the MEX shared library (libmex.so/dylib/dll).
target_link_libraries(arrow_matlab ${Matlab_MX_LIBRARY})
target_link_libraries(arrow_matlab ${Matlab_MEX_LIBRARY})

# Include the MATLAB MEX headers.
target_include_directories(arrow_matlab PRIVATE ${Matlab_INCLUDE_DIRS})
target_include_directories(arrow_matlab PRIVATE ${CPP_SOURCE_DIR})
target_include_directories(arrow_matlab PRIVATE ${ARROW_INCLUDE_DIR})
target_compile_definitions(arrow_matlab PRIVATE ARROW_MATLAB_EXPORTING)

set(mexfcn_sources mex/mexfcn.cc)
list(TRANSFORM mexfcn_sources PREPEND ${CPP_SOURCE_DIR}/arrow/matlab/)

# Build mexfcn MEX binary
matlab_add_mex(R2018a
               NAME mexfcn
               SRC ${mexfcn_sources}
               LINK_TO arrow_matlab)

target_include_directories(mexfcn PRIVATE ${CPP_SOURCE_DIR})

# ##############################################################################
# C++ Tests
# ##############################################################################
# Only build the C++ tests if MATLAB_BUILD_TESTS=ON.
if(MATLAB_BUILD_TESTS)
  enable_testing()

  # Define a test executable target. TODO: Remove the placeholder test. This is
  # just for testing GoogleTest integration.
  add_executable(placeholder_test ${CMAKE_SOURCE_DIR}/src/placeholder_test.cc)
  add_executable(mex_util_test ${CPP_SOURCE_DIR}/arrow/matlab/mex/mex_util_test.cc)

  # Declare a dependency on the GTest::gtest and GTest::gtest_main IMPORTED
  # targets.
  target_link_libraries(placeholder_test GTest::gtest GTest::gtest_main)

  # Declare a dependency on the GTest::gtest and GTest::gtest_main IMPORTED
  # targets.
  target_link_libraries(mex_util_test GTest::gtest GTest::gtest_main)
  target_link_libraries(mex_util_test arrow_matlab)

  # Include the MATLAB MEX headers.
  target_include_directories(mex_util_test PRIVATE ${Matlab_INCLUDE_DIRS})
  # Include the C++ source headers.
  target_include_directories(mex_util_test PRIVATE ${CPP_SOURCE_DIR})

  # Add a test target.
  add_test(PlaceholderTestTarget placeholder_test)
  add_test(CheckNumArgsTestTarget mex_util_test)

  # For Windows:
  # Add the directory of gtest.dll and gtest_main.dll to the %PATH% for running
  # all tests.
  # Add the directory of libmx.dll and libmex.dll to the %PATH% for running
  # CheckNumArgsTestTarget.
  # Note: When appending to the path using set_test_properties' ENVIRONNENT property,
  #       make sure that we escape ';' to avoid cmake from interpreting the input as
  #       a list of strings.
  if(WIN32)
    get_property(GTEST_SHARED_LIB
                 TARGET GTest::gtest
                 PROPERTY IMPORTED_LOCATION)
    get_filename_component(GTEST_SHARED_LIB_DIR ${GTEST_SHARED_LIB} DIRECTORY)

    get_property(GTEST_MAIN_SHARED_LIB
                 TARGET GTest::gtest_main
                 PROPERTY IMPORTED_LOCATION)
    get_filename_component(GTEST_MAIN_SHARED_LIB_DIR ${GTEST_MAIN_SHARED_LIB} DIRECTORY)

    set_tests_properties(PlaceholderTestTarget
                         PROPERTIES ENVIRONMENT
                                    "PATH=${GTEST_SHARED_LIB_DIR}\;${GTEST_MAIN_SHARED_LIB_DIR}\;$ENV{PATH}"
    )

    set(MATLAB_DLL_DEPENDENCIES_DIR "${Matlab_ROOT_DIR}/bin/win64")

    set_tests_properties(CheckNumArgsTestTarget
                         PROPERTIES ENVIRONMENT
                                    "PATH=${MATLAB_DLL_DEPENDENCIES_DIR}\;${GTEST_SHARED_LIB_DIR}\;${GTEST_MAIN_SHARED_LIB_DIR}\;$ENV{PATH}"
    )
  endif()
endif()
