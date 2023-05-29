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
  set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")
  set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
                       "-DCMAKE_INSTALL_LIBDIR=lib" "-DARROW_BUILD_STATIC=OFF")

  if(Arrow_FOUND
     AND NOT GTest_FOUND
     AND ARG_BUILD_GTEST)
    # If find_package has already found a valid Arrow installation, then
    # we don't want to link against the Arrow libraries that will be built
    # from source.
    #
    # However, we still need to create a library target to trigger building
    # of the arrow_ep target, which will ultimately build the bundled
    # GoogleTest binaries.
    add_library(arrow_shared_for_gtest SHARED IMPORTED)
    set(ARROW_LIBRARY_TARGET arrow_shared_for_gtest)
  else()
    add_library(arrow_shared SHARED IMPORTED)
    set(ARROW_LIBRARY_TARGET arrow_shared)

    # Set the runtime shared library (.dll, .so, or .dylib)
    if(WIN32)
      # The shared library (i.e. .dll) is located in the "bin" directory.
      set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/bin")
    else()
      # The shared library (i.e. .so or .dylib) is located in the "lib" directory.
      set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/lib")
    endif()

    set(ARROW_SHARED_LIB_FILENAME
        "${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ARROW_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${ARROW_SHARED_LIB_FILENAME}")

    set_target_properties(arrow_shared PROPERTIES IMPORTED_LOCATION ${ARROW_SHARED_LIB})

    # Set the link-time import library (.lib)
    if(WIN32)
      # The import library (i.e. .lib) is located in the "lib" directory.
      set(ARROW_IMPORT_LIB_FILENAME
          "${CMAKE_IMPORT_LIBRARY_PREFIX}arrow${CMAKE_IMPORT_LIBRARY_SUFFIX}")
      set(ARROW_IMPORT_LIB "${ARROW_PREFIX}/lib/${ARROW_IMPORT_LIB_FILENAME}")

      set_target_properties(arrow_shared PROPERTIES IMPORTED_IMPLIB ${ARROW_IMPORT_LIB})
    endif()

    # Set the include directories
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    set_target_properties(arrow_shared PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                  ${ARROW_INCLUDE_DIR})

    # Set the build byproducts for the ExternalProject build
    # The appropriate libraries need to be guaranteed to be available when linking the test
    # executables.
    if(WIN32)
      set(ARROW_BUILD_BYPRODUCTS "${ARROW_IMPORT_LIB}")
    else()
      set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}")
    endif()
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

  add_dependencies(${ARROW_LIBRARY_TARGET} arrow_ep)

  if(ARG_BUILD_GTEST)
    build_gtest()
  endif()
endfunction()

macro(enable_gtest)
  set(ARROW_GTEST_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")
  set(ARROW_GTEST_MAIN_PREFIX "${ARROW_BINARY_DIR}/googletest_ep-prefix")

  if(WIN32)
    set(ARROW_GTEST_SHARED_LIB_DIR "${ARROW_GTEST_PREFIX}/bin")
    set(ARROW_GTEST_MAIN_SHARED_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/bin")

    set(ARROW_GTEST_LINK_LIB_DIR "${ARROW_GTEST_PREFIX}/lib")
    set(ARROW_GTEST_LINK_LIB
        "${ARROW_GTEST_LINK_LIB_DIR}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )

    set(ARROW_GTEST_MAIN_LINK_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
    set(ARROW_GTEST_MAIN_LINK_LIB
        "${ARROW_GTEST_MAIN_LINK_LIB_DIR}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest_main${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )
  else()
    set(ARROW_GTEST_SHARED_LIB_DIR "${ARROW_GTEST_PREFIX}/lib")
    set(ARROW_GTEST_MAIN_SHARED_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
  endif()

  set(ARROW_GTEST_INCLUDE_DIR "${ARROW_GTEST_PREFIX}/include")
  set(ARROW_GTEST_SHARED_LIB
      "${ARROW_GTEST_SHARED_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )

  set(ARROW_GTEST_MAIN_INCLUDE_DIR "${ARROW_GTEST_MAIN_PREFIX}/include")
  set(ARROW_GTEST_MAIN_SHARED_LIB
      "${ARROW_GTEST_MAIN_SHARED_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )

  list(APPEND ARROW_CMAKE_ARGS "-DARROW_BUILD_TESTS=ON")

  # The appropriate libraries need to be guaranteed to be available when linking the test
  # executables.
  if(WIN32)
    # On Windows, add the gtest link libraries as BUILD_BYPRODUCTS for arrow_ep.
    list(APPEND ARROW_BUILD_BYPRODUCTS "${ARROW_GTEST_LINK_LIB}"
         "${ARROW_GTEST_MAIN_LINK_LIB}")
  else()
    # On Linux and macOS, add the gtest shared libraries as BUILD_BYPRODUCTS for arrow_ep.
    list(APPEND ARROW_BUILD_BYPRODUCTS "${ARROW_GTEST_SHARED_LIB}"
         "${ARROW_GTEST_MAIN_SHARED_LIB}")
  endif()
endmacro()

# Build the GoogleTest binaries that are bundled with the Arrow C++ libraries.
macro(build_gtest)
  file(MAKE_DIRECTORY "${ARROW_GTEST_INCLUDE_DIR}")

  # Create target GTest::gtest
  add_library(GTest::gtest SHARED IMPORTED)
  set_target_properties(GTest::gtest
                        PROPERTIES IMPORTED_LOCATION ${ARROW_GTEST_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_INCLUDE_DIR})
  if(WIN32)
    set_target_properties(GTest::gtest PROPERTIES IMPORTED_IMPLIB ${ARROW_GTEST_LINK_LIB})
  endif()

  add_dependencies(GTest::gtest arrow_ep)

  # Create target GTest::gtest_main
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

set(CMAKE_CXX_STANDARD 17)

set(MLARROW_VERSION "13.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" MLARROW_BASE_VERSION "${MLARROW_VERSION}")

project(mlarrow VERSION "${MLARROW_BASE_VERSION}")

# On Windows, set the global variable that determines the MSVC runtime library that is
# used by targets created in this CMakeLists.txt file.
if(WIN32)
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
endif()

option(MATLAB_BUILD_TESTS "Build the C++ tests for the MATLAB interface" OFF)

# Add tools/cmake directory to the CMAKE_MODULE_PATH.
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/tools/cmake)

# Configure find_package to look for arrow-config.cmake in Config mode
# underneath the Arrow installation directory hierarchy specified
# by ARROW_HOME or Arrow_ROOT.
# NOTE: ARROW_HOME is supported for backwards compatibility.
if(ARROW_HOME AND NOT Arrow_ROOT)
  set(Arrow_ROOT "${ARROW_HOME}")
endif()

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
    find_package(Arrow QUIET)
    # Trigger an automatic build of the Arrow C++ libraries and bundled
    # GoogleTest binaries. If a valid Arrow installation was not already
    # found by find_package, then build_arrow will use the Arrow
    # C++ libraries that are built from source.
    build_arrow(BUILD_GTEST)
  else()
    # On Windows, IMPORTED_LOCATION needs to be set to indicate where the shared
    # libraries live when GTest is found.
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

    find_package(Arrow QUIET)
    if(NOT Arrow_FOUND)
      # Trigger an automatic build of the Arrow C++ libraries.
      build_arrow()
    endif()
  endif()

else()
  find_package(Arrow QUIET)
  if(NOT Arrow_FOUND)
    build_arrow()
  endif()
endif()

# MATLAB is Required
find_package(Matlab REQUIRED COMPONENTS MAIN_PROGRAM)

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

# Declare a dependency on the MEX shared library (libmex.so/dylib/dll).
target_link_libraries(arrow_matlab ${Matlab_MX_LIBRARY})
target_link_libraries(arrow_matlab ${Matlab_MEX_LIBRARY})

# Include the MATLAB MEX headers.
target_include_directories(arrow_matlab PRIVATE ${Matlab_INCLUDE_DIRS})
target_include_directories(arrow_matlab PRIVATE ${CPP_SOURCE_DIR})
target_include_directories(arrow_matlab PRIVATE ${ARROW_INCLUDE_DIR})
target_compile_definitions(arrow_matlab PRIVATE ARROW_MATLAB_EXPORTING)

set(mexcall_sources mex/call.cc)
list(TRANSFORM mexcall_sources PREPEND ${CPP_SOURCE_DIR}/arrow/matlab/)

# Build call MEX binary.
matlab_add_mex(R2018a
               NAME mexcall
               OUTPUT_NAME call
               SRC ${mexcall_sources}
               LINK_TO arrow_matlab)

target_include_directories(mexcall PRIVATE ${CPP_SOURCE_DIR})

# ARROW_SHARED_LIB
# On Windows, this will be ARROW_HOME/bin/arrow.dll and on Linux and macOS, it is the arrow.so/dylib in the newly built arrow_shared library.
if(NOT Arrow_FOUND)
  message(STATUS "ARROW_SHARED_LIB will be set using IMPORTED_LOCATION value when building."
  )
  get_target_property(ARROW_SHARED_LIB arrow_shared IMPORTED_LOCATION)
else()
  # If not building Arrow, ARROW_SHARED_LIB derived from ARROW_PREFIX set to the ARROW_HOME specified with cmake would be non-empty.
  message(STATUS "ARROW_SHARED_LIB: ${ARROW_SHARED_LIB}")
endif()

# ARROW_LINK_LIB
# On Windows, we use the arrow.lib for linking arrow_matlab against the Arrow C++ library.
# The location of arrow.lib is previously saved in IMPORTED_IMPLIB.
if(WIN32)
  # If not building Arrow, IMPORTED_IMPLIB will be empty.
  # Then set ARROW_LINK_LIB to ARROW_IMPORT_LIB which would have been derived from ARROW_PREFIX set to the ARROW_HOME specified with cmake. This will avoid the ARROW_LINK_LIB set to NOTFOUND error.
  # The ARROW_IMPORT_LIB should be ARROW_HOME/lib/arrow.lib on Windows.
  if(NOT Arrow_FOUND)
    message(STATUS "ARROW_LINK_LIB will be set using IMPORTED_IMPLIB value when building."
    )
    get_target_property(ARROW_LINK_LIB arrow_shared IMPORTED_IMPLIB)
  else()
    set(ARROW_LINK_LIB "${ARROW_IMPORT_LIB}")
    message(STATUS "Setting ARROW_LINK_LIB to ARROW_IMPORT_LIB: ${ARROW_IMPORT_LIB}, which is derived from the ARROW_HOME provided."
    )
  endif()
else()
  # On Linux and macOS, it is the arrow.so/dylib in the newly built arrow_shared library used for linking.
  # On Unix, this is the same as ARROW_SHARED_LIB.
  message(STATUS "Setting ARROW_LINK_LIB to ARROW_SHARED_LIB as they are same on Unix.")
  set(ARROW_LINK_LIB "${ARROW_SHARED_LIB}")
endif()

# ARROW_INCLUDE_DIR should be set so that header files in the include directory can be found correctly.
# The value of ARROW_INCLUDE_DIR should be ARROW_HOME/include on all platforms.
if(NOT Arrow_FOUND)
  message(STATUS "ARROW_INCLUDE_DIR will be set using INTERFACE_INCLUDE_DIRECTORIES value when building."
  )
  get_target_property(ARROW_INCLUDE_DIR arrow_shared INTERFACE_INCLUDE_DIRECTORIES)
else()
  # If not building Arrow, ARROW_INCLUDE_DIR derived from ARROW_PREFIX set to the ARROW_HOME specified with cmake would be non-empty.
  message(STATUS "ARROW_INCLUDE_DIR: ${ARROW_INCLUDE_DIR}")
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

  # Add test targets for C++ tests.
  add_test(PlaceholderTestTarget placeholder_test)
  add_test(CheckNumArgsTestTarget mex_util_test)

  # On macOS, add the directory of libarrow.dylib to the $DYLD_LIBRARY_PATH for
  # running CheckNumArgsTestTarget.
  if(APPLE)
    get_target_property(ARROW_SHARED_LIB arrow_shared IMPORTED_LOCATION)
    get_filename_component(ARROW_SHARED_LIB_DIR ${ARROW_SHARED_LIB} DIRECTORY)

    set_tests_properties(CheckNumArgsTestTarget
                         PROPERTIES ENVIRONMENT
                                    "DYLD_LIBRARY_PATH=${ARROW_SHARED_LIB_DIR}")
  endif()

  # On Windows:
  # Add the directory of gtest.dll and gtest_main.dll to the %PATH% for running
  # all tests.
  # Add the directory of libmx.dll, libmex.dll, and libarrow.dll to the %PATH% for running
  # CheckNumArgsTestTarget.
  # Note: When appending to the path using set_test_properties' ENVIRONMENT property,
  #       make sure that we escape ';' to prevent CMake from interpreting the input as
  #       a list of strings.
  if(WIN32)
    get_target_property(GTEST_SHARED_LIB GTest::gtest IMPORTED_LOCATION)
    get_filename_component(GTEST_SHARED_LIB_DIR ${GTEST_SHARED_LIB} DIRECTORY)

    get_target_property(GTEST_MAIN_SHARED_LIB GTest::gtest_main IMPORTED_LOCATION)
    get_filename_component(GTEST_MAIN_SHARED_LIB_DIR ${GTEST_MAIN_SHARED_LIB} DIRECTORY)

    set_tests_properties(PlaceholderTestTarget
                         PROPERTIES ENVIRONMENT
                                    "PATH=${GTEST_SHARED_LIB_DIR}\;${GTEST_MAIN_SHARED_LIB_DIR}\;$ENV{PATH}"
    )

    get_target_property(ARROW_SHARED_LIB arrow_shared IMPORTED_LOCATION)
    get_filename_component(ARROW_SHARED_LIB_DIR ${ARROW_SHARED_LIB} DIRECTORY)

    set(MATLAB_DLL_DEPENDENCIES_DIR "${Matlab_ROOT_DIR}/bin/win64")

    set_tests_properties(CheckNumArgsTestTarget
                         PROPERTIES ENVIRONMENT
                                    "PATH=${ARROW_SHARED_LIB_DIR}\;${MATLAB_DLL_DEPENDENCIES_DIR}\;${GTEST_SHARED_LIB_DIR}\;${GTEST_MAIN_SHARED_LIB_DIR}\;$ENV{PATH}"
    )
  endif()
endif()

# ##############################################################################
# Install
# ##############################################################################
# Create a subdirectory at CMAKE_INSTALL_PREFIX to install the interface.
set(CMAKE_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/arrow_matlab")

if(MATLAB_ARROW_INTERFACE)
  include(BuildMatlabArrowInterface)
endif()

# Create a package hierarchy at CMAKE_INSTALL_PREFIX to install the mex function
# and dependencies.
set(CMAKE_PACKAGED_INSTALL_DIR "${CMAKE_INSTALL_DIR}/+arrow/+cpp")

# Install MATLAB source files.
# On macOS, exclude '.DS_Store' files in the source tree from installation.
install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/matlab/"
        DESTINATION ${CMAKE_INSTALL_DIR}
        PATTERN ".DS_Store" EXCLUDE)

# Install arrow_matlab and mexcall.
# Use the RUNTIME output artifact keyword for Windows.
# Use the LIBRARY output artifact keyword for macOS and Linux.
install(TARGETS arrow_matlab mexcall
        RUNTIME DESTINATION ${CMAKE_PACKAGED_INSTALL_DIR}
        LIBRARY DESTINATION ${CMAKE_PACKAGED_INSTALL_DIR})

get_filename_component(ARROW_SHARED_LIB_DIR ${ARROW_SHARED_LIB} DIRECTORY)
get_filename_component(ARROW_SHARED_LIB_FILENAME ${ARROW_SHARED_LIB} NAME_WE)

# On macOS, use the RPATH values below for runtime dependency resolution. This enables
# relocation of the installation directory.
if(APPLE)
  # Setting INSTALL_RPATH_USE_LINK_PATH to true will add the paths to external dependencies
  # to the RPATH of arrow_matlab and mexcall, including the MATLAB dependencies.
  # If Arrow_FOUND is true, this also includes the path to arrow_shared.
  set_target_properties(arrow_matlab mexcall PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)

  # Add @loader_path to the RPATH of mexcall so that libarrow_matlab.dylib can be found
  # at runtime.
  set_target_properties(mexcall PROPERTIES INSTALL_RPATH "@loader_path")

  if(NOT Arrow_FOUND)
    # If Arrow_FOUND is false, Arrow is built by the arrow_shared target and needs
    # to be copied to CMAKE_PACKAGED_INSTALL_DIR. The DIRECTORY install command is used to
    # install libarrow.dylib (symlink) and the real files it points to.
    #
    # The subfolders cmake and pkgconfig are excluded as they will be empty.
    # Note: The following CMake Issue suggests enabling an option to exclude all
    # folders that would be empty after installation:
    # https://gitlab.kitware.com/cmake/cmake/-/issues/17122
    install(DIRECTORY "${ARROW_SHARED_LIB_DIR}/"
            DESTINATION ${CMAKE_PACKAGED_INSTALL_DIR}
            FILES_MATCHING
            REGEX "${ARROW_SHARED_LIB_FILENAME}\\..*dylib"
            PATTERN "cmake" EXCLUDE
            PATTERN "pkgconfig" EXCLUDE)

    # Add @loader_path to the RPATH of arrow_matlab so that libarrow.dylib can be found
    # at runtime.
    set_target_properties(arrow_matlab PROPERTIES INSTALL_RPATH "@loader_path")
  endif()
endif()

# On Linux, use the RUNPATH values below for runtime dependency resolution. This enables
# relocation of the installation directory.
if(UNIX
   AND NOT APPLE
   AND NOT CYGWIN)
  # Setting INSTALL_RPATH_USE_LINK_PATH to true will add the paths to external dependencies
  # to the RUNPATH of arrow_matlab and mexcall, including the MATLAB dependencies.
  # If Arrow_FOUND is true, this also includes the path to arrow_shared.
  set_target_properties(arrow_matlab mexcall PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)

  # Add $ORIGIN to the RUNPATH of mexcall so that libarrow_matlab.so can be found
  # at runtime.
  set_target_properties(mexcall PROPERTIES INSTALL_RPATH $ORIGIN)

  if(NOT Arrow_FOUND)
    # If Arrow_FOUND is false, Arrow is built by the arrow_shared target and needs
    # to be copied to CMAKE_PACKAGED_INSTALL_DIR. The DIRECTORY install command is used to
    # install libarrow.so (symlink) and the real files it points to.
    #
    # The subfolders cmake and pkgconfig are excluded as they will be empty.
    # Note: The following CMake Issue suggests enabling an option to exclude all
    # folders that would be empty after installation:
    # https://gitlab.kitware.com/cmake/cmake/-/issues/17122
    install(DIRECTORY "${ARROW_SHARED_LIB_DIR}/"
            DESTINATION ${CMAKE_PACKAGED_INSTALL_DIR}
            FILES_MATCHING
            REGEX "${ARROW_SHARED_LIB_FILENAME}\\.so.*"
            PATTERN "cmake" EXCLUDE
            PATTERN "pkgconfig" EXCLUDE)

    # Add $ORIGIN to the RUNPATH of arrow_matlab so that libarrow.so can be found
    # at runtime.
    set_target_properties(arrow_matlab PROPERTIES INSTALL_RPATH $ORIGIN)
  endif()
endif()

# MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE toggles whether an addpath command to add the install
# directory path to the MATLAB Search Path is added to the startup.m file located in the MATLAB
# userpath directory.
option(MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE
       "Sets whether the path to the install directory should be added to the startup.m file located at the MATLAB userpath"
       OFF)

# If MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE is specified ON and MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH
# is not specified, then set MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH=OFF.
if(MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE AND NOT DEFINED
                                              MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH)
  set(MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH OFF)
endif()

# MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH toggles whether the path to the install directory should
# be directly added to the MATLAB Search Path.
option(MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH
       "Sets whether the path to the install directory should be directly added to the MATLAB Search Path"
       ON)

if(MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH OR MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE)
  set(TOOLS_DIR "${CMAKE_SOURCE_DIR}/tools")

  # Pass Matlab_MAIN_PROGRAM, TOOLS_DIR, and INSTALL_DIR to the install step
  # code/script execution scope.
  install(CODE "set(Matlab_MAIN_PROGRAM \"${Matlab_MAIN_PROGRAM}\")")
  install(CODE "set(TOOLS_DIR \"${TOOLS_DIR}\")")
  install(CODE "set(INSTALL_DIR \"${CMAKE_INSTALL_DIR}\")")
  install(CODE "set(MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE \"${MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE}\")"
  )
  install(CODE "set(MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH \"${MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH}\")"
  )

  # Call the CMake script that runs the MATLAB function to add the install directory
  # to the MATLAB Search Path or add a command to the MATLAB startup file to add the
  # install directory to the MATLAB Search Path.
  install(SCRIPT "${TOOLS_DIR}/UpdateMatlabSearchPath.cmake")
endif()
