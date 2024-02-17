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

# Build the Arrow C++ libraries using ExternalProject_Add.
function(build_arrow)
  set(options)
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
  set(ARROW_CMAKE_ARGS
      "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}" "-DCMAKE_INSTALL_LIBDIR=lib"
      "-DARROW_BUILD_STATIC=OFF" "-DARROW_CSV=ON")

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
  if(WIN32)
    set(ARROW_BUILD_BYPRODUCTS "${ARROW_IMPORT_LIB}")
  else()
    set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}")
  endif()

  # Building the Arrow C++ libraries requires ExternalProject.
  include(ExternalProject)

  externalproject_add(arrow_ep
                      SOURCE_DIR "${CMAKE_SOURCE_DIR}/../cpp"
                      BINARY_DIR "${ARROW_BINARY_DIR}"
                      CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
                      BUILD_BYPRODUCTS "${ARROW_BUILD_BYPRODUCTS}")

  add_dependencies(${ARROW_LIBRARY_TARGET} arrow_ep)

endfunction()

set(CMAKE_CXX_STANDARD 17)

set(MLARROW_VERSION "16.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" MLARROW_BASE_VERSION "${MLARROW_VERSION}")

project(mlarrow VERSION "${MLARROW_BASE_VERSION}")

# On Windows, set the global variable that determines the MSVC runtime library that is
# used by targets created in this CMakeLists.txt file.
if(WIN32)
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
endif()

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

find_package(Arrow QUIET)
if(NOT Arrow_FOUND)
  build_arrow()
endif()

# MATLAB is Required
find_package(Matlab REQUIRED COMPONENTS MAIN_PROGRAM)

message(STATUS "Mex Library: ${Matlab_MEX_LIBRARY}")
message(STATUS "Mex Include Folder: ${Matlab_INCLUDE_DIRS}")

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
# Install
# ##############################################################################
# Create a subdirectory at CMAKE_INSTALL_PREFIX to install the interface.
set(CMAKE_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/arrow_matlab")

# Build the MATLAB Interface to Arrow.
include(BuildMatlabArrowInterface)

# Install MATLAB source files.
# On macOS, exclude '.DS_Store' files in the source tree from installation.
install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/matlab/"
        DESTINATION ${CMAKE_INSTALL_DIR}
        PATTERN ".DS_Store" EXCLUDE)

get_filename_component(ARROW_SHARED_LIB_DIR ${ARROW_SHARED_LIB} DIRECTORY)
get_filename_component(ARROW_SHARED_LIB_FILENAME ${ARROW_SHARED_LIB} NAME_WE)

if(NOT Arrow_FOUND)
  # If Arrow_FOUND is false, Arrow is built by the arrow_shared target and needs
  # to be copied to CMAKE_PACKAGED_INSTALL_DIR.

  if(APPLE)
    # Install libarrow.dylib (symlink) and the real files it points to.
    # on macOS, we need to match these files: libarrow.dylib
    #                                         libarrow.1300.dylib
    #                                         libarrow.1300.0.0.dylib
    # where the version number might change.
    set(SHARED_LIBRARY_VERSION_REGEX
        "${ARROW_SHARED_LIB_FILENAME}(([.][0-9]+)?([.][0-9]+)?([.][0-9]+)?)${CMAKE_SHARED_LIBRARY_SUFFIX}"
    )
  elseif(UNIX AND NOT CYGWIN)
    # Install libarrow.so (symlink) and the real files it points to.
    # On Linux, we need to match these files: libarrow.so
    #                                         libarrow.so.1200
    #                                         libarrow.so.1200.0.0
    # where the version number might change.
    set(SHARED_LIBRARY_VERSION_REGEX
        "${ARROW_SHARED_LIB_FILENAME}${CMAKE_SHARED_LIBRARY_SUFFIX}(([.][0-9]+)?([.][0-9]+)?([.][0-9]+)?)"
    )
  else()
    set(SHARED_LIBRARY_VERSION_REGEX
        ${ARROW_SHARED_LIB_FILENAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()

  # The subfolders cmake and pkgconfig are excluded as they will be empty.
  # Note: The following CMake Issue suggests enabling an option to exclude all
  # folders that would be empty after installation:
  # https://gitlab.kitware.com/cmake/cmake/-/issues/17122

  set(CMAKE_PACKAGED_INSTALL_DIR "${CMAKE_INSTALL_DIR}/+arrow")

  install(DIRECTORY "${ARROW_SHARED_LIB_DIR}/"
          DESTINATION ${CMAKE_PACKAGED_INSTALL_DIR}
          FILES_MATCHING
          REGEX ${SHARED_LIBRARY_VERSION_REGEX}
          PATTERN "cmake" EXCLUDE
          PATTERN "pkgconfig" EXCLUDE)
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
