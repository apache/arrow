# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# - Find Thrift (a cross platform RPC lib/tool)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Thrift_ROOT - When set, this path is inspected instead of standard library
#                locations as the root of the Thrift installation.
#                The environment variable THRIFT_HOME overrides this variable.
#
# This module defines
#  THRIFT_VERSION, version string of ant if found
#  THRIFT_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_LIB, THRIFT library
#  THRIFT_FOUND, If false, do not try to use ant

function(EXTRACT_THRIFT_VERSION)
  if(THRIFT_INCLUDE_DIR)
    file(READ "${THRIFT_INCLUDE_DIR}/thrift/config.h" THRIFT_CONFIG_H_CONTENT)
    string(REGEX MATCH "#define PACKAGE_VERSION \"[0-9.]+\"" THRIFT_VERSION_DEFINITION
                 "${THRIFT_CONFIG_H_CONTENT}")
    string(REGEX MATCH "[0-9.]+" THRIFT_VERSION "${THRIFT_VERSION_DEFINITION}")
    set(THRIFT_VERSION "${THRIFT_VERSION}" PARENT_SCOPE)
  else()
    set(THRIFT_VERSION "" PARENT_SCOPE)
  endif()
endfunction(EXTRACT_THRIFT_VERSION)

if(MSVC_TOOLCHAIN AND NOT DEFINED THRIFT_MSVC_LIB_SUFFIX)
  if(NOT ARROW_THRIFT_USE_SHARED)
    if(ARROW_USE_STATIC_CRT)
      set(THRIFT_MSVC_LIB_SUFFIX "mt")
    else()
      set(THRIFT_MSVC_LIB_SUFFIX "md")
    endif()
  endif()
endif()
set(THRIFT_LIB_NAME_BASE "thrift${THRIFT_MSVC_LIB_SUFFIX}")

if(ARROW_THRIFT_USE_SHARED)
  set(THRIFT_LIB_NAMES thrift)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(
      APPEND
        THRIFT_LIB_NAMES
        "${CMAKE_IMPORT_LIBRARY_PREFIX}${THRIFT_LIB_NAME_BASE}${CMAKE_IMPORT_LIBRARY_SUFFIX}"
      )
  endif()
  list(
    APPEND
      THRIFT_LIB_NAMES
      "${CMAKE_SHARED_LIBRARY_PREFIX}${THRIFT_LIB_NAME_BASE}${CMAKE_SHARED_LIBRARY_SUFFIX}"
    )
else()
  set(
    THRIFT_LIB_NAMES
    "${CMAKE_STATIC_LIBRARY_PREFIX}${THRIFT_LIB_NAME_BASE}${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

if(Thrift_ROOT)
  find_library(THRIFT_LIB
               NAMES ${THRIFT_LIB_NAMES}
               PATHS ${Thrift_ROOT}
               PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
  find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h
            PATHS ${Thrift_ROOT}
            PATH_SUFFIXES "include")
  find_program(THRIFT_COMPILER thrift PATHS ${Thrift_ROOT} PATH_SUFFIXES "bin")
  extract_thrift_version()
else()
  # THRIFT-4760: The pkgconfig files are currently only installed when using autotools.
  # Starting with 0.13, they are also installed for the CMake-based installations of Thrift.
  find_package(PkgConfig QUIET)
  pkg_check_modules(THRIFT_PC thrift)
  if(THRIFT_PC_FOUND)
    set(THRIFT_INCLUDE_DIR "${THRIFT_PC_INCLUDEDIR}")

    list(APPEND THRIFT_PC_LIBRARY_DIRS "${THRIFT_PC_LIBDIR}")

    find_library(THRIFT_LIB
                 NAMES ${THRIFT_LIB_NAMES}
                 PATHS ${THRIFT_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH)
    find_program(THRIFT_COMPILER thrift
                 HINTS ${THRIFT_PC_PREFIX}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES "bin")
    set(THRIFT_VERSION ${THRIFT_PC_VERSION})
  else()
    find_library(THRIFT_LIB
                 NAMES ${THRIFT_LIB_NAMES}
                 PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
    find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h PATH_SUFFIXES "include")
    find_program(THRIFT_COMPILER thrift PATH_SUFFIXES "bin")
    extract_thrift_version()
  endif()
endif()

if(THRIFT_COMPILER)
  set(Thrift_COMPILER_FOUND TRUE)
else()
  set(Thrift_COMPILER_FOUND FALSE)
endif()

find_package_handle_standard_args(Thrift
                                  REQUIRED_VARS
                                  THRIFT_LIB
                                  THRIFT_INCLUDE_DIR
                                  VERSION_VAR
                                  THRIFT_VERSION
                                  HANDLE_COMPONENTS)

if(Thrift_FOUND OR THRIFT_FOUND)
  set(Thrift_FOUND TRUE)
  if(ARROW_THRIFT_USE_SHARED)
    add_library(thrift::thrift SHARED IMPORTED)
  else()
    add_library(thrift::thrift STATIC IMPORTED)
  endif()
  set_target_properties(thrift::thrift
                        PROPERTIES IMPORTED_LOCATION "${THRIFT_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_INCLUDE_DIR}")
  if(WIN32 AND NOT MSVC_TOOLCHAIN)
    # We don't need this for Visual C++ because Thrift uses
    # "#pragma comment(lib, "Ws2_32.lib")" in
    # thrift/windows/config.h for Visual C++.
    set_target_properties(thrift::thrift PROPERTIES INTERFACE_LINK_LIBRARIES "ws2_32")
  endif()
endif()
