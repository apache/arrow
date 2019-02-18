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

# - Find GFLAGS (gflags.h, libgflags.a, libgflags.so, and libgflags.so.0)
# This module defines
#  GFLAGS_INCLUDE_DIR, directory containing headers
#  GFLAGS_SHARED_LIB, path to libgflags shared library
#  GFLAGS_STATIC_LIB, path to libgflags static library
#  GFLAGS_FOUND, whether gflags has been found

find_package(gflags CONFIG)
set(GFLAGS_FOUND ${gflags_FOUND})
if(GFLAGS_FOUND)
  message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
  string(TOUPPER "${CMAKE_BUILD_TYPE}" _CONFIG)
  get_target_property(_GFLAGS_CONFIGURATIONS gflags IMPORTED_CONFIGURATIONS)
  foreach(_GFLAGS_CONFIG IN LISTS _GFLAGS_CONFIGURATIONS)
    if("${_GFLAGS_CONFIG}" STREQUAL "${_CONFIG}")
      set(_GFLAGS_TARGET_CONFIG "${_GFLAGS_CONFIG}")
    endif()
  endforeach()
  if(NOT _GFLAGS_TARGET_CONFIG)
    list(GET _GFLAGS_CONFIGURATIONS 0 _GFLAGS_TARGET_CONFIG)
  endif()
  if(GFLAGS_SHARED)
    get_target_property(GFLAGS_SHARED_LIB gflags_shared
                        IMPORTED_LOCATION_${_GFLAGS_TARGET_CONFIG})
    message(STATUS "GFlags shared library: ${GFLAGS_SHARED_LIB}")
  endif()
  if(TARGET gflags_static)
    set(GFLAGS_STATIC TRUE)
    get_target_property(GFLAGS_STATIC_LIB gflags_static
                        IMPORTED_LOCATION_${_GFLAGS_TARGET_CONFIG})
    message(STATUS "GFlags static library: ${GFLAGS_STATIC_LIB}")
  endif()
  unset(_CONFIG)
  unset(_GFLAGS_CONFIGURATIONS)
  unset(_GFLAGS_CONFIG)
  unset(_GFLAGS_TARGET_CONFIG)
  return()
endif()

pkg_check_modules(GFLAGS gflags)
if(GFLAGS_FOUND)
  set(GFLAGS_INCLUDE_DIR "${GFLAGS_INCLUDEDIR}")
  message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
  find_library(GFLAGS_SHARED_LIB
               NAMES "${GFLAGS_LIBRARIES}"
               PATHS "${GFLAGS_LIBDIR}"
               NO_DEFAULT_PATH)
  if(GFLAGS_SHARED_LIB)
    message(STATUS "GFlags shared library: ${GFLAGS_SHARED_LIB}")
    add_thirdparty_lib(gflags
                       SHARED_LIB
                       "${GFLAGS_SHARED_LIB}"
                       INCLUDE_DIRECTORIES
                       "${GFLAGS_INCLUDE_DIR}")
    target_compile_definitions(gflags_shared INTERFACE "GFLAGS_IS_A_DLL=1")
    set(GFLAGS_STATIC_LIB FLASE)
    return()
  else()
    set(GFLAGS_FOUND FALSE)
  endif()
endif()

if(GFLAGS_HOME)
  set(GFlags_ROOT "${GFLAGS_HOME}")
endif()
if(CMAKE_VERSION VERSION_LESS 3.12)
  list(INSERT CMAKE_PREFIX_PATH 0 "${GFlags_ROOT}")
endif()

if(MSVC AND NOT DEFINED GFLAGS_MSVC_STATIC_LIB_SUFFIX)
  set(GFLAGS_MSVC_STATIC_LIB_SUFFIX "_static")
endif()

set(GFLAGS_STATIC_LIB_SUFFIX
    "${GFLAGS_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

set(GFLAGS_STATIC_LIB_NAME
    "${CMAKE_STATIC_LIBRARY_PREFIX}gflags${GFLAGS_STATIC_LIB_SUFFIX}")

find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h)
find_library(GFLAGS_SHARED_LIB NAMES gflags)
find_library(GFLAGS_STATIC_LIB NAMES ${GFLAGS_STATIC_LIB_NAME})

if(GFLAGS_INCLUDE_DIR)
  message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
  if(GFLAGS_SHARED_LIB)
    message(STATUS "GFlags shared library: ${GFLAGS_SHARED_LIB}")
    set(GFLAGS_SHARED TRUE)
  else()
    set(GFLAGS_SHARED FALSE)
  endif()
  if(GFLAGS_STATIC_LIB)
    message(STATUS "GFlags static library: ${GFLAGS_STATIC_LIB}")
    set(GFLAGS_STATIC TRUE)
  else()
    set(GFLAGS_STATIC FALSE)
  endif()
  if(GFLAGS_SHARED OR GFLAGS_STATIC)
    set(GFLAGS_FOUND TRUE)
    if(MSVC)
      set(GFLAGS_MSVC_DEPS "shlwapi.lib")
    endif()
    add_thirdparty_lib(gflags
                       SHARED_LIB
                       "${GFLAGS_SHARED_LIB}"
                       STATIC_LIB
                       "${GFLAGS_STATIC_LIB}"
                       INCLUDE_DIRECTORIES
                       "${GFLAGS_INCLUDE_DIR}"
                       DEPS
                       "${GFLAGS_MSVC_DEPS}")
    if(GFLAGS_SHARED)
      target_compile_definitions(gflags_shared INTERFACE "GFLAGS_IS_A_DLL=1")
    endif()
    if(GFLAGS_STATIC)
      target_compile_definitions(gflags_static INTERFACE "GFLAGS_IS_A_DLL=0")
    endif()
  else()
    set(GFLAGS_FOUND FALSE)
  endif()
else()
  set(GFLAGS_FOUND FALSE)
endif()

mark_as_advanced(GFLAGS_INCLUDE_DIR GFLAGS_SHARED_LIB GFLAGS_STATIC_LIB)
