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

if( NOT "${GFLAGS_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${GFLAGS_HOME}" _native_path )
    list( APPEND _gflags_roots ${_native_path} )
elseif ( GFlags_HOME )
    list( APPEND _gflags_roots ${GFlags_HOME} )
endif()

if (MSVC AND NOT DEFINED GFLAGS_MSVC_STATIC_LIB_SUFFIX)
  set(GFLAGS_MSVC_STATIC_LIB_SUFFIX "_static")
endif()

set(GFLAGS_STATIC_LIB_SUFFIX
  "${GFLAGS_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

set(GFLAGS_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}gflags${GFLAGS_STATIC_LIB_SUFFIX})

message(STATUS "GFLAGS_HOME: ${GFLAGS_HOME}")

if ( _gflags_roots )
  find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h
    PATHS ${_gflags_roots}
    NO_DEFAULT_PATH
    PATH_SUFFIXES "include" )
  find_library(GFLAGS_STATIC_LIB NAMES ${GFLAGS_STATIC_LIB_NAME}
    PATHS ${_gflags_roots}
    NO_DEFAULT_PATH
    PATH_SUFFIXES "lib" )
  find_library(GFLAGS_SHARED_LIB NAMES gflags
    PATHS ${_gflags_roots}
    NO_DEFAULT_PATH
    PATH_SUFFIXES "lib" )
else()
  find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h
    # make sure we don't accidentally pick up a different version
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)
  find_library(GFLAGS_STATIC_LIB ${GFLAGS_STATIC_LIB_NAME}
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)
  find_library(GFLAGS_SHARED_LIB gflags
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)
endif()

if (GFLAGS_INCLUDE_DIR AND GFLAGS_STATIC_LIB)
  set(GFLAGS_FOUND TRUE)
else ()
  set(GFLAGS_FOUND FALSE)
endif ()

mark_as_advanced(
  GFLAGS_INCLUDE_DIR
  GFLAGS_STATIC_LIB
)
