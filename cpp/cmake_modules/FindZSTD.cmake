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

# - Find ZSTD (zstd.h, libzstd.a, libzstd.so, and libzstd.so.0)
# This module defines
#  ZSTD_INCLUDE_DIR, directory containing headers
#  ZSTD_STATIC_LIB, path to libzstd static library
#  ZSTD_FOUND, whether zstd has been found

if( NOT "${ZSTD_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${ZSTD_HOME}" _native_path )
    list( APPEND _zstd_roots ${_native_path} )
elseif ( ZStd_HOME )
    list( APPEND _zstd_roots ${ZStd_HOME} )
endif()

if (MSVC AND NOT DEFINED ZSTD_MSVC_STATIC_LIB_SUFFIX)
  set(ZSTD_MSVC_STATIC_LIB_SUFFIX "_static")
endif()

set(ZSTD_STATIC_LIB_SUFFIX
  "${ZSTD_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

set(ZSTD_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}zstd${ZSTD_STATIC_LIB_SUFFIX})

find_path(ZSTD_INCLUDE_DIR NAMES zstd.h
  PATHS ${_zstd_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include" )
find_library(ZSTD_STATIC_LIB NAMES ${ZSTD_STATIC_LIB_NAME} lib${ZSTD_STATIC_LIB_NAME}
  PATHS ${_zstd_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib" )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZSTD REQUIRED_VARS
  ZSTD_STATIC_LIB ZSTD_INCLUDE_DIR)
