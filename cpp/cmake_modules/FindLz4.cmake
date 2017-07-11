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

# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.0)
# This module defines
#  LZ4_INCLUDE_DIR, directory containing headers
#  LZ4_SHARED_LIB, path to liblz4 shared library
#  LZ4_STATIC_LIB, path to liblz4 static library
#  LZ4_FOUND, whether lz4 has been found

if( NOT "${LZ4_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${LZ4_HOME}" _native_path )
    list( APPEND _lz4_roots ${_native_path} )
elseif ( Lz4_HOME )
    list( APPEND _lz4_roots ${Lz4_HOME} )
endif()

if (MSVC AND NOT LZ4_MSVC_STATIC_LIB_SUFFIX)
  set(LZ4_MSVC_STATIC_LIB_SUFFIX "_static")
endif()

set(LZ4_STATIC_LIB_SUFFIX
  "${LZ4_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

set(LZ4_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}lz4${LZ4_STATIC_LIB_SUFFIX})

find_path(LZ4_INCLUDE_DIR NAMES lz4.h
  PATHS ${_lz4_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include" )
find_library(LZ4_STATIC_LIB NAMES ${LZ4_STATIC_LIB_NAME} lib${LZ4_STATIC_LIB_NAME}
  PATHS ${_lz4_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib" )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 REQUIRED_VARS
  LZ4_STATIC_LIB LZ4_INCLUDE_DIR)
