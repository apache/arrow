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
#
# Tries to find bz2 headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Bz2)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  BZ2_HOME - When set, this path is inspected instead of standard library
#             locations as the root of the bz2 installation.
#
# This module defines
#  BZ2_INCLUDE_DIR, directory containing headers
#  BZ2_STATIC_LIB, path to libbz2 static library
#  BZ2_FOUND, whether bz2 has been found

if( NOT "${BZ2_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${BZ2_HOME}" _native_path )
    list( APPEND _bz2_roots ${_native_path} )
elseif ( BZ2_HOME )
    list( APPEND _bz2_roots ${BZ2_HOME} )
endif()

if (MSVC)
  set(BZ2_STATIC_LIB_NAME
    ${CMAKE_STATIC_LIBRARY_PREFIX}bz2_static${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
  set(BZ2_STATIC_LIB_NAME
    ${CMAKE_STATIC_LIBRARY_PREFIX}bz2${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

find_path(BZ2_INCLUDE_DIR NAMES bzlib.h
  PATHS ${_bz2_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include" )
find_library(BZ2_STATIC_LIB NAMES ${BZ2_STATIC_LIB_NAME} lib${BZ2_STATIC_LIB_NAME}
  PATHS ${_bz2_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib" )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BZ2 REQUIRED_VARS
  BZ2_STATIC_LIB BZ2_INCLUDE_DIR)
