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
# Tries to find CURL headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(CURL)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  CURL_HOME - When set, this path is inspected instead of standard library
#             locations as the root of the CURL installation.
#             The environment variable CURL_HOME overrides this variable.
#
# - Find CURL (curl/curl.h, libcurl.a)
# This module defines
#  CURL_INCLUDE_DIR, directory containing headers
#  CURL_STATIC_LIB, path to curl's static library
#  CURL_FOUND, whether curl has been found

if( NOT "${CURL_HOME}" STREQUAL "")
  file( TO_CMAKE_PATH "${CURL_HOME}" _native_path )
  list( APPEND _curl_roots ${_native_path} )
elseif ( CURL_HOME )
  list( APPEND _curl_roots ${CURL_HOME} )
endif()

if (MSVC)
  # curl uses curl.lib for Windows.
  set(CURL_LIB_NAME curllib)
else ()
  # curl uses libcurl.a for non Windows.
  set(CURL_LIB_NAME
          ${CMAKE_SHARED_LIBRARY_PREFIX}curl${CMAKE_STATIC_LIBRARY_SUFFIX})
endif ()

# Try the parameterized roots, if they exist
if (_curl_roots)
  find_path(CURL_INCLUDE_DIR NAMES curl/curl.h
          PATHS ${_curl_roots} NO_DEFAULT_PATH
          PATH_SUFFIXES "include")
  find_library(CURL_STATIC_LIB
          NAMES ${CURL_LIB_NAME}
          PATHS ${_curl_roots} NO_DEFAULT_PATH
          PATH_SUFFIXES "lib")
else ()
  pkg_check_modules(PKG_CURL libcurl)
  if (PKG_CURL_FOUND)
    set(CURL_INCLUDE_DIR ${PKG_CURL_INCLUDEDIR})
    find_library(CURL_STATIC_LIB
            NAMES ${CURL_LIB_NAME}
            PATHS ${PKG_CURL_LIBDIR} NO_DEFAULT_PATH)
  else ()
    find_path(CURL_INCLUDE_DIR NAMES curl/curl.h)
    find_library(CURL_STATIC_LIB NAMES ${CURL_LIB_NAME})
  endif ()
endif ()

if (CURL_INCLUDE_DIR AND CURL_STATIC_LIB)
  set(CURL_FOUND TRUE)
else ()
  set(CURL_FOUND FALSE)
endif ()

if (CURL_FOUND)
  if (NOT CURL_FIND_QUIETLY)
    if (CURL_STATIC_LIB)
      message(STATUS "Found the CURL static library: ${CURL_STATIC_LIB}")
    endif ()
  endif ()
else ()
  if (NOT CURL_FIND_QUIETLY)
    set(CURL_ERR_MSG "Could not find the CURL library. Looked in")
    if ( _curl_roots )
      set(CURL_ERR_MSG "${CURL_ERR_MSG} ${_curl_roots}.")
    else ()
      set(CURL_ERR_MSG "${CURL_ERR_MSG} system search paths.")
    endif ()
    if (CURL_FIND_REQUIRED)
      message(FATAL_ERROR "${CURL_ERR_MSG}")
    else (CURL_FIND_REQUIRED)
      message(STATUS "${CURL_ERR_MSG}")
    endif (CURL_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
        CURL_INCLUDE_DIR
        CURL_STATIC_LIB
)