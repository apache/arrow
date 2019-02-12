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
# Tries to find c-ares headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(c-ares)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  CARES_HOME - When set, this path is inspected instead of standard library
#               locations as the root of the c-ares installation.
#               The environment variable CARES_HOME overrides this variable.
#
# - Find CARES
# This module defines
#  CARES_INCLUDE_DIR, directory containing headers
#  CARES_SHARED_LIB, path to c-ares's shared library
#  CARES_FOUND, whether c-ares has been found

if( NOT "${CARES_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${CARES_HOME}" _native_path )
    list( APPEND _cares_roots ${_native_path} )
elseif ( CARES_HOME )
    list( APPEND _cares_roots ${CARES_HOME} )
endif()

if (MSVC)
  set(CARES_LIB_NAME cares.lib)
else ()
  set(CARES_LIB_NAME
    ${CMAKE_SHARED_LIBRARY_PREFIX}cares${CMAKE_SHARED_LIBRARY_SUFFIX})
  set(CARES_STATIC_LIB_NAMES
    ${CMAKE_STATIC_LIBRARY_PREFIX}cares${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${CMAKE_STATIC_LIBRARY_PREFIX}cares_static${CMAKE_STATIC_LIBRARY_SUFFIX})
endif ()

# Try the parameterized roots, if they exist
if (_cares_roots)
  find_path(CARES_INCLUDE_DIR NAMES ares.h
    PATHS ${_cares_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "include")
  find_library(CARES_SHARED_LIB
    NAMES ${CARES_LIB_NAME}
    PATHS ${_cares_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "lib")
  find_library(CARES_STATIC_LIB
    NAMES ${CARES_STATIC_LIB_NAMES}
    PATHS ${_cares_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "lib")
else ()
  pkg_check_modules(PKG_CARES cares)
  if (PKG_CARES_FOUND)
    set(CARES_INCLUDE_DIR ${PKG_CARES_INCLUDEDIR})
    find_library(CARES_SHARED_LIB
      NAMES ${CARES_LIB_NAME}
      PATHS ${PKG_CARES_LIBDIR} NO_DEFAULT_PATH)
  else ()
    find_path(CARES_INCLUDE_DIR NAMES cares.h)
    find_library(CARES_SHARED_LIB NAMES ${CARES_LIB_NAME})
  endif ()
endif ()

if (CARES_INCLUDE_DIR AND CARES_SHARED_LIB)
  set(CARES_FOUND TRUE)
else ()
  set(CARES_FOUND FALSE)
endif ()

if (CARES_FOUND)
  if (NOT CARES_FIND_QUIETLY)
    if (CARES_SHARED_LIB)
      message(STATUS "Found the c-ares shared library: ${CARES_SHARED_LIB}")
    endif ()
  endif ()
else ()
  if (NOT CARES_FIND_QUIETLY)
    set(CARES_ERR_MSG "Could not find the c-ares library. Looked in ")
    if ( _cares_roots )
      set(CARES_ERR_MSG "${CARES_ERR_MSG} ${_cares_roots}.")
    else ()
      set(CARES_ERR_MSG "${CARES_ERR_MSG} system search paths.")
    endif ()
    if (CARES_FIND_REQUIRED)
      message(FATAL_ERROR "${CARES_ERR_MSG}")
    else (CARES_FIND_REQUIRED)
      message(STATUS "${CARES_ERR_MSG}")
    endif (CARES_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  CARES_INCLUDE_DIR
  CARES_LIBRARIES
  CARES_SHARED_LIB
  CARES_STATIC_LIB
)
