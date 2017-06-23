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
# Tries to find ZLIB headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(ZLIB)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  ZLIB_HOME - When set, this path is inspected instead of standard library
#             locations as the root of the ZLIB installation.
#             The environment variable ZLIB_HOME overrides this variable.
#
# - Find ZLIB (zlib.h, libz.a, libz.so, and libz.so.1)
# This module defines
#  ZLIB_INCLUDE_DIR, directory containing headers
#  ZLIB_LIBS, directory containing zlib libraries
#  ZLIB_STATIC_LIB, path to libz.a
#  ZLIB_SHARED_LIB, path to libz's shared library
#  ZLIB_FOUND, whether zlib has been found

if( NOT "${ZLIB_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${ZLIB_HOME}" _native_path )
    list( APPEND _zlib_roots ${_native_path} )
elseif ( ZLIB_HOME )
    list( APPEND _zlib_roots ${ZLIB_HOME} )
endif()

# Try the parameterized roots, if they exist
if ( _zlib_roots )
    find_path( ZLIB_INCLUDE_DIR NAMES zlib.h
        PATHS ${_zlib_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( ZLIB_LIBRARIES NAMES libz.a zlib
        PATHS ${_zlib_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" )
else ()
    find_path( ZLIB_INCLUDE_DIR NAMES zlib.h )
    # Only look for the static library
    find_library( ZLIB_LIBRARIES NAMES libz.a zlib )
endif ()


if (ZLIB_INCLUDE_DIR AND (PARQUET_MINIMAL_DEPENDENCY OR ZLIB_LIBRARIES))
  set(ZLIB_FOUND TRUE)
  get_filename_component( ZLIB_LIBS ${ZLIB_LIBRARIES} PATH )
  set(ZLIB_HEADER_NAME zlib.h)
  set(ZLIB_HEADER ${ZLIB_INCLUDE_DIR}/${ZLIB_HEADER_NAME})
  set(ZLIB_LIB_NAME z)
  if (MSVC)
    if (NOT ZLIB_MSVC_STATIC_LIB_SUFFIX)
      set(ZLIB_MSVC_STATIC_LIB_SUFFIX libstatic)
    endif()
    set(ZLIB_MSVC_SHARED_LIB_SUFFIX lib)
  endif()
  set(ZLIB_STATIC_LIB ${ZLIB_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_LIB_NAME}${ZLIB_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(ZLIB_SHARED_LIB ${ZLIB_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${ZLIB_LIB_NAME}${ZLIB_MSVC_SHARED_LIB_SUFFIX}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(ZLIB_FOUND FALSE)
endif ()

if (ZLIB_FOUND)
  if (NOT ZLIB_FIND_QUIETLY)
    if (PARQUET_MINIMAL_DEPENDENCY)
      message(STATUS "Found the ZLIB header: ${ZLIB_HEADER}")
    else()
      message(STATUS "Found the ZLIB library: ${ZLIB_LIBRARIES}")
    endif ()
  endif ()
else ()
  if (NOT ZLIB_FIND_QUIETLY)
    set(ZLIB_ERR_MSG "Could not find the ZLIB library. Looked in ")
    if ( _zlib_roots )
      set(ZLIB_ERR_MSG "${ZLIB_ERR_MSG} in ${_zlib_roots}.")
    else ()
      set(ZLIB_ERR_MSG "${ZLIB_ERR_MSG} system search paths.")
    endif ()
    if (ZLIB_FIND_REQUIRED)
      message(FATAL_ERROR "${ZLIB_ERR_MSG}")
    else (ZLIB_FIND_REQUIRED)
      message(STATUS "${ZLIB_ERR_MSG}")
    endif (ZLIB_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  ZLIB_INCLUDE_DIR
  ZLIB_LIBS
  ZLIB_LIBRARIES
  ZLIB_STATIC_LIB
  ZLIB_SHARED_LIB
)
