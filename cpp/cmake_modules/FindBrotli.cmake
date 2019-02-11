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
# Tries to find Brotli headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Brotli)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Brotli_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the Brotli installation.
#                The environment variable BROTLI_HOME overrides this veriable.
#
# This module defines
#  BROTLI_INCLUDE_DIR, directory containing headers
#  BROTLI_STATIC_LIB, path to libbrotli's static library
#  BROTLI_SHARED_LIB, path to libbrotli's shared library
#  BROTLI_FOUND, whether brotli has been found

if( NOT "${BROTLI_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${BROTLI_HOME}" _native_path )
    list( APPEND _brotli_roots ${_native_path} )
elseif ( Brotli_HOME )
    list( APPEND _brotli_roots ${Brotli_HOME} )
endif()

find_path( BROTLI_INCLUDE_DIR NAMES brotli/decode.h
  PATHS ${_brotli_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include" )

set(BROTLI_LIB_NAME brotli)

if (DEFINED BROTLI_MSVC_STATIC_LIB_SUFFIX)
  set(BROTLI_STATIC_LIB_SUFFIX "${BROTLI_MSVC_STATIC_LIB_SUFFIX}")
endif()
if (NOT DEFINED BROTLI_STATIC_LIB_SUFFIX)
  if (MSVC)
    set(BROTLI_STATIC_LIB_SUFFIX _static)
  else()
    set(BROTLI_STATIC_LIB_SUFFIX -static)
  endif()
endif()

foreach (BROTLI_LIB enc dec common)
  string(TOUPPER "LIBRARY_${BROTLI_LIB}" LIBRARY)

  find_library(BROTLI_SHARED_${LIBRARY}
    NAMES ${CMAKE_SHARED_LIBRARY_PREFIX}${BROTLI_LIB_NAME}${BROTLI_LIB}${BROTLI_SHARED_LIB_SUFFIX}${CMAKE_SHARED_LIBRARY_SUFFIX}
    PATHS ${_brotli_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
  list(APPEND BROTLI_SHARED_LIB ${BROTLI_SHARED_${LIBRARY}})

  find_library(BROTLI_STATIC_${LIBRARY}
    NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}${BROTLI_LIB}${BROTLI_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${_brotli_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
  list(APPEND BROTLI_STATIC_LIB ${BROTLI_STATIC_${LIBRARY}})
endforeach()

if (BROTLI_INCLUDE_DIR AND (PARQUET_MINIMAL_DEPENDENCY OR BROTLI_STATIC_LIB OR BROTLI_SHARED_LIB))
  set(BROTLI_FOUND TRUE)
  if (NOT Brotli_FIND_QUIETLY)
    if (PARQUET_MINIMAL_DEPENDENCY)
      message(STATUS "Found the Brotli headers: ${BROTLI_INCLUDE_DIR}")
    else ()
      if (BROTLI_STATIC_LIB)
        message(STATUS "Found the Brotli static library: ${BROTLI_STATIC_LIB}")
      endif()
      if (BROTLI_SHARED_LIB)
        message(STATUS "Found the Brotli shared library: ${BROTLI_SHARED_LIB}")
      endif()
    endif ()
  endif ()
else ()
  set(BROTLI_FOUND FALSE)
  if (NOT Brotli_FIND_QUIETLY)
    set(BROTLI_ERR_MSG "Could not find the Brotli library. Looked in ")
    if ( _brotli_roots )
      set(BROTLI_ERR_MSG "${BROTLI_ERR_MSG} in ${_brotli_roots}.")
    else ()
      set(BROTLI_ERR_MSG "${BROTLI_ERR_MSG} system search paths.")
    endif ()
    if (Brotli_FIND_REQUIRED)
      message(FATAL_ERROR "${BROTLI_ERR_MSG}")
    else (Brotli_FIND_REQUIRED)
      message(STATUS "${BROTLI_ERR_MSG}")
    endif (Brotli_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  BROTLI_INCLUDE_DIR
  BROTLI_STATIC_LIB
  BROTLI_SHARED_LIB
)
