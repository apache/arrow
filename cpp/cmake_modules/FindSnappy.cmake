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
# Tries to find Snappy headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Snappy)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Snappy_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the Snappy installation.
#                The environment variable SNAPPY_HOME overrides this variable.
#
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing snappy libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_SHARED_LIB, path to libsnappy's shared library
#  SNAPPY_FOUND, whether snappy has been found

if( NOT "${SNAPPY_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${SNAPPY_HOME}" _native_path )
    list( APPEND _snappy_roots ${_native_path} )
elseif ( Snappy_HOME )
    list( APPEND _snappy_roots ${Snappy_HOME} )
endif()

message(STATUS "SNAPPY_HOME: ${SNAPPY_HOME}")
find_path(SNAPPY_INCLUDE_DIR snappy.h HINTS
  ${_snappy_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library( SNAPPY_LIBRARIES NAMES snappy PATHS
  ${_snappy_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib")

if (SNAPPY_INCLUDE_DIR AND (PARQUET_MINIMAL_DEPENDENCY OR SNAPPY_LIBRARIES))
  set(SNAPPY_FOUND TRUE)
  get_filename_component( SNAPPY_LIBS ${SNAPPY_LIBRARIES} PATH )
  set(SNAPPY_HEADER_NAME snappy.h)
  set(SNAPPY_HEADER ${SNAPPY_INCLUDE_DIR}/${SNAPPY_HEADER_NAME})
  set(SNAPPY_LIB_NAME snappy)
  set(SNAPPY_STATIC_LIB ${SNAPPY_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_LIB_NAME}${SNAPPY_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(SNAPPY_SHARED_LIB ${SNAPPY_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${SNAPPY_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  if (NOT Snappy_FIND_QUIETLY)
    if (PARQUET_MINIMAL_DEPENDENCY)
      message(STATUS "Found the Snappy header: ${SNAPPY_HEADER}")
    else ()
      message(STATUS "Found the Snappy library: ${SNAPPY_LIBRARIES}")
    endif ()
  endif ()
else ()
  if (NOT Snappy_FIND_QUIETLY)
    set(SNAPPY_ERR_MSG "Could not find the Snappy library. Looked in ")
    if ( _snappy_roots )
      set(SNAPPY_ERR_MSG "${SNAPPY_ERR_MSG} in ${_snappy_roots}.")
    else ()
      set(SNAPPY_ERR_MSG "${SNAPPY_ERR_MSG} system search paths.")
    endif ()
    if (Snappy_FIND_REQUIRED)
      message(FATAL_ERROR "${SNAPPY_ERR_MSG}")
    else (Snappy_FIND_REQUIRED)
      message(STATUS "${SNAPPY_ERR_MSG}")
    endif (Snappy_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_LIBRARIES
  SNAPPY_STATIC_LIB
  SNAPPY_SHARED_LIB
)
