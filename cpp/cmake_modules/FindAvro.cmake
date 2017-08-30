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
#  find_package(Avro)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  AVRO_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the Snappy installation.
#                The environment variable AVRO_HOME overrides this variable.
#
# This module defines
#  AVRO_INCLUDE_DIR, directory containing headers
#  AVRO_LIBS, directory containing snappy libraries
#  AVRO_STATIC_LIB, path to libsnappy.a
#  AVRO_SHARED_LIB, path to libsnappy's shared library
#  AVRO_FOUND, whether snappy has been found

if( NOT "${AVRO_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${AVRO_HOME}" _native_path )
    list( APPEND _AVRO_roots ${_native_path} )
elseif ( AVRO_HOME )
    list( APPEND _AVRO_roots ${AVRO_HOME} )
endif()

message(STATUS "AVRO_HOME: ${AVRO_HOME}")
find_path(AVRO_INCLUDE_DIR avro.h HINTS
  ${_AVRO_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library( AVRO_LIBRARIES NAMES avro PATHS
  ${_AVRO_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib")

if (AVRO_INCLUDE_DIR AND (PARQUET_MINIMAL_DEPENDENCY OR AVRO_LIBRARIES))
  set(AVRO_FOUND TRUE)
  get_filename_component( AVRO_LIBS ${AVRO_LIBRARIES} PATH )
  set(AVRO_HEADER_NAME avro.h)
  set(AVRO_HEADER ${AVRO_INCLUDE_DIR}/${AVRO_HEADER_NAME})
  set(AVRO_LIB_NAME avro)
  set(AVRO_STATIC_LIB ${AVRO_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${AVRO_LIB_NAME}${AVRO_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(AVRO_SHARED_LIB ${AVRO_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${AVRO_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(AVRO_FOUND FALSE)
endif ()

if (AVRO_FOUND)
  if (NOT AVRO_FIND_QUIETLY)
    if (PARQUET_MINIMAL_DEPENDENCY)
      message(STATUS "Found the Avro header: ${AVRO_HEADER}")
    else ()
      message(STATUS "Found the Avro library: ${AVRO_LIBRARIES}")
    endif ()
  endif ()
else ()
  if (NOT AVRO_FIND_QUIETLY)
    set(AVRO_ERR_MSG "Could not find the Avro library. Looked in ")
    if ( _AVRO_roots )
      set(AVRO_ERR_MSG "${AVRO_ERR_MSG} in ${_AVRO_roots}.")
    else ()
      set(AVRO_ERR_MSG "${AVRO_ERR_MSG} system search paths.")
    endif ()
    if (AVRO_FIND_REQUIRED)
      message(FATAL_ERROR "${AVRO_ERR_MSG}")
    else (AVRO_FIND_REQUIRED)
      message(STATUS "${AVRO_ERR_MSG}")
    endif (AVRO_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  AVRO_INCLUDE_DIR
  AVRO_LIBS
  AVRO_LIBRARIES
  AVRO_STATIC_LIB
  AVRO_SHARED_LIB
)
