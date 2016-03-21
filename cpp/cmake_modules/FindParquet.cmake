# Copyright 2012 Cloudera Inc.
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

# - Find PARQUET (parquet/parquet.h, libparquet.a, libparquet.so)
# This module defines
#  PARQUET_INCLUDE_DIR, directory containing headers
#  PARQUET_LIBS, directory containing parquet libraries
#  PARQUET_STATIC_LIB, path to libparquet.a
#  PARQUET_SHARED_LIB, path to libparquet's shared library
#  PARQUET_FOUND, whether parquet has been found

if( NOT "$ENV{PARQUET_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{PARQUET_HOME}" _native_path )
    list( APPEND _parquet_roots ${_native_path} )
elseif ( Parquet_HOME )
    list( APPEND _parquet_roots ${Parquet_HOME} )
endif()

# Try the parameterized roots, if they exist
if ( _parquet_roots )
    find_path( PARQUET_INCLUDE_DIR NAMES parquet/api/reader.h
        PATHS ${_parquet_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( PARQUET_LIBRARIES NAMES parquet
        PATHS ${_parquet_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" )
else ()
    find_path( PARQUET_INCLUDE_DIR NAMES parquet/api/reader.h )
    find_library( PARQUET_LIBRARIES NAMES parquet )
endif ()


if (PARQUET_INCLUDE_DIR AND PARQUET_LIBRARIES)
  set(PARQUET_FOUND TRUE)
  get_filename_component( PARQUET_LIBS ${PARQUET_LIBRARIES} PATH )
  set(PARQUET_LIB_NAME libparquet)
  set(PARQUET_STATIC_LIB ${PARQUET_LIBS}/${PARQUET_LIB_NAME}.a)
  set(PARQUET_SHARED_LIB ${PARQUET_LIBS}/${PARQUET_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(PARQUET_FOUND FALSE)
endif ()

if (PARQUET_FOUND)
  if (NOT Parquet_FIND_QUIETLY)
    message(STATUS "Found the Parquet library: ${PARQUET_LIBRARIES}")
  endif ()
else ()
  if (NOT Parquet_FIND_QUIETLY)
    set(PARQUET_ERR_MSG "Could not find the Parquet library. Looked in ")
    if ( _parquet_roots )
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} in ${_parquet_roots}.")
    else ()
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} system search paths.")
    endif ()
    if (Parquet_FIND_REQUIRED)
      message(FATAL_ERROR "${PARQUET_ERR_MSG}")
    else (Parquet_FIND_REQUIRED)
      message(STATUS "${PARQUET_ERR_MSG}")
    endif (Parquet_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  PARQUET_INCLUDE_DIR
  PARQUET_LIBS
  PARQUET_LIBRARIES
  PARQUET_STATIC_LIB
  PARQUET_SHARED_LIB
)
