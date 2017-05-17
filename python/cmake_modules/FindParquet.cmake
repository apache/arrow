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

# - Find PARQUET (parquet/parquet.h, libparquet.a, libparquet.so)
# This module defines
#  PARQUET_INCLUDE_DIR, directory containing headers
#  PARQUET_LIBS, directory containing parquet libraries
#  PARQUET_STATIC_LIB, path to libparquet.a
#  PARQUET_SHARED_LIB, path to libparquet's shared library
#  PARQUET_FOUND, whether parquet has been found

include(FindPkgConfig)

if(NOT "$ENV{PARQUET_HOME}" STREQUAL "")
    set(PARQUET_HOME "$ENV{PARQUET_HOME}")
endif()

if(PARQUET_HOME)
    set(PARQUET_SEARCH_HEADER_PATHS
        ${PARQUET_HOME}/include
        )
    set(PARQUET_SEARCH_LIB_PATH
        ${PARQUET_HOME}/lib
        )
    find_path(PARQUET_INCLUDE_DIR parquet/api/reader.h PATHS
        ${PARQUET_SEARCH_HEADER_PATHS}
        # make sure we don't accidentally pick up a different version
        NO_DEFAULT_PATH
        )
    find_library(PARQUET_LIBRARIES NAMES parquet
        PATHS ${PARQUET_HOME} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib")
    find_library(PARQUET_ARROW_LIBRARIES NAMES parquet_arrow
        PATHS ${PARQUET_HOME} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib")
    get_filename_component(PARQUET_LIBS ${PARQUET_LIBRARIES} PATH )
else()
    pkg_check_modules(PARQUET parquet)
    if (PARQUET_FOUND)
        pkg_get_variable(PARQUET_ABI_VERSION parquet abi_version)
        message(STATUS "Parquet C++ ABI version: ${PARQUET_ABI_VERSION}")
        pkg_get_variable(PARQUET_SO_VERSION parquet so_version)
        message(STATUS "Parquet C++ SO version: ${PARQUET_SO_VERSION}")
        set(PARQUET_INCLUDE_DIR ${PARQUET_INCLUDE_DIRS})
        set(PARQUET_LIBS ${PARQUET_LIBRARY_DIRS})
        set(PARQUET_SEARCH_LIB_PATH ${PARQUET_LIBRARY_DIRS})
        message(STATUS "Searching for parquet libs in: ${PARQUET_SEARCH_LIB_PATH}")
        find_library(PARQUET_LIBRARIES NAMES parquet
            PATHS ${PARQUET_SEARCH_LIB_PATH} NO_DEFAULT_PATH)
        find_library(PARQUET_ARROW_LIBRARIES NAMES parquet_arrow
            PATHS ${PARQUET_SEARCH_LIB_PATH} NO_DEFAULT_PATH)
        message(STATUS "${PARQUET_ARROW_LIBRARIES}")
    else()
        find_path(PARQUET_INCLUDE_DIR NAMES parquet/api/reader.h )
        find_library(PARQUET_LIBRARIES NAMES parquet)
        find_library(PARQUET_ARROW_LIBRARIES NAMES parquet_arrow)
        get_filename_component(PARQUET_LIBS ${PARQUET_LIBRARIES} PATH )
    endif()
endif()

if (PARQUET_INCLUDE_DIR AND PARQUET_LIBRARIES)
  set(PARQUET_FOUND TRUE)
  if (MSVC)
    set(PARQUET_STATIC_LIB "${PARQUET_LIBRARIES}_static")
    set(PARQUET_SHARED_LIB "${PARQUET_LIBRARIES}")
  else()
    set(PARQUET_LIB_NAME libparquet)
    set(PARQUET_STATIC_LIB ${PARQUET_LIBS}/${PARQUET_LIB_NAME}.a)
    set(PARQUET_SHARED_LIB ${PARQUET_LIBS}/${PARQUET_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
else ()
  set(PARQUET_FOUND FALSE)
endif ()

if (PARQUET_INCLUDE_DIR AND PARQUET_ARROW_LIBRARIES)
  set(PARQUET_ARROW_FOUND TRUE)
  get_filename_component(PARQUET_ARROW_LIBS ${PARQUET_ARROW_LIBRARIES} PATH)
  if (MSVC)
    set(PARQUET_ARROW_STATIC_LIB "${PARQUET_ARROW_LIBRARIES}_static")
    set(PARQUET_ARROW_SHARED_LIB "${PARQUET_ARROW_LIBRARIES}")
  else()
    set(PARQUET_ARROW_LIB_NAME libparquet_arrow)
    set(PARQUET_ARROW_STATIC_LIB
      ${PARQUET_ARROW_LIBS}/${PARQUET_ARROW_LIB_NAME}.a)
    set(PARQUET_ARROW_SHARED_LIB
      ${PARQUET_ARROW_LIBS}/${PARQUET_ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
else ()
  set(PARQUET_ARROW_FOUND FALSE)
endif ()

if (PARQUET_FOUND AND PARQUET_ARROW_FOUND)
  if (NOT Parquet_FIND_QUIETLY)
    message(STATUS "Found the Parquet library: ${PARQUET_LIBRARIES}")
    message(STATUS "Found the Parquet Arrow library: ${PARQUET_ARROW_LIBS}")
  endif ()
else ()
  if (NOT Parquet_FIND_QUIETLY)
    if (NOT PARQUET_FOUND)
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} Could not find the parquet library.")
    endif()

    if (NOT PARQUET_ARROW_FOUND)
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} Could not find the parquet_arrow library. Did you build with -DPARQUET_ARROW=on?")
    endif()
    set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} Looked in ")
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
  PARQUET_FOUND
  PARQUET_INCLUDE_DIR
  PARQUET_LIBS
  PARQUET_LIBRARIES
  PARQUET_STATIC_LIB
  PARQUET_SHARED_LIB

  PARQUET_ARROW_FOUND
  PARQUET_ARROW_LIBS
  PARQUET_ARROW_STATIC_LIB
  PARQUET_ARROW_SHARED_LIB
)
