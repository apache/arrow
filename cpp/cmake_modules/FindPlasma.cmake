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

# - Find PLASMA (plasma/client.h, libplasma.a, libplasma.so)
# This module defines
#  PLASMA_INCLUDE_DIR, directory containing headers
#  PLASMA_LIBS, directory containing plasma libraries
#  PLASMA_STATIC_LIB, path to libplasma.a
#  PLASMA_SHARED_LIB, path to libplasma's shared library
#  PLASMA_SHARED_IMP_LIB, path to libplasma's import library (MSVC only)
#  PLASMA_FOUND, whether plasma has been found

include(FindPkgConfig)

if("$ENV{ARROW_HOME}" STREQUAL "")
  pkg_check_modules(PLASMA plasma)
  if(PLASMA_FOUND)
    pkg_get_variable(PLASMA_EXECUTABLE plasma executable)
    pkg_get_variable(PLASMA_SO_VERSION plasma so_version)
    set(PLASMA_ABI_VERSION ${PLASMA_SO_VERSION})
    message(STATUS "Plasma SO and ABI version: ${PLASMA_SO_VERSION}")
    pkg_get_variable(PLASMA_FULL_SO_VERSION plasma full_so_version)
    message(STATUS "Plasma full SO version: ${PLASMA_FULL_SO_VERSION}")
    set(PLASMA_INCLUDE_DIR ${PLASMA_INCLUDE_DIRS})
    set(PLASMA_LIBS ${PLASMA_LIBRARY_DIRS})
    set(PLASMA_SEARCH_LIB_PATH ${PLASMA_LIBRARY_DIRS})
  endif()
else()
  set(PLASMA_HOME "$ENV{ARROW_HOME}")

  set(PLASMA_EXECUTABLE ${PLASMA_HOME}/bin/plasma_store_server)

  set(PLASMA_SEARCH_HEADER_PATHS ${PLASMA_HOME}/include)

  set(PLASMA_SEARCH_LIB_PATH ${PLASMA_HOME}/lib)

  find_path(PLASMA_INCLUDE_DIR plasma/client.h
            PATHS ${PLASMA_SEARCH_HEADER_PATHS}
                  # make sure we don't accidentally pick up a different version
            NO_DEFAULT_PATH)
endif()

find_library(PLASMA_LIB_PATH NAMES plasma PATHS ${PLASMA_SEARCH_LIB_PATH} NO_DEFAULT_PATH)
get_filename_component(PLASMA_LIBS ${PLASMA_LIB_PATH} DIRECTORY)

if(PLASMA_INCLUDE_DIR AND PLASMA_LIBS)
  set(PLASMA_FOUND TRUE)
  set(PLASMA_LIB_NAME plasma)

  set(PLASMA_STATIC_LIB ${PLASMA_LIBS}/lib${PLASMA_LIB_NAME}.a)

  set(PLASMA_SHARED_LIB
      ${PLASMA_LIBS}/lib${PLASMA_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()

if(PLASMA_FOUND)
  if(NOT Plasma_FIND_QUIETLY)
    message(STATUS "Found the Plasma core library: ${PLASMA_LIB_PATH}")
    message(STATUS "Found Plasma executable: ${PLASMA_EXECUTABLE}")
  endif()
else()
  if(NOT Plasma_FIND_QUIETLY)
    set(PLASMA_ERR_MSG "Could not find the Plasma library. Looked for headers")
    set(PLASMA_ERR_MSG "${PLASMA_ERR_MSG} in ${PLASMA_SEARCH_HEADER_PATHS}, and for libs")
    set(PLASMA_ERR_MSG "${PLASMA_ERR_MSG} in ${PLASMA_SEARCH_LIB_PATH}")
    if(Plasma_FIND_REQUIRED)
      message(FATAL_ERROR "${PLASMA_ERR_MSG}")
    else(Plasma_FIND_REQUIRED)
      message(STATUS "${PLASMA_ERR_MSG}")
    endif(Plasma_FIND_REQUIRED)
  endif()
  set(PLASMA_FOUND FALSE)
endif()

mark_as_advanced(PLASMA_INCLUDE_DIR PLASMA_STATIC_LIB PLASMA_SHARED_LIB)
