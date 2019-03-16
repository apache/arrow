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

# - Find GANDIVA (gandiva/client.h, libgandiva.a, libgandiva.so)
# This module defines
#  GANDIVA_INCLUDE_DIR, directory containing headers
#  GANDIVA_LIBS, directory containing gandiva libraries
#  GANDIVA_STATIC_LIB, path to libgandiva.a
#  GANDIVA_SHARED_LIB, path to libgandiva's shared library
#  GANDIVA_SHARED_IMP_LIB, path to libgandiva's import library (MSVC only)
#  GANDIVA_FOUND, whether gandiva has been found

include(FindPkgConfig)

if("$ENV{ARROW_HOME}" STREQUAL "")
  pkg_check_modules(GANDIVA gandiva)
  if(GANDIVA_FOUND)
    pkg_get_variable(GANDIVA_SO_VERSION gandiva so_version)
    set(GANDIVA_ABI_VERSION ${GANDIVA_SO_VERSION})
    message(STATUS "Gandiva SO and ABI version: ${GANDIVA_SO_VERSION}")
    pkg_get_variable(GANDIVA_FULL_SO_VERSION gandiva full_so_version)
    message(STATUS "Gandiva full SO version: ${GANDIVA_FULL_SO_VERSION}")
    set(GANDIVA_INCLUDE_DIR ${GANDIVA_INCLUDE_DIRS})
    set(GANDIVA_LIBS ${GANDIVA_LIBRARY_DIRS})
    set(GANDIVA_SEARCH_LIB_PATH ${GANDIVA_LIBRARY_DIRS})
  endif()
else()
  set(GANDIVA_HOME "$ENV{ARROW_HOME}")

  set(GANDIVA_SEARCH_HEADER_PATHS ${GANDIVA_HOME}/include)

  set(GANDIVA_SEARCH_LIB_PATH "${GANDIVA_HOME}")

  find_path(GANDIVA_INCLUDE_DIR gandiva/expression_registry.h
            PATHS ${GANDIVA_SEARCH_HEADER_PATHS}
                  # make sure we don't accidentally pick up a different version
            NO_DEFAULT_PATH)
endif()

find_library(GANDIVA_LIB_PATH
             NAMES gandiva
             PATHS ${GANDIVA_SEARCH_LIB_PATH}
             NO_DEFAULT_PATH
             PATH_SUFFIXES "lib")
get_filename_component(GANDIVA_LIBS ${GANDIVA_LIB_PATH} DIRECTORY)

find_library(GANDIVA_SHARED_LIB_PATH
             NAMES gandiva
             PATHS ${GANDIVA_SEARCH_LIB_PATH}
             NO_DEFAULT_PATH
             PATH_SUFFIXES "bin")

get_filename_component(GANDIVA_SHARED_LIBS ${GANDIVA_SHARED_LIB_PATH} PATH)

if(GANDIVA_INCLUDE_DIR AND GANDIVA_LIBS AND GANDIVA_SHARED_LIBS)
  set(GANDIVA_FOUND TRUE)
  set(GANDIVA_LIB_NAME gandiva)

  if(MSVC)
    set(
      GANDIVA_STATIC_LIB
      "${GANDIVA_LIBS}/${GANDIVA_LIB_NAME}${GANDIVA_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    set(GANDIVA_SHARED_LIB
        "${GANDIVA_SHARED_LIBS}/${GANDIVA_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(GANDIVA_SHARED_IMP_LIB "${GANDIVA_LIBS}/${GANDIVA_LIB_NAME}.lib")
  else()
    set(
      GANDIVA_STATIC_LIB
      ${GANDIVA_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
      )
    set(
      GANDIVA_SHARED_LIB
      ${GANDIVA_SHARED_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
      )
  endif()
endif()

if(GANDIVA_FOUND)
  if(NOT Gandiva_FIND_QUIETLY)
    message(STATUS "Found the Gandiva core library: ${GANDIVA_LIB_PATH}")
  endif()
else()
  if(NOT Gandiva_FIND_QUIETLY)
    set(GANDIVA_ERR_MSG "Could not find the Gandiva library. Looked for headers")
    set(GANDIVA_ERR_MSG
        "${GANDIVA_ERR_MSG} in ${GANDIVA_SEARCH_HEADER_PATHS}, and for libs")
    set(GANDIVA_ERR_MSG "${GANDIVA_ERR_MSG} in ${GANDIVA_SEARCH_LIB_PATH}")
    if(Gandiva_FIND_REQUIRED)
      message(FATAL_ERROR "${GANDIVA_ERR_MSG}")
    else(Gandiva_FIND_REQUIRED)
      message(STATUS "${GANDIVA_ERR_MSG}")
    endif(Gandiva_FIND_REQUIRED)
  endif()
  set(GANDIVA_FOUND FALSE)
endif()

mark_as_advanced(GANDIVA_INCLUDE_DIR GANDIVA_STATIC_LIB GANDIVA_SHARED_LIB)
