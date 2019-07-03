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

# - Find ARROW Flight (arrow/flight/api.h, libarrow_flight.a, libarrow_flight.so)
#
# This module requires Arrow from which it uses
#   ARROW_FOUND
#   ARROW_SEARCH_HEADER_PATHS
#   ARROW_SEARCH_LIB_PATH
#   ARROW_HOME
#
# This module defines
#  ARROW_FLIGHT_FOUND, whether Flight has been found
#  ARROW_FLIGHT_INCLUDE_DIR, directory containing headers
#  ARROW_FLIGHT_LIBS, directory containing Flight libraries
#  ARROW_FLIGHT_STATIC_LIB, path to libarrow_flight.a
#  ARROW_FLIGHT_SHARED_LIB, path to libarrow_flight.so
#  ARROW_FLIGHT_SHARED_IMP_LIB, path to libarrow_flight's import library (MSVC only)

include(FindPkgConfig)
include(GNUInstallDirs)

if(NOT DEFINED ARROW_FOUND)
  if(ArrowFlight_FIND_REQUIRED)
    find_package(Arrow REQUIRED)
  else()
    find_package(Arrow)
  endif()
endif()

if(NOT ARROW_FOUND)
  set(ARROW_FLIGHT_FOUND FALSE)
  return()
endif()

find_path(ARROW_FLIGHT_INCLUDE_DIR arrow/flight/api.h
          PATHS ${ARROW_SEARCH_HEADER_PATHS}
          PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES}
          NO_DEFAULT_PATH)

if(NOT (ARROW_FLIGHT_INCLUDE_DIR STREQUAL ARROW_INCLUDE_DIR))
  set(ARROW_FLIGHT_WARN_MSG "Mismatch of Arrow and Arrow Flight include directories:")
  set(ARROW_FLIGHT_WARN_MSG
      "${ARROW_FLIGHT_WARN_MSG}  ARROW_INCLUDE_DIR=${ARROW_INCLUDE_DIR}")
  set(ARROW_FLIGHT_WARN_MSG
      "${ARROW_FLIGHT_WARN_MSG}  ARROW_FLIGHT_INCLUDE_DIR=${ARROW_FLIGHT_INCLUDE_DIR}")
  message(WARNING ${ARROW_FLIGHT_WARN_MSG})
endif()

find_library(ARROW_FLIGHT_LIB_PATH
             NAMES arrow_flight
             PATHS ${ARROW_SEARCH_LIB_PATH}
             PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
             NO_DEFAULT_PATH)
get_filename_component(ARROW_FLIGHT_LIBS ${ARROW_FLIGHT_LIB_PATH} DIRECTORY)

if(MSVC)
  # Prioritize "/bin" over LIB_PATH_SUFFIXES - DLL files are installed
  # in "/bin" and static objects are in "/lib", so we want to search
  # "/bin" first
  find_library(ARROW_FLIGHT_SHARED_LIBRARIES
               NAMES arrow_flight
               PATHS ${ARROW_HOME}
               PATH_SUFFIXES "bin" ${LIB_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  get_filename_component(ARROW_FLIGHT_SHARED_LIBS ${ARROW_FLIGHT_SHARED_LIBRARIES} DIRECTORY)
endif()

if(ARROW_FLIGHT_INCLUDE_DIR AND ARROW_FLIGHT_LIBS)
  set(ARROW_FLIGHT_FOUND TRUE)
  set(ARROW_FLIGHT_LIB_NAME arrow_flight)
  if(MSVC)
    set(
      ARROW_FLIGHT_STATIC_LIB
      ${ARROW_FLIGHT_LIBS}/${ARROW_FLIGHT_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
      )
    set(ARROW_FLIGHT_SHARED_LIB
        ${ARROW_FLIGHT_SHARED_LIBS}/${ARROW_FLIGHT_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_FLIGHT_SHARED_IMP_LIB ${ARROW_FLIGHT_LIBS}/${ARROW_FLIGHT_LIB_NAME}.lib)
  else()
    set(ARROW_FLIGHT_STATIC_LIB ${ARROW_LIBS}/lib${ARROW_FLIGHT_LIB_NAME}.a)
    set(ARROW_FLIGHT_SHARED_LIB
        ${ARROW_LIBS}/lib${ARROW_FLIGHT_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
endif()

if(ARROW_FLIGHT_FOUND)
  if(NOT ArrowFlight_FIND_QUIETLY)
    message(STATUS "Found the Arrow Flight library: ${ARROW_FLIGHT_LIB_PATH}")
  endif()
else()
  if(NOT ArrowFlight_FIND_QUIETLY)
    set(ARROW_FLIGHT_ERR_MSG "Could not find the Arrow Flight library. Looked for headers")
    set(ARROW_FLIGHT_ERR_MSG
        "${ARROW_FLIGHT_ERR_MSG} in ${ARROW_SEARCH_HEADER_PATHS}, and for libs")
    set(ARROW_FLIGHT_ERR_MSG "${ARROW_FLIGHT_ERR_MSG} in ${ARROW_SEARCH_LIB_PATH}")
    if(ArrowFlight_FIND_REQUIRED)
      message(FATAL_ERROR "${ARROW_FLIGHT_ERR_MSG}")
    else(ArrowFlight_FIND_REQUIRED)
      message(STATUS "${ARROW_FLIGHT_ERR_MSG}")
    endif(ArrowFlight_FIND_REQUIRED)
  endif()
  set(ARROW_FLIGHT_FOUND FALSE)
endif()

if(MSVC)
  mark_as_advanced(ARROW_FLIGHT_INCLUDE_DIR ARROW_FLIGHT_STATIC_LIB ARROW_FLIGHT_SHARED_LIB
                   ARROW_FLIGHT_SHARED_IMP_LIB)
else()
  mark_as_advanced(ARROW_FLIGHT_INCLUDE_DIR ARROW_FLIGHT_STATIC_LIB ARROW_FLIGHT_SHARED_LIB)
endif()
