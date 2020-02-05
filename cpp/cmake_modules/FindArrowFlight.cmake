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

# - Find Arrow Flight (arrow/flight/api.h, libarrow_flight.a, libarrow_flight.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_FLIGHT_FOUND, whether Flight has been found
#  ARROW_FLIGHT_IMPORT_LIB,
#    path to libarrow_flight's import library (Windows only)
#  ARROW_FLIGHT_INCLUDE_DIR, directory containing headers
#  ARROW_FLIGHT_LIBS, deprecated. Use ARROW_FLIGHT_LIB_DIR instead
#  ARROW_FLIGHT_LIB_DIR, directory containing Flight libraries
#  ARROW_FLIGHT_SHARED_IMP_LIB, deprecated. Use ARROW_FLIGHT_IMPORT_LIB instead
#  ARROW_FLIGHT_SHARED_LIB, path to libarrow_flight's shared library
#  ARROW_FLIGHT_STATIC_LIB, path to libarrow_flight.a

if(DEFINED ARROW_FLIGHT_FOUND)
  return()
endif()

set(find_package_arguments)
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION)
  list(APPEND find_package_arguments "${${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION}")
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_REQUIRED)
  list(APPEND find_package_arguments REQUIRED)
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_QUIETLY)
  list(APPEND find_package_arguments QUIET)
endif()
find_package(Arrow ${find_package_arguments})

if(ARROW_FOUND)
  arrow_find_package(ARROW_FLIGHT
                     "${ARROW_HOME}"
                     arrow_flight
                     arrow/flight/api.h
                     ArrowFlight
                     arrow-flight)
  if(NOT ARROW_FLIGHT_VERSION)
    set(ARROW_FLIGHT_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_FLIGHT_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_FLIGHT_VERSION_MATCH TRUE)
else()
  set(ARROW_FLIGHT_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_FLIGHT_IMPORT_LIB
                 ARROW_FLIGHT_INCLUDE_DIR
                 ARROW_FLIGHT_LIBS
                 ARROW_FLIGHT_LIB_DIR
                 ARROW_FLIGHT_SHARED_IMP_LIB
                 ARROW_FLIGHT_SHARED_LIB
                 ARROW_FLIGHT_STATIC_LIB
                 ARROW_FLIGHT_VERSION
                 ARROW_FLIGHT_VERSION_MATCH)

find_package_handle_standard_args(ArrowFlight
                                  REQUIRED_VARS
                                  ARROW_FLIGHT_INCLUDE_DIR
                                  ARROW_FLIGHT_LIB_DIR
                                  ARROW_FLIGHT_VERSION_MATCH
                                  VERSION_VAR
                                  ARROW_FLIGHT_VERSION)
set(ARROW_FLIGHT_FOUND ${ArrowFlight_FOUND})

if(ArrowFlight_FOUND AND NOT ArrowFlight_FIND_QUIETLY)
  message(STATUS "Found the Arrow Flight by ${ARROW_FLIGHT_FIND_APPROACH}")
  message(STATUS "Found the Arrow Flight shared library: ${ARROW_FLIGHT_SHARED_LIB}")
  message(STATUS "Found the Arrow Flight import library: ${ARROW_FLIGHT_IMPORT_LIB}")
  message(STATUS "Found the Arrow Flight static library: ${ARROW_FLIGHT_STATIC_LIB}")
endif()
