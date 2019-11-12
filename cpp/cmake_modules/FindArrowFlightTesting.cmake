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

# - Find Arrow Flight testing library
#   (arrow/flight/test_util.h,
#    libarrow_flight_testing.a,
#    libarrow_flight_testing.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_FLIGHT_TESTING_FOUND,
#    whether Arrow Flight testing library has been found
#  ARROW_FLIGHT_TESTING_IMPORT_LIB,
#    path to libarrow_flight_testing's import library (Windows only)
#  ARROW_FLIGHT_TESTING_INCLUDE_DIR, directory containing headers
#  ARROW_FLIGHT_TESTING_LIB_DIR, directory containing Arrow testing libraries
#  ARROW_FLIGHT_TESTING_SHARED_LIB,
#    path to libarrow_flight_testing's shared library
#  ARROW_FLIGHT_TESTING_STATIC_LIB, path to libarrow_flight_testing.a

if(DEFINED ARROW_FLIGHT_TESTING_FOUND)
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
find_package(ArrowFlight ${find_package_arguments})
find_package(ArrowTesting ${find_package_arguments})

if(ARROW_TESTING_FOUND AND ARROW_FLIGHT_FOUND)
  arrow_find_package(ARROW_FLIGHT_TESTING
                     "${ARROW_HOME}"
                     arrow_flight_testing
                     arrow/flight/test_util.h
                     ArrowFlightTesting
                     arrow-flight-testing)
  if(NOT ARROW_FLIGHT_TESTING_VERSION)
    set(ARROW_FLIGHT_TESTING_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_FLIGHT_TESTING_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_FLIGHT_TESTING_VERSION_MATCH TRUE)
else()
  set(ARROW_FLIGHT_TESTING_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_FLIGHT_TESTING_IMPORT_LIB
                 ARROW_FLIGHT_TESTING_INCLUDE_DIR
                 ARROW_FLIGHT_TESTING_LIBS
                 ARROW_FLIGHT_TESTING_LIB_DIR
                 ARROW_FLIGHT_TESTING_SHARED_IMP_LIB
                 ARROW_FLIGHT_TESTING_SHARED_LIB
                 ARROW_FLIGHT_TESTING_STATIC_LIB
                 ARROW_FLIGHT_TESTING_VERSION
                 ARROW_FLIGHT_TESTING_VERSION_MATCH)

find_package_handle_standard_args(ArrowFlightTesting
                                  REQUIRED_VARS
                                  ARROW_FLIGHT_TESTING_INCLUDE_DIR
                                  ARROW_FLIGHT_TESTING_LIB_DIR
                                  ARROW_FLIGHT_TESTING_VERSION_MATCH
                                  VERSION_VAR
                                  ARROW_FLIGHT_TESTING_VERSION)
set(ARROW_FLIGHT_TESTING_FOUND ${ArrowFlightTesting_FOUND})

if(ArrowFlightTesting_FOUND AND NOT ArrowFlightTesting_FIND_QUIETLY)
  message(
    STATUS "Found the Arrow Flight testing by ${ARROW_FLIGHT_TESTING_FIND_APPROACH}")
  message(
    STATUS
      "Found the Arrow Flight testing shared library: ${ARROW_FLIGHT_TESTING_SHARED_LIB}")
  message(
    STATUS
      "Found the Arrow Flight testing import library: ${ARROW_FLIGHT_TESTING_IMPORT_LIB}")
  message(
    STATUS
      "Found the Arrow Flight testing static library: ${ARROW_FLIGHT_TESTING_STATIC_LIB}")
endif()
