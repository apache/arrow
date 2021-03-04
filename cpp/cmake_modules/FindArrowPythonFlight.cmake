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

# - Find Arrow Python Flight
#   (arrow/python/flight.h, libarrow_python_flight.a, libarrow_python_flight.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_PYTHON_FLIGHT_FOUND, whether Arrow Python Flight has been found
#  ARROW_PYTHON_FLIGHT_IMPORT_LIB,
#    path to libarrow_python_flight's import library (Windows only)
#  ARROW_PYTHON_FLIGHT_INCLUDE_DIR, directory containing headers
#  ARROW_PYTHON_FLIGHT_LIB_DIR,
#    directory containing Arrow Python Flight libraries
#  ARROW_PYTHON_FLIGHT_SHARED_LIB, path to libarrow_python_flight's shared library
#  ARROW_PYTHON_FLIGHT_STATIC_LIB, path to libarrow_python_flight.a

if(DEFINED ARROW_PYTHON_FLIGHT_FOUND)
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
find_package(ArrowPython ${find_package_arguments})

if(ARROW_PYTHON_FOUND AND ARROW_FLIGHT_FOUND)
  arrow_find_package(ARROW_PYTHON_FLIGHT
                     "${ARROW_HOME}"
                     arrow_python_flight
                     arrow/python/flight.h
                     ArrowPythonFlight
                     arrow-python-flight)
  if(NOT ARROW_PYTHON_FLIGHT_VERSION)
    set(ARROW_PYTHON_FLIGHT_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_PYTHON_FLIGHT_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_PYTHON_FLIGHT_VERSION_MATCH TRUE)
else()
  set(ARROW_PYTHON_FLIGHT_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_PYTHON_FLIGHT_IMPORT_LIB
                 ARROW_PYTHON_FLIGHT_INCLUDE_DIR
                 ARROW_PYTHON_FLIGHT_LIBS
                 ARROW_PYTHON_FLIGHT_LIB_DIR
                 ARROW_PYTHON_FLIGHT_SHARED_IMP_LIB
                 ARROW_PYTHON_FLIGHT_SHARED_LIB
                 ARROW_PYTHON_FLIGHT_STATIC_LIB
                 ARROW_PYTHON_FLIGHT_VERSION
                 ARROW_PYTHON_FLIGHT_VERSION_MATCH)

find_package_handle_standard_args(ArrowPythonFlight
                                  REQUIRED_VARS
                                  ARROW_PYTHON_FLIGHT_INCLUDE_DIR
                                  ARROW_PYTHON_FLIGHT_LIB_DIR
                                  ARROW_PYTHON_FLIGHT_VERSION_MATCH
                                  VERSION_VAR
                                  ARROW_PYTHON_FLIGHT_VERSION)
set(ARROW_PYTHON_FLIGHT_FOUND ${ArrowPythonFlight_FOUND})

if(ArrowPythonFlight_FOUND AND NOT ArrowPythonFlight_FIND_QUIETLY)
  message(STATUS "Found the Arrow Python Flight by ${ARROW_PYTHON_FLIGHT_FIND_APPROACH}")
  message(
    STATUS
      "Found the Arrow Python Flight shared library: ${ARROW_PYTHON_FLIGHT_SHARED_LIB}")
  message(
    STATUS
      "Found the Arrow Python Flight import library: ${ARROW_PYTHON_FLIGHT_IMPORT_LIB}")
  message(
    STATUS
      "Found the Arrow Python Flight static library: ${ARROW_PYTHON_FLIGHT_STATIC_LIB}")
endif()
