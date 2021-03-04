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

# - Find Arrow testing library
#   (arrow/testing/util.h, libarrow_testing.a, libarrow_testing.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_TESTING_FOUND, whether Arrow testing library has been found
#  ARROW_TESTING_IMPORT_LIB,
#    path to libarrow_testing's import library (Windows only)
#  ARROW_TESTING_INCLUDE_DIR, directory containing headers
#  ARROW_TESTING_LIB_DIR, directory containing Arrow testing libraries
#  ARROW_TESTING_SHARED_LIB, path to libarrow_testing's shared library
#  ARROW_TESTING_STATIC_LIB, path to libarrow_testing.a

if(DEFINED ARROW_TESTING_FOUND)
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
  arrow_find_package(ARROW_TESTING
                     "${ARROW_HOME}"
                     arrow_testing
                     arrow/testing/util.h
                     ArrowTesting
                     arrow-testing)
  if(NOT ARROW_TESTING_VERSION)
    set(ARROW_TESTING_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_TESTING_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_TESTING_VERSION_MATCH TRUE)
else()
  set(ARROW_TESTING_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_TESTING_IMPORT_LIB
                 ARROW_TESTING_INCLUDE_DIR
                 ARROW_TESTING_LIBS
                 ARROW_TESTING_LIB_DIR
                 ARROW_TESTING_SHARED_IMP_LIB
                 ARROW_TESTING_SHARED_LIB
                 ARROW_TESTING_STATIC_LIB
                 ARROW_TESTING_VERSION
                 ARROW_TESTING_VERSION_MATCH)

find_package_handle_standard_args(ArrowTesting
                                  REQUIRED_VARS
                                  ARROW_TESTING_INCLUDE_DIR
                                  ARROW_TESTING_LIB_DIR
                                  ARROW_TESTING_VERSION_MATCH
                                  VERSION_VAR
                                  ARROW_TESTING_VERSION)
set(ARROW_TESTING_FOUND ${ArrowTesting_FOUND})

if(ArrowTesting_FOUND AND NOT ArrowTesting_FIND_QUIETLY)
  message(STATUS "Found the Arrow testing by ${ARROW_TESTING_FIND_APPROACH}")
  message(STATUS "Found the Arrow testing shared library: ${ARROW_TESTING_SHARED_LIB}")
  message(STATUS "Found the Arrow testing import library: ${ARROW_TESTING_IMPORT_LIB}")
  message(STATUS "Found the Arrow testing static library: ${ARROW_TESTING_STATIC_LIB}")
endif()
