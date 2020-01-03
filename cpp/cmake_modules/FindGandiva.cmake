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

# - Find Gandiva (gandiva/arrow.h, libgandiva.a, libgandiva.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  GANDIVA_FOUND, whether Gandiva has been found
#  GANDIVA_IMPORT_LIB, path to libgandiva's import library (Windows only)
#  GANDIVA_INCLUDE_DIR, directory containing headers
#  GANDIVA_LIBS, deprecated. Use GANDIVA_LIB_DIR instead
#  GANDIVA_LIB_DIR, directory containing Gandiva libraries
#  GANDIVA_SHARED_IMP_LIB, deprecated. Use GANDIVA_IMPORT_LIB instead
#  GANDIVA_SHARED_LIB, path to libgandiva's shared library
#  GANDIVA_SO_VERSION, shared object version of found Gandiva such as "100"
#  GANDIVA_STATIC_LIB, path to libgandiva.a

if(DEFINED GANDIVA_FOUND)
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
  arrow_find_package(GANDIVA
                     "${ARROW_HOME}"
                     gandiva
                     gandiva/arrow.h
                     Gandiva
                     gandiva)
  if(NOT GANDIVA_VERSION)
    set(GANDIVA_VERSION "${ARROW_VERSION}")
  endif()
  set(GANDIVA_ABI_VERSION "${ARROW_ABI_VERSION}")
  set(GANDIVA_SO_VERSION "${ARROW_SO_VERSION}")
endif()

if("${GANDIVA_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(GANDIVA_VERSION_MATCH TRUE)
else()
  set(GANDIVA_VERSION_MATCH FALSE)
endif()

mark_as_advanced(GANDIVA_ABI_VERSION
                 GANDIVA_IMPORT_LIB
                 GANDIVA_INCLUDE_DIR
                 GANDIVA_LIBS
                 GANDIVA_LIB_DIR
                 GANDIVA_SHARED_IMP_LIB
                 GANDIVA_SHARED_LIB
                 GANDIVA_SO_VERSION
                 GANDIVA_STATIC_LIB
                 GANDIVA_VERSION
                 GANDIVA_VERSION_MATCH)

find_package_handle_standard_args(Gandiva
                                  REQUIRED_VARS
                                  GANDIVA_INCLUDE_DIR
                                  GANDIVA_LIB_DIR
                                  GANDIVA_SO_VERSION
                                  GANDIVA_VERSION_MATCH
                                  VERSION_VAR
                                  GANDIVA_VERSION)
set(GANDIVA_FOUND ${Gandiva_FOUND})

if(Gandiva_FOUND AND NOT Gandiva_FIND_QUIETLY)
  message(STATUS "Found the Gandiva by ${GANDIVA_FIND_APPROACH}")
  message(STATUS "Found the Gandiva shared library: ${GANDIVA_SHARED_LIB}")
  message(STATUS "Found the Gandiva import library: ${GANDIVA_IMPORT_LIB}")
  message(STATUS "Found the Gandiva static library: ${GANDIVA_STATIC_LIB}")
endif()
