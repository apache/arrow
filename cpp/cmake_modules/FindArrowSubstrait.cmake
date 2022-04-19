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

# - Find Arrow Substrait (libarrow_substrait.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_SUBSTRAIT_FOUND, whether Arrow Substrait has been found
#  ARROW_SUBSTRAIT_IMPORT_LIB,
#    path to libarrow_substrait's import library (Windows only)
#  ARROW_SUBSTRAIT_INCLUDE_DIR, directory containing headers
#  ARROW_SUBSTRAIT_LIB_DIR, directory containing Arrow Substrait libraries
#  ARROW_SUBSTRAIT_SHARED_LIB, path to libarrow_substrait's shared library
#  ARROW_SUBSTRAIT_STATIC_LIB, path to libarrow_substrait.a

if(DEFINED ARROW_SUBSTRAIT_FOUND)
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
find_package(Parquet ${find_package_arguments})

if(ARROW_FOUND AND PARQUET_FOUND)
  arrow_find_package(ARROW_SUBSTRAIT
                     "${ARROW_HOME}"
                     arrow_substrait
                     arrow/engine/substrait/api.h
                     ArrowSubstrait
                     arrow-substrait)
  if(NOT ARROW_SUBSTRAIT_VERSION)
    set(ARROW_SUBSTRAIT_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_SUBSTRAIT_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_SUBSTRAIT_VERSION_MATCH TRUE)
else()
  set(ARROW_SUBSTRAIT_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_SUBSTRAIT_IMPORT_LIB
                 ARROW_SUBSTRAIT_INCLUDE_DIR
                 ARROW_SUBSTRAIT_LIBS
                 ARROW_SUBSTRAIT_LIB_DIR
                 ARROW_SUBSTRAIT_SHARED_IMP_LIB
                 ARROW_SUBSTRAIT_SHARED_LIB
                 ARROW_SUBSTRAIT_STATIC_LIB
                 ARROW_SUBSTRAIT_VERSION
                 ARROW_SUBSTRAIT_VERSION_MATCH)

find_package_handle_standard_args(
  ArrowSubstrait
  REQUIRED_VARS ARROW_SUBSTRAIT_INCLUDE_DIR ARROW_SUBSTRAIT_LIB_DIR
                ARROW_SUBSTRAIT_VERSION_MATCH
  VERSION_VAR ARROW_SUBSTRAIT_VERSION)
set(ARROW_SUBSTRAIT_FOUND ${ArrowSubstrait_FOUND})

if(ArrowSubstrait_FOUND AND NOT ArrowSubstrait_FIND_QUIETLY)
  message(STATUS "Found the Arrow Substrait by ${ARROW_SUBSTRAIT_FIND_APPROACH}")
  message(STATUS "Found the Arrow Substrait shared library: ${ARROW_SUBSTRAIT_SHARED_LIB}"
  )
  message(STATUS "Found the Arrow Substrait import library: ${ARROW_SUBSTRAIT_IMPORT_LIB}"
  )
  message(STATUS "Found the Arrow Substrait static library: ${ARROW_SUBSTRAIT_STATIC_LIB}"
  )
endif()
