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

# - Find Arrow Engine (arrow/engine/api.h, libarrow_engine.a, libarrow_engine.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_ENGINE_FOUND, whether Arrow Engine has been found
#  ARROW_ENGINE_IMPORT_LIB,
#    path to libarrow_engine's import library (Windows only)
#  ARROW_ENGINE_INCLUDE_DIR, directory containing headers
#  ARROW_ENGINE_LIB_DIR, directory containing Arrow Engine libraries
#  ARROW_ENGINE_SHARED_LIB, path to libarrow_engine's shared library
#  ARROW_ENGINE_STATIC_LIB, path to libarrow_engine.a

if(DEFINED ARROW_ENGINE_FOUND)
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
  arrow_find_package(ARROW_ENGINE
                     "${ARROW_HOME}"
                     arrow_engine
                     arrow/engine/api.h
                     ArrowEngine
                     arrow-engine)
  if(NOT ARROW_ENGINE_VERSION)
    set(ARROW_ENGINE_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_ENGINE_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_ENGINE_VERSION_MATCH TRUE)
else()
  set(ARROW_ENGINE_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_ENGINE_IMPORT_LIB
                 ARROW_ENGINE_INCLUDE_DIR
                 ARROW_ENGINE_LIBS
                 ARROW_ENGINE_LIB_DIR
                 ARROW_ENGINE_SHARED_IMP_LIB
                 ARROW_ENGINE_SHARED_LIB
                 ARROW_ENGINE_STATIC_LIB
                 ARROW_ENGINE_VERSION
                 ARROW_ENGINE_VERSION_MATCH)

find_package_handle_standard_args(
  ArrowEngine
  REQUIRED_VARS ARROW_ENGINE_INCLUDE_DIR ARROW_ENGINE_LIB_DIR ARROW_ENGINE_VERSION_MATCH
  VERSION_VAR ARROW_ENGINE_VERSION)
set(ARROW_ENGINE_FOUND ${ArrowEngine_FOUND})

if(ArrowEngine_FOUND AND NOT ArrowEngine_FIND_QUIETLY)
  message(STATUS "Found the Arrow Engine by ${ARROW_ENGINE_FIND_APPROACH}")
  message(STATUS "Found the Arrow Engine shared library: ${ARROW_ENGINE_SHARED_LIB}")
  message(STATUS "Found the Arrow Engine import library: ${ARROW_ENGINE_IMPORT_LIB}")
  message(STATUS "Found the Arrow Engine static library: ${ARROW_ENGINE_STATIC_LIB}")
endif()
