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

# - Find Arrow Dataset (arrow/dataset/api.h, libarrow_dataset.a, libarrow_dataset.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_DATASET_FOUND, whether Arrow Dataset has been found
#  ARROW_DATASET_IMPORT_LIB,
#    path to libarrow_dataset's import library (Windows only)
#  ARROW_DATASET_INCLUDE_DIR, directory containing headers
#  ARROW_DATASET_LIB_DIR, directory containing Arrow Dataset libraries
#  ARROW_DATASET_SHARED_LIB, path to libarrow_dataset's shared library
#  ARROW_DATASET_STATIC_LIB, path to libarrow_dataset.a

if(DEFINED ARROW_DATASET_FOUND)
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
  arrow_find_package(ARROW_DATASET
                     "${ARROW_HOME}"
                     arrow_dataset
                     arrow/dataset/api.h
                     ArrowDataset
                     arrow-dataset)
  if(NOT ARROW_DATASET_VERSION)
    set(ARROW_DATASET_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_DATASET_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_DATASET_VERSION_MATCH TRUE)
else()
  set(ARROW_DATASET_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_DATASET_IMPORT_LIB
                 ARROW_DATASET_INCLUDE_DIR
                 ARROW_DATASET_LIBS
                 ARROW_DATASET_LIB_DIR
                 ARROW_DATASET_SHARED_IMP_LIB
                 ARROW_DATASET_SHARED_LIB
                 ARROW_DATASET_STATIC_LIB
                 ARROW_DATASET_VERSION
                 ARROW_DATASET_VERSION_MATCH)

find_package_handle_standard_args(ArrowDataset
                                  REQUIRED_VARS
                                  ARROW_DATASET_INCLUDE_DIR
                                  ARROW_DATASET_LIB_DIR
                                  ARROW_DATASET_VERSION_MATCH
                                  VERSION_VAR
                                  ARROW_DATASET_VERSION)
set(ARROW_DATASET_FOUND ${ArrowDataset_FOUND})

if(ArrowDataset_FOUND AND NOT ArrowDataset_FIND_QUIETLY)
  message(STATUS "Found the Arrow Dataset by ${ARROW_DATASET_FIND_APPROACH}")
  message(STATUS "Found the Arrow Dataset shared library: ${ARROW_DATASET_SHARED_LIB}")
  message(STATUS "Found the Arrow Dataset import library: ${ARROW_DATASET_IMPORT_LIB}")
  message(STATUS "Found the Arrow Dataset static library: ${ARROW_DATASET_STATIC_LIB}")
endif()
