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

# - Find Arrow CUDA (arrow/gpu/cuda_api.h, libarrow_cuda.a, libarrow_cuda.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_CUDA_FOUND, whether Arrow CUDA has been found
#  ARROW_CUDA_IMPORT_LIB, path to libarrow_cuda's import library (Windows only)
#  ARROW_CUDA_INCLUDE_DIR, directory containing headers
#  ARROW_CUDA_LIBS, deprecated. Use ARROW_CUDA_LIB_DIR instead
#  ARROW_CUDA_LIB_DIR, directory containing Arrow CUDA libraries
#  ARROW_CUDA_SHARED_IMP_LIB, deprecated. Use ARROW_CUDA_IMPORT_LIB instead
#  ARROW_CUDA_SHARED_LIB, path to libarrow_cuda's shared library
#  ARROW_CUDA_STATIC_LIB, path to libarrow_cuda.a

if(DEFINED ARROW_CUDA_FOUND)
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
  arrow_find_package(ARROW_CUDA
                     "${ARROW_HOME}"
                     arrow_cuda
                     arrow/gpu/cuda_api.h
                     ArrowCUDA
                     arrow-cuda)
  if(NOT ARROW_CUDA_VERSION)
    set(ARROW_CUDA_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_CUDA_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_CUDA_VERSION_MATCH TRUE)
else()
  set(ARROW_CUDA_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_CUDA_IMPORT_LIB
                 ARROW_CUDA_INCLUDE_DIR
                 ARROW_CUDA_LIBS
                 ARROW_CUDA_LIB_DIR
                 ARROW_CUDA_SHARED_IMP_LIB
                 ARROW_CUDA_SHARED_LIB
                 ARROW_CUDA_STATIC_LIB
                 ARROW_CUDA_VERSION
                 ARROW_CUDA_VERSION_MATCH)

find_package_handle_standard_args(ArrowCUDA
                                  REQUIRED_VARS
                                  ARROW_CUDA_INCLUDE_DIR
                                  ARROW_CUDA_LIB_DIR
                                  ARROW_CUDA_VERSION_MATCH
                                  VERSION_VAR
                                  ARROW_CUDA_VERSION)
set(ARROW_CUDA_FOUND ${ArrowCUDA_FOUND})

if(ArrowCUDA_FOUND AND NOT ArrowCUDA_FIND_QUIETLY)
  message(STATUS "Found the Arrow CUDA by ${ARROW_CUDA_FIND_APPROACH}")
  message(STATUS "Found the Arrow CUDA shared library: ${ARROW_CUDA_SHARED_LIB}")
  message(STATUS "Found the Arrow CUDA import library: ${ARROW_CUDA_IMPORT_LIB}")
  message(STATUS "Found the Arrow CUDA static library: ${ARROW_CUDA_STATIC_LIB}")
endif()
