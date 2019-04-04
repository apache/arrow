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

# - Find ARROW CUDA (arrow/gpu/cuda_api.h, libarrow_cuda.a, libarrow_cuda.so)
#
# This module requires Arrow from which it uses
#   ARROW_FOUND
#   ARROW_SEARCH_HEADER_PATHS
#   ARROW_SEARCH_LIB_PATH
#   ARROW_HOME
#
# This module defines
#  ARROW_CUDA_INCLUDE_DIR, directory containing headers
#  ARROW_CUDA_LIBS, directory containing arrow libraries
#  ARROW_CUDA_STATIC_LIB, path to libarrow.a
#  ARROW_CUDA_SHARED_LIB, path to libarrow's shared library
#  ARROW_CUDA_SHARED_IMP_LIB, path to libarrow's import library (MSVC only)
#  ARROW_CUDA_FOUND, whether arrow has been found

include(FindPkgConfig)
include(GNUInstallDirs)

if(NOT DEFINED ARROW_FOUND)
  if(ArrowCuda_FIND_REQUIRED)
    find_package(Arrow REQUIRED)
  else()
    find_package(Arrow)
  endif()
endif()

if(NOT ARROW_FOUND)
  set(ARROW_CUDA_FOUND FALSE)
  return()
endif()

find_path(ARROW_CUDA_INCLUDE_DIR arrow/gpu/cuda_api.h
          PATHS ${ARROW_SEARCH_HEADER_PATHS}
          PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES}
          NO_DEFAULT_PATH)

if(NOT (ARROW_CUDA_INCLUDE_DIR STREQUAL ARROW_INCLUDE_DIR))
  set(ARROW_CUDA_WARN_MSG "Mismatch of Arrow and Arrow CUDA include directories:")
  set(ARROW_CUDA_WARN_MSG
      "${ARROW_CUDA_WARN_MSG}  ARROW_INCLUDE_DIR=${ARROW_INCLUDE_DIR}")
  set(ARROW_CUDA_WARN_MSG
      "${ARROW_CUDA_WARN_MSG}  ARROW_CUDA_INCLUDE_DIR=${ARROW_CUDA_INCLUDE_DIR}")
  message(WARNING ${ARROW_CUDA_WARN_MSG})
endif()

find_library(ARROW_CUDA_LIB_PATH
             NAMES arrow_cuda
             PATHS ${ARROW_SEARCH_LIB_PATH}
             PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
             NO_DEFAULT_PATH)
get_filename_component(ARROW_CUDA_LIBS ${ARROW_CUDA_LIB_PATH} DIRECTORY)

if(MSVC)
  find_library(ARROW_CUDA_SHARED_LIBRARIES
               NAMES arrow_cuda
               PATHS ${ARROW_HOME}
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  get_filename_component(ARROW_CUDA_SHARED_LIBS ${ARROW_CUDA_SHARED_LIBRARIES} PATH)
endif()

if(ARROW_CUDA_INCLUDE_DIR AND ARROW_CUDA_LIBS)
  set(ARROW_CUDA_FOUND TRUE)
  set(ARROW_CUDA_LIB_NAME arrow_cuda)
  if(MSVC)
    set(
      ARROW_CUDA_STATIC_LIB
      ${ARROW_CUDA_LIBS}/${ARROW_CUDA_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
      )
    set(ARROW_CUDA_SHARED_LIB
        ${ARROW_CUDA_SHARED_LIBS}/${ARROW_CUDA_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_CUDA_SHARED_IMP_LIB ${ARROW_CUDA_LIBS}/${ARROW_CUDA_LIB_NAME}.lib)
  else()
    set(ARROW_CUDA_STATIC_LIB ${ARROW_LIBS}/lib${ARROW_CUDA_LIB_NAME}.a)
    set(ARROW_CUDA_SHARED_LIB
        ${ARROW_LIBS}/lib${ARROW_CUDA_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
endif()

if(ARROW_CUDA_FOUND)
  if(NOT ArrowCuda_FIND_QUIETLY)
    message(STATUS "Found the Arrow CUDA library: ${ARROW_CUDA_LIB_PATH}")
  endif()
else()
  if(NOT ArrowCuda_FIND_QUIETLY)
    set(ARROW_CUDA_ERR_MSG "Could not find the Arrow CUDA library. Looked for headers")
    set(ARROW_CUDA_ERR_MSG
        "${ARROW_CUDA_ERR_MSG} in ${ARROW_SEARCH_HEADER_PATHS}, and for libs")
    set(ARROW_CUDA_ERR_MSG "${ARROW_CUDA_ERR_MSG} in ${ARROW_SEARCH_LIB_PATH}")
    if(ArrowCuda_FIND_REQUIRED)
      message(FATAL_ERROR "${ARROW_CUDA_ERR_MSG}")
    else(ArrowCuda_FIND_REQUIRED)
      message(STATUS "${ARROW_CUDA_ERR_MSG}")
    endif(ArrowCuda_FIND_REQUIRED)
  endif()
  set(ARROW_CUDA_FOUND FALSE)
endif()

if(MSVC)
  mark_as_advanced(ARROW_CUDA_INCLUDE_DIR ARROW_CUDA_STATIC_LIB ARROW_CUDA_SHARED_LIB
                   ARROW_CUDA_SHARED_IMP_LIB)
else()
  mark_as_advanced(ARROW_CUDA_INCLUDE_DIR ARROW_CUDA_STATIC_LIB ARROW_CUDA_SHARED_LIB)
endif()
