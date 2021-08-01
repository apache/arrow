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

if(DEFINED ARROW_SKYHOOK_FOUND)
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

find_package(ArrowDataset ${find_package_arguments})
find_package(Parquet ${find_package_arguments})

if(ARROW_DATASET_FOUND AND PARQUET_FOUND)
  arrow_find_package(ARROW_SKYHOOK
                     "${ARROW_HOME}"
                     cls_arrow
                     ArrowCls
                     arrow-cls)
  if(NOT ARROW_SKYHOOK_VERSION)
    set(ARROW_SKYHOOK_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_SKYHOOK_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_SKYHOOK_VERSION_MATCH TRUE)
else()
  set(ARROW_SKYHOOK_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_SKYHOOK_IMPORT_LIB
                 ARROW_SKYHOOK_INCLUDE_DIR
                 ARROW_SKYHOOK_LIBS
                 ARROW_SKYHOOK_LIB_DIR
                 ARROW_SKYHOOK_SHARED_IMP_LIB
                 ARROW_SKYHOOK_SHARED_LIB
                 ARROW_SKYHOOK_STATIC_LIB
                 ARROW_SKYHOOK_VERSION
                 ARROW_SKYHOOK_VERSION_MATCH)

find_package_handle_standard_args(
  ArrowCls
  REQUIRED_VARS ARROW_SKYHOOK_INCLUDE_DIR ARROW_SKYHOOK_LIB_DIR
                ARROW_SKYHOOK_VERSION_MATCH
  VERSION_VAR ARROW_SKYHOOK_VERSION)
set(ARROW_SKYHOOK_FOUND ${ArrowCls_FOUND})

if(ArrowCls_FOUND AND NOT ArrowCls_FIND_QUIETLY)
  message(STATUS "Found the Arrow Cls by ${ARROW_SKYHOOK_FIND_APPROACH}")
  message(STATUS "Found the Arrow Cls shared library: ${ARROW_SKYHOOK_SHARED_LIB}")
  message(STATUS "Found the Arrow Cls import library: ${ARROW_SKYHOOK_IMPORT_LIB}")
  message(STATUS "Found the Arrow Cls static library: ${ARROW_SKYHOOK_STATIC_LIB}")
endif()
