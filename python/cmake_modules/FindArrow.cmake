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

# - Find ARROW (arrow/api.h, libarrow.a, libarrow.so)
# This module defines
#  ARROW_INCLUDE_DIR, directory containing headers
#  ARROW_LIBS, directory containing arrow libraries
#  ARROW_STATIC_LIB, path to libarrow.a
#  ARROW_SHARED_LIB, path to libarrow's shared library
#  ARROW_FOUND, whether arrow has been found

set(ARROW_SEARCH_HEADER_PATHS
  $ENV{ARROW_HOME}/include
)

set(ARROW_SEARCH_LIB_PATH
  $ENV{ARROW_HOME}/lib
)

find_path(ARROW_INCLUDE_DIR arrow/array.h PATHS
  ${ARROW_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(ARROW_LIB_PATH NAMES arrow
  PATHS
  ${ARROW_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

find_library(ARROW_IO_LIB_PATH NAMES arrow_io
  PATHS
  ${ARROW_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

if (ARROW_INCLUDE_DIR AND ARROW_LIB_PATH)
  set(ARROW_FOUND TRUE)
  set(ARROW_LIB_NAME libarrow)
  set(ARROW_IO_LIB_NAME libarrow_io)

  set(ARROW_LIBS ${ARROW_SEARCH_LIB_PATH})
  set(ARROW_STATIC_LIB ${ARROW_SEARCH_LIB_PATH}/${ARROW_LIB_NAME}.a)
  set(ARROW_SHARED_LIB ${ARROW_LIBS}/${ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})

  set(ARROW_IO_STATIC_LIB ${ARROW_SEARCH_LIB_PATH}/${ARROW_IO_LIB_NAME}.a)
  set(ARROW_IO_SHARED_LIB ${ARROW_LIBS}/${ARROW_IO_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  if (NOT Arrow_FIND_QUIETLY)
    message(STATUS "Found the Arrow core library: ${ARROW_LIB_PATH}")
    message(STATUS "Found the Arrow IO library: ${ARROW_IO_LIB_PATH}")
  endif ()
else ()
  if (NOT Arrow_FIND_QUIETLY)
    set(ARROW_ERR_MSG "Could not find the Arrow library. Looked for headers")
    set(ARROW_ERR_MSG "${ARROW_ERR_MSG} in ${ARROW_SEARCH_HEADER_PATHS}, and for libs")
    set(ARROW_ERR_MSG "${ARROW_ERR_MSG} in ${ARROW_SEARCH_LIB_PATH}")
    if (Arrow_FIND_REQUIRED)
      message(FATAL_ERROR "${ARROW_ERR_MSG}")
    else (Arrow_FIND_REQUIRED)
      message(STATUS "${ARROW_ERR_MSG}")
    endif (Arrow_FIND_REQUIRED)
  endif ()
  set(ARROW_FOUND FALSE)
endif ()

mark_as_advanced(
  ARROW_INCLUDE_DIR
  ARROW_LIBS
  ARROW_STATIC_LIB
  ARROW_SHARED_LIB
  ARROW_IO_STATIC_LIB
  ARROW_IO_SHARED_LIB
)
