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

set(SNAPPY_STATIC_LIB_SUFFIX "${SNAPPY_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

if(ARROW_SNAPPY_USE_SHARED)
  set(SNAPPY_LIB_NAMES snappy)
else()
  set(SNAPPY_LIB_NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}snappy${SNAPPY_STATIC_LIB_SUFFIX}" snappy)
endif()

if(Snappy_ROOT)
  find_library(Snappy_LIB
               NAMES ${SNAPPY_LIB_NAMES}
               PATHS ${Snappy_ROOT}
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(Snappy_INCLUDE_DIR
            NAMES snappy.h
            PATHS ${Snappy_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
else()
  find_library(Snappy_LIB NAMES ${SNAPPY_LIB_NAMES} HINTS "${CMAKE_ROOT}/Modules/")
  find_path(Snappy_INCLUDE_DIR NAMES snappy.h HINTS "${CMAKE_ROOT}/Modules/" PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(Snappy REQUIRED_VARS Snappy_LIB Snappy_INCLUDE_DIR)

if(Snappy_FOUND)
  add_library(Snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(Snappy::snappy
                        PROPERTIES IMPORTED_LOCATION "${Snappy_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${Snappy_INCLUDE_DIR}")
endif()
