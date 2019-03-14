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

if(c-ares_ROOT)
  find_library(CARES_LIB
    NAMES cares
    PATHS ${c-ares_ROOT} "${c-ares_ROOT}/Library"
    PATH_SUFFIXES "lib64" "lib" "bin"
    NO_DEFAULT_PATH)
  find_path(CARES_INCLUDE_DIR NAMES ares.h
    PATHS ${CARES_ROOT} "${CARES_ROOT}/Library"
    NO_DEFAULT_PATH
    PATH_SUFFIXES "include")
else()
  find_library(CARES_LIB
    NAMES cares
    PATH_SUFFIXES "lib64" "lib" "bin")
  find_path(CARES_INCLUDE_DIR NAMES ares.h
    PATH_SUFFIXES "include")
endif()

find_package_handle_standard_args(c-aresAlt
  REQUIRED_VARS CARES_INCLUDE_DIR CARES_LIB)

if (c-aresAlt_FOUND)
  add_library(c-ares::cares UNKNOWN IMPORTED)
  set_target_properties(c-ares::cares PROPERTIES
    IMPORTED_LOCATION "${CARES_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${CARES_INCLUDE_DIR}"
  )
endif()
