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
               PATHS ${c-ares_ROOT}
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(CARES_INCLUDE_DIR
            NAMES ares.h
            PATHS ${c-ares_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
else()
  find_library(CARES_LIB NAMES cares PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(CARES_INCLUDE_DIR NAMES ares.h PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(c-aresAlt REQUIRED_VARS CARES_INCLUDE_DIR CARES_LIB)

if(c-aresAlt_FOUND)
  add_library(c-ares::cares UNKNOWN IMPORTED)
  set_target_properties(c-ares::cares
                        PROPERTIES IMPORTED_LOCATION "${CARES_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${CARES_INCLUDE_DIR}")
endif()
