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

if(Flatbuffers_ROOT)
  find_library(FLATBUFFERS_LIB
               NAMES flatbuffers
               PATHS ${Flatbuffers_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(FLATBUFFERS_INCLUDE_DIR
            NAMES flatbuffers/flatbuffers.h
            PATHS ${Flatbuffers_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
  find_program(FLATC
               NAMES flatc
               PATHS ${Flatbuffers_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES "bin")
else()
  find_library(FLATBUFFERS_LIB NAMES flatbuffers PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(FLATBUFFERS_INCLUDE_DIR
            NAMES flatbuffers/flatbuffers.h
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
  find_program(FLATC NAMES flatc PATHS PATH_SUFFIXES "bin")
endif()

find_package_handle_standard_args(FlatbuffersAlt
                                  REQUIRED_VARS
                                  FLATBUFFERS_LIB
                                  FLATBUFFERS_INCLUDE_DIR
                                  FLATC)

if(FlatbuffersAlt_FOUND)
  add_library(flatbuffers::flatbuffers UNKNOWN IMPORTED)
  set_target_properties(flatbuffers::flatbuffers
                        PROPERTIES IMPORTED_LOCATION "${FLATBUFFERS_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${FLATBUFFERS_INCLUDE_DIR}")
  add_executable(flatbuffers::flatc IMPORTED)
  set_target_properties(flatbuffers::flatc PROPERTIES IMPORTED_LOCATION "${FLATC}")
endif()
