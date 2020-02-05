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

if(RapidJSON_ROOT)
  find_path(RAPIDJSON_INCLUDE_DIR
            NAMES rapidjson/rapidjson.h
            PATHS ${RapidJSON_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES "include")
else()
  find_path(RAPIDJSON_INCLUDE_DIR NAMES rapidjson/rapidjson.h PATH_SUFFIXES "include")
endif()

if(RAPIDJSON_INCLUDE_DIR)
  file(READ "${RAPIDJSON_INCLUDE_DIR}/rapidjson/rapidjson.h" RAPIDJSON_H_CONTENT)
  string(REGEX MATCH "#define RAPIDJSON_MAJOR_VERSION ([0-9]+)"
               RAPIDJSON_MAJOR_VERSION_DEFINITION "${RAPIDJSON_H_CONTENT}")
  string(REGEX
         REPLACE "^.+ ([0-9]+)$" "\\1" RAPIDJSON_MAJOR_VERSION
                 "${RAPIDJSON_MAJOR_VERSION_DEFINITION}")
  string(REGEX MATCH "#define RAPIDJSON_MINOR_VERSION ([0-9]+)"
               RAPIDJSON_MINOR_VERSION_DEFINITION "${RAPIDJSON_H_CONTENT}")
  string(REGEX
         REPLACE "^.+ ([0-9]+)$" "\\1" RAPIDJSON_MINOR_VERSION
                 "${RAPIDJSON_MINOR_VERSION_DEFINITION}")
  string(REGEX MATCH "#define RAPIDJSON_PATCH_VERSION ([0-9]+)"
               RAPIDJSON_PATCH_VERSION_DEFINITION "${RAPIDJSON_H_CONTENT}")
  string(REGEX
         REPLACE "^.+ ([0-9]+)$" "\\1" RAPIDJSON_PATCH_VERSION
                 "${RAPIDJSON_PATCH_VERSION_DEFINITION}")
  if("${RAPIDJSON_MAJOR_VERSION}" STREQUAL ""
     OR "${RAPIDJSON_MINOR_VERSION}" STREQUAL ""
     OR "${RAPIDJSON_PATCH_VERSION}" STREQUAL "")
    set(RAPIDJSON_VERSION "0.0.0")
  else()
    set(
      RAPIDJSON_VERSION
      "${RAPIDJSON_MAJOR_VERSION}.${RAPIDJSON_MINOR_VERSION}.${RAPIDJSON_PATCH_VERSION}")
  endif()
endif()

find_package_handle_standard_args(RapidJSONAlt
                                  REQUIRED_VARS
                                  RAPIDJSON_INCLUDE_DIR
                                  VERSION_VAR
                                  RAPIDJSON_VERSION)
