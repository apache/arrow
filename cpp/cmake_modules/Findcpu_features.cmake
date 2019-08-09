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

if(cpu_features_ROOT)
  find_library(cpu_features_LIB
               NAMES cpu_features
               PATHS ${cpu_features_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
else()
  find_library(cpu_features_LIB NAMES cpu_features)
endif()

find_package_handle_standard_args(cpu_features REQUIRED_VARS cpu_features_LIB
                                  cpu_features_INCLUDE_DIR)

if(cpu_features_FOUND)
  add_library(cpu_features::cpu_features UNKNOWN IMPORTED)
  set_target_properties(cpu_features::cpu_features
                        PROPERTIES IMPORTED_LOCATION "${cpu_features_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${cpu_features_INCLUDE_DIR}")
endif()
