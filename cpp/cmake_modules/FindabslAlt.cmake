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

if(abslAlt_FOUND)
  return()
endif()

set(find_package_args)

if(abslAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
if(abslAlt_FIND_REQUIRED)
  list(APPEND find_package_args REQUIRED)
endif()

find_package(absl ${find_package_args})

if(NOT DEFINED absl_VERSION)
  # Abseil does not define a version when build 'live at head'.
  # As this is their recommended path we need to define a large version to pass version checks.
  # CMake removes the '_head' suffix for version comparison but it will show up in the logs
  # and matches the abseil-cpp.pc version of 'head'
  set(absl_VERSION 99999999_head)
endif()

set(abslAlt_VERSION ${absl_VERSION})

find_package_handle_standard_args(
  abslAlt
  REQUIRED_VARS absl_FOUND
  VERSION_VAR abslAlt_VERSION)
