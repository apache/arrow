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

if(SimdjsonAlt_FOUND)
  return()
endif()

# Manual search failed, try CMake config mode
set(find_package_args)
if(SimdjsonAlt_FIND_VERSION)
  list(APPEND find_package_args ${SimdjsonAlt_FIND_VERSION})
endif()
if(SimdjsonAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(simdjson ${find_package_args} CONFIG)
if(simdjson_FOUND)
  set(SimdjsonAlt_FOUND TRUE)
endif()
