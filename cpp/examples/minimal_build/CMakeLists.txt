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

cmake_minimum_required(VERSION 3.0)

project(ArrowMinimalExample)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)

find_package(Arrow REQUIRED)

set(CMAKE_CXX_STANDARD 11)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_executable(arrow_example example.cc)

if (ARROW_LINK_SHARED)
  target_link_libraries(arrow_example PRIVATE arrow_shared)
else()
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)

  # We must also link to libarrow_bundled_dependencies.a
  get_property(arrow_static_loc TARGET arrow_static PROPERTY LOCATION)
  get_filename_component(arrow_lib_dir ${arrow_static_loc} DIRECTORY)

  add_library(arrow_dependencies STATIC IMPORTED)
  set_target_properties(arrow_dependencies
    PROPERTIES IMPORTED_LOCATION
    "${arrow_lib_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}")
  target_link_libraries(arrow_example PRIVATE arrow_static arrow_dependencies Threads::Threads)
endif()
