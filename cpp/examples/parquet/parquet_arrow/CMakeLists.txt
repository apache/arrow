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

# Require cmake that supports BYPRODUCTS in add_custom_command, ExternalProject_Add [1].
cmake_minimum_required(VERSION 3.2.0)

project(parquet_arrow_example)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

option(PARQUET_LINK_SHARED "Link to the Parquet shared library" ON)

# This ensures that things like gnu++11 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 11)
endif()

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Look for installed packages the system
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(parquet-arrow-example reader_writer.cc)
if(PARQUET_LINK_SHARED)
  target_link_libraries(parquet-arrow-example parquet_shared)
else()
  target_link_libraries(parquet-arrow-example parquet_static)
endif()
