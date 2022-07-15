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

cmake_minimum_required(VERSION 3.11)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(arrow-jni)

if("${CMAKE_CXX_STANDARD}" STREQUAL "")
  set(CMAKE_CXX_STANDARD 11)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ARROW_JNI_ENABLE_DEFAULT "Whether enable components by default or not" ON)

option(ARROW_JNI_ENABLE_C "Enable C data interface" ${ARROW_JNI_ENABLE_DEFAULT})

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)

if(ARROW_JNI_ENABLE_C)
  add_subdirectory(c)
endif()
