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

# find_package() uses <PackageName>_ROOT variables.
# https://cmake.org/cmake/help/latest/policy/CMP0074.html
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(arrow-java-jni)

if("${CMAKE_CXX_STANDARD}" STREQUAL "")
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Components
option(ARROW_JAVA_JNI_ENABLE_DEFAULT "Whether enable components by default or not" ON)
option(ARROW_JAVA_JNI_ENABLE_C "Enable C data interface" ${ARROW_JAVA_JNI_ENABLE_DEFAULT})
option(ARROW_JAVA_JNI_ENABLE_DATASET "Enable dataset" ${ARROW_JAVA_JNI_ENABLE_DEFAULT})
option(ARROW_JAVA_JNI_ENABLE_GANDIVA "Enable Gandiva" ${ARROW_JAVA_JNI_ENABLE_DEFAULT})
option(ARROW_JAVA_JNI_ENABLE_ORC "Enable ORC" ${ARROW_JAVA_JNI_ENABLE_DEFAULT})

include(GNUInstallDirs)

# ccache
option(ARROW_JAVA_JNI_USE_CCACHE "Use ccache when compiling (if available)" ON)
if(ARROW_USE_CCACHE
   AND NOT CMAKE_C_COMPILER_LAUNCHER
   AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(CCACHE ccache)
  if(CCACHE)
    message(STATUS "Using ccache: ${CCACHE}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE})
    # ARROW-3985: let ccache preserve C++ comments, because some of them may be
    # meaningful to the compiler
    set(ENV{CCACHE_COMMENTS} "1")
  endif()
endif()

# Build
find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)

add_library(jni INTERFACE IMPORTED)
set_target_properties(jni PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${JNI_INCLUDE_DIRS}")

include(CTest)
if(BUILD_TESTING)
  find_package(ArrowTesting REQUIRED)
  find_package(GTest REQUIRED)
  add_library(arrow_java_test INTERFACE IMPORTED)
  target_link_libraries(arrow_java_test INTERFACE ArrowTesting::arrow_testing_static
                                                  GTest::gtest_main)
endif()

if(ARROW_JAVA_JNI_ENABLE_C)
  add_subdirectory(c)
endif()
if(ARROW_JAVA_JNI_ENABLE_DATASET)
  add_subdirectory(dataset)
endif()
if(ARROW_JAVA_JNI_ENABLE_GANDIVA)
  add_subdirectory(gandiva)
endif()
if(ARROW_JAVA_JNI_ENABLE_ORC)
  add_subdirectory(adapter/orc)
endif()
