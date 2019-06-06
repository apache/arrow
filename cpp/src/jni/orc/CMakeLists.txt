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

#
# arrow_orc_jni
#

project(arrow_orc_jni)

cmake_minimum_required(VERSION 3.11)

find_package(JNI REQUIRED)

add_custom_target(arrow_orc_jni)

set(JNI_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

add_subdirectory(../../../../java/adapter/orc ./java)

set(ARROW_BUILD_STATIC OFF)

add_arrow_lib(arrow_orc_jni
              BUILD_SHARED
              SOURCES
              jni_wrapper.cpp
              OUTPUTS
              ARROW_ORC_JNI_LIBRARIES
              SHARED_PRIVATE_LINK_LIBS
              arrow_static
              EXTRA_INCLUDES
              ${JNI_HEADERS_DIR}
              PRIVATE_INCLUDES
              ${JNI_INCLUDE_DIRS}
              ${CMAKE_CURRENT_BINARY_DIR}
              DEPENDENCIES
              arrow_static
              arrow_orc_java
              OUTPUT_PATH
              ${CMAKE_CURRENT_BINARY_DIR})

add_dependencies(arrow_orc_jni ${ARROW_ORC_JNI_LIBRARIES})
