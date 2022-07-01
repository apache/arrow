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
# specific language governing permissions and limitationsn
# under the License.

#
# arrow_dataset_jni
#

project(arrow_dataset_jni)

cmake_minimum_required(VERSION 3.11)

find_package(JNI REQUIRED)

add_custom_target(arrow_dataset_jni)

set(JNI_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

add_subdirectory(../../../../dataset ./java)

set(ARROW_BUILD_STATIC OFF)

set(ARROW_DATASET_JNI_LIBS arrow_dataset_static)

set(ARROW_DATASET_JNI_SOURCES jni_wrapper.cc jni_util.cc)

add_arrow_lib(arrow_dataset_jni
              BUILD_SHARED
              SOURCES
              ${ARROW_DATASET_JNI_SOURCES}
              OUTPUTS
              ARROW_DATASET_JNI_LIBRARIES
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_DATASET_JNI_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_JNI_LIBS}
              EXTRA_INCLUDES
              ${JNI_HEADERS_DIR}
              PRIVATE_INCLUDES
              ${JNI_INCLUDE_DIRS}
              DEPENDENCIES
              arrow_static
              arrow_dataset_java)

add_dependencies(arrow_dataset_jni ${ARROW_DATASET_JNI_LIBRARIES})

add_arrow_test(dataset_jni_test
               SOURCES
               jni_util_test.cc
               jni_util.cc
               EXTRA_INCLUDES
               ${JNI_INCLUDE_DIRS})
