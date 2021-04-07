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

add_subdirectory(../../../../java/dataset ./java)

set(ARROW_BUILD_STATIC OFF)

set(ARROW_DATASET_JNI_LIBS arrow_dataset_static ${ARROW_PROTOBUF_LIBPROTOBUF})
set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(PROTO_OUTPUT_FILES "${PROTO_OUTPUT_DIR}/record_batch.pb.cc")
set(PROTO_OUTPUT_FILES ${PROTO_OUTPUT_FILES} "${PROTO_OUTPUT_DIR}/record_batch.pb.h")

set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(ABS_ARROW_DATASET_PROTO
                       ${CMAKE_SOURCE_DIR}/src/jni/dataset/proto/record_batch.proto
                       ABSOLUTE)

add_custom_command(OUTPUT ${PROTO_OUTPUT_FILES}
                   COMMAND ${ARROW_PROTOBUF_PROTOC}
                           --proto_path
                           ${CMAKE_SOURCE_DIR}/src/jni/dataset/proto
                           --cpp_out
                           ${PROTO_OUTPUT_DIR}
                           ${CMAKE_SOURCE_DIR}/src/jni/dataset/proto/record_batch.proto
                   DEPENDS ${ABS_ARROW_DATASET_PROTO} ${ARROW_PROTOBUF_LIBPROTOBUF}
                   COMMENT "Running PROTO compiler on record_batch.proto"
                   VERBATIM)

add_custom_target(arrow_dataset_jni_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})

set(PROTO_SRCS "${PROTO_OUTPUT_DIR}/record_batch.pb.cc")
set(PROTO_HDRS "${PROTO_OUTPUT_DIR}/record_batch.pb.h")
set(ARROW_DATASET_JNI_SOURCES jni_wrapper.cc jni_util.cc ${PROTO_SRCS})

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
              arrow_dataset_java
              arrow_dataset_jni_proto)

add_dependencies(arrow_dataset_jni ${ARROW_DATASET_JNI_LIBRARIES})

add_arrow_test(dataset_jni_test
               SOURCES
               jni_util_test.cc
               jni_util.cc
               EXTRA_INCLUDES
               ${JNI_INCLUDE_DIRS})
