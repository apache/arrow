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

project(gandiva_jni)

# Find protobuf
set(Protobuf_USE_STATIC_LIBS "ON")
find_package(Protobuf REQUIRED)

# Find JNI
find_package(JNI REQUIRED)

set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(PROTO_OUTPUT_FILES "${PROTO_OUTPUT_DIR}/Types.pb.cc")
set(PROTO_OUTPUT_FILES ${PROTO_OUTPUT_FILES} "${PROTO_OUTPUT_DIR}/Types.pb.h")

set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(ABS_GANDIVA_PROTO ${CMAKE_SOURCE_DIR}/src/gandiva/proto/Types.proto ABSOLUTE)

add_custom_command(
  OUTPUT ${PROTO_OUTPUT_FILES}
  COMMAND ${PROTOBUF_EXECUTABLE} --proto_path ${CMAKE_SOURCE_DIR}/src/gandiva/proto --cpp_out ${PROTO_OUTPUT_DIR} ${CMAKE_SOURCE_DIR}/src/gandiva/proto/Types.proto
  DEPENDS ${ABS_GANDIVA_PROTO}
  COMMENT "Running PROTO compiler on Types.proto"
  VERBATIM
)

add_custom_target(gandiva_jni_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})
set(PROTO_SRCS "${PROTO_OUTPUT_DIR}/Types.pb.cc")
set(PROTO_HDRS "${PROTO_OUTPUT_DIR}/Types.pb.h")

# Create the jni header file (from the java class).
set(JNI_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/java")
add_subdirectory(../../../../java/gandiva ./java/gandiva)

add_library(gandiva_jni SHARED
  config_builder.cc
  config_holder.cc
  expression_registry_helper.cc
  jni_common.cc
  ${PROTO_SRCS})
add_dependencies(gandiva_jni gandiva_java gandiva_jni_proto)

# For users of gandiva_jni library (including integ tests), include-dir is :
#   /usr/**/include dir after install,
#   cpp/include during build
# For building gandiva_jni library itself, include-dir (in addition to above) is :
#   cpp/src
target_include_directories(gandiva_jni
  PUBLIC
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    ${JNI_HEADERS_DIR}
  PRIVATE
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/src
)

# filter out everything that is not needed for the jni bridge
# statically linked stdc++ has conflicts with stdc++ loaded by other libraries.
if (NOT APPLE)
set_target_properties(gandiva_jni PROPERTIES LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/src/gandiva/jni/symbols.map")
endif()

# PROTOBUF is a private dependency i.e users of gandiva also will not have a dependency on protobuf.
target_link_libraries(gandiva_jni
  PRIVATE
    ${PROTOBUF_STATIC_LIB}
    gandiva_static
)
