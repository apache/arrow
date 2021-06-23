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

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/generated)
set(PROTO_OUTPUT_FILES "${PROTO_OUTPUT_DIR}/gandiva_cache_file.pb.h")
set(PROTO_OUTPUT_FILES ${PROTO_OUTPUT_FILES} "${PROTO_OUTPUT_DIR}/gandiva_cache_file.pb.cc")

add_custom_target(gandiva_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})

set(PROTO_SRCS "${PROTO_OUTPUT_DIR}/gandiva_cache_file.pb.cc")
set(PROTO_HDRS "${PROTO_OUTPUT_DIR}/gandiva_cache_file.pb.h")

set(GANDIVA_LINK_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF})
if(ARROW_BUILD_STATIC)
    list(APPEND GANDIVA_LINK_LIBS gandiva_static)
else()
    list(APPEND GANDIVA_LINK_LIBS gandiva_shared)
endif()

set(GANDIVA_PROTO_SOURCES
        ${PROTO_SRCS})

add_arrow_lib(gandiva_proto
        SOURCES
        ${GANDIVA_PROTO_SOURCES}
        OUTPUTS
        GANDIVA_PROTO_LIBRARIES
        SHARED_PRIVATE_LINK_LIBS
        ${PROTOBUF_PROTOC_LIBRARY}
        ${GANDIVA_LINK_LIBS}
        STATIC_LINK_LIBS
        ${GANDIVA_LINK_LIBS}
        DEPENDENCIES
        ${GANDIVA_LINK_LIBS}
        EXTRA_INCLUDES
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        PRIVATE_INCLUDES
        ${CMAKE_CURRENT_BINARY_DIR})

add_dependencies(gandiva ${GANDIVA_PROTO_LIBRARIES})

