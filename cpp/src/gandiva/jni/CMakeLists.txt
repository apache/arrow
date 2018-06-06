# Copyright (C) 2017-2018 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project(gandiva_jni)

# Find protobuf
set(Protobuf_USE_STATIC_LIBS, ON)
find_package(Protobuf REQUIRED)

# generate the protobuf files from the proto definition.
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/../proto/Types.proto)

add_library(gandiva_jni STATIC
  #jni.cc
  ${PROTO_SRCS}
  ${PROTO_HDRS})

# For users of gandiva_jni library (including integ tests), include-dir is :
#   /usr/**/include dir after install,
#   cpp/include during build
# For building gandiva_jni library itself, include-dir (in addition to above) is :
#   cpp/src
target_include_directories(gandiva_jni
  PUBLIC
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

# ARROW is a public dependency i.e users of gandiva also will have a dependency on arrow.
target_link_libraries(gandiva_jni
  PRIVATE
    protobuf::libprotobuf
)
