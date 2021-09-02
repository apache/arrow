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
# arrow_ffi_java
#

# Headers: top level
cmake_minimum_required(VERSION 3.11)
project(arrow_ffi_java)

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

set(JNI_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
                    ${JNI_INCLUDE_DIRS} ${JNI_HEADERS_DIR})

add_jar(${PROJECT_NAME}
        src/main/java/org/apache/arrow/ffi/jni/JniLoader.java
        src/main/java/org/apache/arrow/ffi/jni/JniWrapper.java
        src/main/java/org/apache/arrow/ffi/jni/PrivateData.java
        GENERATE_NATIVE_HEADERS
        arrow_ffi_java-native
        DESTINATION
        ${JNI_HEADERS_DIR})

set(SOURCES src/main/cpp/jni_wrapper.cc)
add_library(arrow_ffi_jni SHARED ${SOURCES})
target_link_libraries(arrow_ffi_jni ${JAVA_JVM_LIBRARY})
add_dependencies(arrow_ffi_jni ${PROJECT_NAME})
