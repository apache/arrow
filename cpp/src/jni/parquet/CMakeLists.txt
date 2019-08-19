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
# arrow_parquet_jni
#

project(arrow_parquet_jni)

cmake_minimum_required(VERSION 3.11)

find_package(JNI REQUIRED)

add_custom_target(arrow_parquet_jni)

set(JNI_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

add_subdirectory(../../../../java/adapter/parquet ./java)

set(ARROW_BUILD_STATIC OFF)

add_arrow_lib(arrow_parquet_jni
              BUILD_SHARED
              SOURCES
              jni_wrapper.cpp
              HdfsConnector.cpp
              FileConnector.cpp
              ParquetReader.cpp
              ParquetWriter.cpp
              OUTPUTS
              ARROW_PARQUET_JNI_LIBRARIES
              SHARED_LINK_LIBS
              parquet
              arrow
              arrow_dataset
              EXTRA_INCLUDES
              ${JNI_HEADERS_DIR}
              PRIVATE_INCLUDES
              ${JNI_INCLUDE_DIRS}
              DEPENDENCIES
              arrow_parquet_java)

add_dependencies(arrow_parquet_jni ${ARROW_PARQUET_JNI_LIBRARIES})
