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

cmake_minimum_required(VERSION 3.16)

project(ArrowTutorialExamples)

find_package(ArrowDataset)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra")

set(CMAKE_BUILD_TYPE Release)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_executable(arrow_example arrow_example.cc)
target_link_libraries(arrow_example PRIVATE Arrow::arrow_shared)

add_executable(file_access_example file_access_example.cc)
target_link_libraries(file_access_example PRIVATE Arrow::arrow_shared
                                                  Parquet::parquet_shared)

add_executable(compute_example compute_example.cc)
target_link_libraries(compute_example PRIVATE Arrow::arrow_shared)

add_executable(dataset_example dataset_example.cc)
target_link_libraries(dataset_example PRIVATE ArrowDataset::arrow_dataset_shared)
