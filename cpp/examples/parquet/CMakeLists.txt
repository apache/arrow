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

add_executable(parquet-low-level-example low-level-api/reader-writer.cc)
add_executable(parquet-low-level-example2 low-level-api/reader-writer2.cc)
add_executable(parquet-with-index-page-skipping low-level-api/reader-writer-with-index.cc)
target_include_directories(parquet-low-level-example PRIVATE low-level-api/)
target_include_directories(parquet-low-level-example2 PRIVATE low-level-api/)
target_include_directories(parquet-with-index-page-skipping PRIVATE low-level-api/)
target_link_libraries(parquet-low-level-example parquet_static)
target_link_libraries(parquet-low-level-example2 parquet_static)
target_link_libraries(parquet-with-index-page-skipping parquet_static)

add_executable(parquet-arrow-example parquet-arrow/reader-writer.cc)
# Prefer shared linkage but use static if shared build is deactivated
if (ARROW_BUILD_SHARED)
  target_link_libraries(parquet-arrow-example parquet_shared)
else()
  target_link_libraries(parquet-arrow-example parquet_static)
endif()

add_dependencies(parquet
  parquet-low-level-example
  parquet-low-level-example2
  parquet-with-index-page-skipping
  parquet-arrow-example)
