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

add_library(parquet_compression STATIC
  codec.cc
  snappy-codec.cc
  gzip-codec.cc
)
target_link_libraries(parquet_compression
  snappystatic
  zlibstatic)

set_target_properties(parquet_compression
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# Headers: compression
install(FILES
  codec.h
  DESTINATION include/parquet/compression)

ADD_PARQUET_TEST(codec-test)
