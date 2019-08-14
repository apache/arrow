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

arrow_install_all_headers("arrow/testing")

if(DEFINED $ENV{ARROW_TEST_DATA})
  set(ARROW_TEST_DATA $ENV{ARROW_TEST_DATA})
else()
  set(ARROW_TEST_DATA ${CMAKE_SOURCE_DIR}/../testing)
endif()

if(DEFINED $ENV{PARQUET_TEST_DATA})
  set(PARQUET_TEST_DATA $ENV{PARQUET_TEST_DATA})
else()
  set(PARQUET_TEST_DATA ${CMAKE_SOURCE_DIR}/submodules/parquet-testing)
endif()

configure_file(test_data.h.in "${CMAKE_CURRENT_BINARY_DIR}/test_data.h" @ONLY)
