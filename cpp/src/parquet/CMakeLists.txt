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

# Headers: top level
install(FILES
  encoding.h
  exception.h
  schema.h
  types.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/parquet")

configure_file(parquet_version.h.in
  "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h"
  @ONLY)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/parquet_version.h"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/parquet")

# pkg-config support
configure_file(parquet.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/parquet.pc"
  @ONLY)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/parquet.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

ADD_PARQUET_TEST(encoding-test)
ADD_PARQUET_TEST(public-api-test)
ADD_PARQUET_TEST(types-test)
ADD_PARQUET_TEST(reader-test)
ADD_PARQUET_TEST(schema-test)
ADD_PARQUET_BENCHMARK(encoding-benchmark)
