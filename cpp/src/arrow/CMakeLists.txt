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
  allocator.h
  api.h
  array.h
  buffer.h
  builder.h
  compare.h
  loader.h
  memory_pool.h
  pretty_print.h
  status.h
  table.h
  tensor.h
  type.h
  type_fwd.h
  type_traits.h
  test-util.h
  visitor.h
  visitor_inline.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow")

# pkg-config support
configure_file(arrow.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

#######################################
# Unit tests
#######################################

ADD_ARROW_TEST(allocator-test)
ADD_ARROW_TEST(array-test)
ADD_ARROW_TEST(array-decimal-test)
ADD_ARROW_TEST(buffer-test)
ADD_ARROW_TEST(memory_pool-test)
ADD_ARROW_TEST(pretty_print-test)
ADD_ARROW_TEST(status-test)
ADD_ARROW_TEST(type-test)
ADD_ARROW_TEST(table-test)
ADD_ARROW_TEST(tensor-test)

ADD_ARROW_BENCHMARK(builder-benchmark)
ADD_ARROW_BENCHMARK(column-benchmark)
