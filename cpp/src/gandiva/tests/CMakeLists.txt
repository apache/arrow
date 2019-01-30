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

ADD_GANDIVA_TEST(filter_test)
ADD_GANDIVA_TEST(projector_test)
ADD_GANDIVA_TEST(projector_build_validation_test)
ADD_GANDIVA_TEST(if_expr_test)
ADD_GANDIVA_TEST(literal_test)
ADD_GANDIVA_TEST(boolean_expr_test)
ADD_GANDIVA_TEST(binary_test)
ADD_GANDIVA_TEST(date_time_test)
ADD_GANDIVA_TEST(to_string_test)
ADD_GANDIVA_TEST(hash_test)
ADD_GANDIVA_TEST(in_expr_test)
ADD_GANDIVA_TEST(null_validity_test)
ADD_GANDIVA_TEST(decimal_test)
ADD_GANDIVA_TEST(decimal_single_test)

ADD_GANDIVA_TEST(projector_test_static
  SOURCES projector_test.cc
  USE_STATIC_LINKING)

ADD_ARROW_BENCHMARK(micro_benchmarks
  PREFIX "gandiva"
  EXTRA_LINK_LIBS gandiva_static)
