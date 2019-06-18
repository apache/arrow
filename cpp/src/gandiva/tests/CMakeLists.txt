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

add_gandiva_test(filter_test)
add_gandiva_test(projector_test)
add_gandiva_test(projector_build_validation_test)
add_gandiva_test(if_expr_test)
add_gandiva_test(literal_test)
add_gandiva_test(boolean_expr_test)
add_gandiva_test(binary_test)
add_gandiva_test(date_time_test)
add_gandiva_test(to_string_test)
add_gandiva_test(utf8_test)
add_gandiva_test(hash_test)
add_gandiva_test(in_expr_test)
add_gandiva_test(null_validity_test)
add_gandiva_test(decimal_test)
add_gandiva_test(decimal_single_test)

add_gandiva_test(projector_test_static SOURCES projector_test.cc USE_STATIC_LINKING)

add_arrow_benchmark(micro_benchmarks
                    PREFIX
                    "gandiva"
                    EXTRA_LINK_LIBS
                    gandiva_static)
