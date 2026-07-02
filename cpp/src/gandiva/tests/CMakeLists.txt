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

add_gandiva_test(projector-test
                 SOURCES
                 binary_test.cc
                 boolean_expr_test.cc
                 date_time_test.cc
                 decimal_alignment_test.cc
                 decimal_single_test.cc
                 decimal_test.cc
                 filter_project_test.cc
                 filter_test.cc
                 hash_test.cc
                 huge_table_test.cc
                 if_expr_test.cc
                 in_expr_test.cc
                 literal_test.cc
                 null_validity_test.cc
                 projector_build_validation_test.cc
                 projector_test.cc
                 test_util.cc
                 to_string_test.cc
                 utf8_test.cc)

if(ARROW_BUILD_STATIC)
  add_gandiva_test(projector_test_static
                   SOURCES
                   projector_test.cc
                   test_util.cc
                   USE_STATIC_LINKING)
  add_arrow_benchmark(micro_benchmarks
                      SOURCES
                      micro_benchmarks.cc
                      test_util.cc
                      PREFIX
                      "gandiva"
                      EXTRA_LINK_LIBS
                      gandiva_static)

  # Calls the precompiled REPLACE functions directly, so it compiles
  # string_ops.cc/context_helper.cc with GANDIVA_UNIT_TEST=1 (which exposes them
  # as linkable symbols). Only built when ARROW_BUILD_BENCHMARKS is ON.
  add_arrow_benchmark(string_ops_benchmark
                      SOURCES
                      string_ops_benchmark.cc
                      ../precompiled/string_ops.cc
                      ../context_helper.cc
                      PREFIX
                      "gandiva"
                      EXTRA_LINK_LIBS
                      gandiva_static)
  if(TARGET gandiva-string-ops-benchmark)
    target_compile_definitions(gandiva-string-ops-benchmark
                               PRIVATE GANDIVA_UNIT_TEST=1 ARROW_STATIC GANDIVA_STATIC)
    target_include_directories(gandiva-string-ops-benchmark SYSTEM
                               PRIVATE ${CMAKE_SOURCE_DIR}/src)
  endif()
endif()

add_subdirectory(external_functions)
