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

arrow_install_all_headers("arrow/compute/exec")

add_arrow_compute_test(subtree_test
                       REQUIRE_ALL_KERNELS
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       subtree_test.cc)

add_arrow_compute_test(plan_test
                       REQUIRE_ALL_KERNELS
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       plan_test.cc
                       test_nodes_test.cc
                       test_nodes.cc)
add_arrow_compute_test(fetch_node_test
                       REQUIRE_ALL_KERNELS
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       fetch_node_test.cc
                       test_nodes.cc)
add_arrow_compute_test(hash_join_node_test
                       REQUIRE_ALL_KERNELS
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       hash_join_node_test.cc
                       bloom_filter_test.cc)
add_arrow_compute_test(pivot_longer_node_test
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       pivot_longer_node_test.cc
                       test_nodes.cc)
add_arrow_compute_test(asof_join_node_test
                       REQUIRE_ALL_KERNELS
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       asof_join_node_test.cc
                       test_nodes.cc)
add_arrow_compute_test(tpch_node_test PREFIX "arrow-compute")
add_arrow_compute_test(union_node_test PREFIX "arrow-compute")
add_arrow_compute_test(groupby_test REQUIRE_ALL_KERNELS PREFIX "arrow-compute")
add_arrow_compute_test(util_test
                       PREFIX
                       "arrow-compute"
                       SOURCES
                       util_test.cc
                       task_util_test.cc)

add_arrow_benchmark(expression_benchmark PREFIX "arrow-compute")

add_arrow_benchmark(filter_benchmark
                    PREFIX
                    "arrow-compute"
                    SOURCES
                    benchmark_util.cc
                    filter_benchmark.cc)

add_arrow_benchmark(project_benchmark
                    PREFIX
                    "arrow-compute"
                    SOURCES
                    benchmark_util.cc
                    project_benchmark.cc)

add_arrow_benchmark(asof_join_benchmark PREFIX "arrow-compute")

add_arrow_benchmark(tpch_benchmark PREFIX "arrow-compute")

if(ARROW_BUILD_OPENMP_BENCHMARKS)
  find_package(OpenMP REQUIRED)
  add_arrow_benchmark(hash_join_benchmark
                      PREFIX
                      "arrow-compute"
                      EXTRA_LINK_LIBS
                      OpenMP::OpenMP_CXX)
  if(MSVC)
    target_compile_options(arrow-compute-hash-join-benchmark
                           PRIVATE "-openmp:experimental -openmp:llvm")
  endif()
endif()
