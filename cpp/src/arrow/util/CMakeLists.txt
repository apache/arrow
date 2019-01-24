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

#######################################
# arrow_util
#######################################

# Headers: top level
ARROW_INSTALL_ALL_HEADERS("arrow/util")

#######################################
# arrow_test_main
#######################################

if (ARROW_BUILD_BENCHMARKS)
  add_library(arrow_benchmark_main benchmark_main.cc)
  if (APPLE)
    target_link_libraries(arrow_benchmark_main
      gbenchmark_static
    )
  elseif(MSVC)
    target_link_libraries(arrow_benchmark_main
      gbenchmark_static
      Shlwapi.lib
  )
  else()
    target_link_libraries(arrow_benchmark_main
      gbenchmark_static
      pthread
    )
  endif()

  # TODO(wesm): Some benchmarks include gtest.h
  add_dependencies(arrow_benchmark_main ${GTEST_LIBRARY})
endif()

ADD_ARROW_TEST(bit-util-test)
ADD_ARROW_TEST(checked-cast-test)
ADD_ARROW_TEST(compression-test)
ADD_ARROW_TEST(decimal-test)
ADD_ARROW_TEST(hashing-test)
ADD_ARROW_TEST(int-util-test)
ADD_ARROW_TEST(key-value-metadata-test)
ADD_ARROW_TEST(lazy-test)
ADD_ARROW_TEST(logging-test)
ADD_ARROW_TEST(parsing-util-test)
ADD_ARROW_TEST(rle-encoding-test)
ADD_ARROW_TEST(stl-util-test)
ADD_ARROW_TEST(task-group-test)
ADD_ARROW_TEST(thread-pool-test)
ADD_ARROW_TEST(trie-test)
ADD_ARROW_TEST(utf8-util-test)

ADD_ARROW_BENCHMARK(bit-util-benchmark)
ADD_ARROW_BENCHMARK(compression-benchmark)
ADD_ARROW_BENCHMARK(decimal-benchmark)
ADD_ARROW_BENCHMARK(hashing-benchmark)
ADD_ARROW_BENCHMARK(int-util-benchmark)
ADD_ARROW_BENCHMARK(lazy-benchmark)
ADD_ARROW_BENCHMARK(machine-benchmark)
ADD_ARROW_BENCHMARK(number-parsing-benchmark)
ADD_ARROW_BENCHMARK(thread-pool-benchmark)
ADD_ARROW_BENCHMARK(trie-benchmark)
ADD_ARROW_BENCHMARK(utf8-util-benchmark)
