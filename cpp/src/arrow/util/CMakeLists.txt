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

#
# arrow_util
#

# Headers: top level
arrow_install_all_headers("arrow/util")

#
# arrow_test_main
#

if(ARROW_BUILD_BENCHMARKS)
  add_library(arrow_benchmark_main benchmark_main.cc)
  if(APPLE)
    target_link_libraries(arrow_benchmark_main gbenchmark_static)
  elseif(MSVC)
    target_link_libraries(arrow_benchmark_main gbenchmark_static Shlwapi.lib)
  else()
    target_link_libraries(arrow_benchmark_main gbenchmark_static pthread)
  endif()

  # TODO(wesm): Some benchmarks include gtest.h
  add_dependencies(arrow_benchmark_main ${GTEST_LIBRARY})
endif()

add_arrow_test(bit-util-test)
add_arrow_test(checked-cast-test)
add_arrow_test(compression-test)
add_arrow_test(decimal-test)
add_arrow_test(hashing-test)
add_arrow_test(int-util-test)
add_arrow_test(key-value-metadata-test)
add_arrow_test(lazy-test)
add_arrow_test(logging-test)
add_arrow_test(parsing-util-test)
add_arrow_test(rle-encoding-test)
add_arrow_test(stl-util-test)
add_arrow_test(task-group-test)
add_arrow_test(thread-pool-test)
add_arrow_test(trie-test)
add_arrow_test(utf8-util-test)

add_arrow_benchmark(bit-util-benchmark)
add_arrow_benchmark(compression-benchmark)
add_arrow_benchmark(decimal-benchmark)
add_arrow_benchmark(hashing-benchmark)
add_arrow_benchmark(int-util-benchmark)
add_arrow_benchmark(lazy-benchmark)
add_arrow_benchmark(machine-benchmark)
add_arrow_benchmark(number-parsing-benchmark)
add_arrow_benchmark(thread-pool-benchmark)
add_arrow_benchmark(trie-benchmark)
add_arrow_benchmark(utf8-util-benchmark)
