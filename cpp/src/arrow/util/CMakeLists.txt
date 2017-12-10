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
install(FILES
  bit-stream-utils.h
  bit-util.h
  bpacking.h
  compiler-util.h
  compression_brotli.h
  compression.h
  compression_lz4.h
  compression_snappy.h
  compression_zlib.h
  compression_zstd.h
  cpu-info.h
  decimal.h
  hash-util.h
  hash.h
  key_value_metadata.h
  logging.h
  macros.h
  parallel.h
  rle-encoding.h
  sse-util.h
  stl.h
  type_traits.h
  variant.h
  visibility.h
  DESTINATION include/arrow/util)

#######################################
# arrow_test_main
#######################################

if (ARROW_BUILD_BENCHMARKS)
  add_library(arrow_benchmark_main benchmark_main.cc)
  if (APPLE)
    target_link_libraries(arrow_benchmark_main
      benchmark
    )
  else()
	  target_link_libraries(arrow_benchmark_main
      benchmark
      pthread
	  )
  endif()

  # TODO(wesm): Some benchmarks include gtest.h
  add_dependencies(arrow_benchmark_main gtest)
endif()

ADD_ARROW_TEST(bit-util-test)
ADD_ARROW_TEST(compression-test)
ADD_ARROW_TEST(decimal-test)
ADD_ARROW_TEST(key-value-metadata-test)
ADD_ARROW_TEST(rle-encoding-test)
ADD_ARROW_TEST(stl-util-test)

add_subdirectory(variant)
