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

if(WIN32)
  # This manifest enables long file paths on Windows 10+
  # See https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file#enable-long-paths-in-windows-10-version-1607-and-later
  if(MSVC)
    set(IO_UTIL_TEST_SOURCES io_util_test.cc io_util_test.manifest)
  else()
    set(IO_UTIL_TEST_SOURCES io_util_test.cc io_util_test.rc)
  endif()
else()
  set(IO_UTIL_TEST_SOURCES io_util_test.cc)
endif()

add_arrow_test(utility-test
               SOURCES
               align_util_test.cc
               async_generator_test.cc
               async_util_test.cc
               bit_block_counter_test.cc
               bit_util_test.cc
               byte_size_test.cc
               cache_test.cc
               checked_cast_test.cc
               compression_test.cc
               decimal_test.cc
               formatting_util_test.cc
               key_value_metadata_test.cc
               hashing_test.cc
               int_util_test.cc
               ${IO_UTIL_TEST_SOURCES}
               iterator_test.cc
               logging_test.cc
               queue_test.cc
               range_test.cc
               reflection_test.cc
               rle_encoding_test.cc
               rle_util_test.cc
               small_vector_test.cc
               stl_util_test.cc
               string_test.cc
               tdigest_test.cc
               test_common.cc
               time_test.cc
               tracing_test.cc
               trie_test.cc
               uri_test.cc
               utf8_util_test.cc
               value_parsing_test.cc)

add_arrow_test(threading-utility-test
               SOURCES
               cancel_test.cc
               counting_semaphore_test.cc
               future_test.cc
               task_group_test.cc
               test_common.cc
               thread_pool_test.cc)

add_arrow_benchmark(bit_block_counter_benchmark)
add_arrow_benchmark(bit_util_benchmark)
add_arrow_benchmark(bitmap_reader_benchmark)
add_arrow_benchmark(cache_benchmark)
add_arrow_benchmark(compression_benchmark)
add_arrow_benchmark(decimal_benchmark)
add_arrow_benchmark(hashing_benchmark)
add_arrow_benchmark(int_util_benchmark)
add_arrow_benchmark(machine_benchmark)
add_arrow_benchmark(queue_benchmark)
add_arrow_benchmark(range_benchmark)
add_arrow_benchmark(small_vector_benchmark)
add_arrow_benchmark(tdigest_benchmark)
add_arrow_benchmark(thread_pool_benchmark)
add_arrow_benchmark(trie_benchmark)
add_arrow_benchmark(utf8_util_benchmark)
add_arrow_benchmark(value_parsing_benchmark)
