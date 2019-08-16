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

add_arrow_test(utility-test
               SOURCES
               align_util_test.cc
               checked_cast_test.cc
               key_value_metadata_test.cc
               hashing_test.cc
               int_util_test.cc
               io_util_test.cc
               parsing_util_test.cc
               range_test.cc
               stl_util_test.cc
               trie_test.cc
               utf8_util_test.cc)

add_arrow_test(bit_util_test)
add_arrow_test(compression_test)
add_arrow_test(decimal_test)
add_arrow_test(logging_test)
add_arrow_test(rle_encoding_test)
add_arrow_test(task_group_test)
add_arrow_test(thread_pool_test)
if(ARROW_WITH_URIPARSER)
  add_arrow_test(uri_test)
endif()

add_arrow_benchmark(bit_util_benchmark)
add_arrow_benchmark(compression_benchmark)
add_arrow_benchmark(decimal_benchmark)
add_arrow_benchmark(hashing_benchmark)
add_arrow_benchmark(int_util_benchmark)
add_arrow_benchmark(machine_benchmark)
add_arrow_benchmark(number_parsing_benchmark)
add_arrow_benchmark(range_benchmark)
add_arrow_benchmark(thread_pool_benchmark)
add_arrow_benchmark(trie_benchmark)
add_arrow_benchmark(utf8_util_benchmark)
