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

tests = {
    'arrow-io-buffered-test': {'sources': ['buffered_test.cc']},
    'arrow-io-compressed-test': {'sources': ['compressed_test.cc']},
    'arrow-io-file-test': {'sources': ['file_test.cc']},
    'arrow-io-memory-test': {'sources': ['memory_test.cc']},
}

# if needs_hdfs
#   ...
#

foreach key, val : tests
    exc = executable(
        key,
        sources: val['sources'],
        dependencies: [arrow_test_dep],
        implicit_include_directories: false,  # stdio.h conflicts with stdlib
    )
    test(key, exc)
endforeach

# TODO: benchmarks
# TODO: arrow_install_all_headers
