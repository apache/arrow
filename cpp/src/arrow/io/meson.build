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

io_tests = ['buffered_test', 'compressed_test', 'file_test', 'memory_test']

foreach io_test : io_tests
    test_name = 'arrow-io-@0@'.format(io_test.replace('_', '-'))
    exc = executable(
        test_name,
        sources: '@0@.cc'.format(io_test),
        dependencies: [arrow_test_dep],
        implicit_include_directories: false,
    )
    test(test_name, exc)
endforeach

if needs_hdfs
    hdfs_incdir = '../../../thirdparty/hadoop/include'
    exc = executable(
        'arrow-io-hdfs-test',
        sources: 'hdfs_test.cc',
        dependencies: [arrow_test_dep, filesystem_dep],
        implicit_include_directories: false,
        include_directories: [hdfs_incdir],
    )
    test('arrow-io-hdfs-test', exc)
endif

io_benchmarks = ['file_benchmark', 'compressed_benchmark']

foreach io_benchmark : io_benchmarks
    benchmark_name = 'arrow-io-@0@'.format(io_benchmark.replace('_', '-'))
    exc = executable(
        benchmark_name,
        sources: '@0@.cc'.format(io_benchmark),
        dependencies: [arrow_benchmark_dep],
        implicit_include_directories: false,
    )
    benchmark(benchmark_name, exc)
endforeach

# TODO: Add memory benchmark with SIMD. See GH-45823

install_headers(
    [
        'api.h',
        'buffered.h',
        'caching.h',
        'compressed.h',
        'concurrency.h',
        'file.h',
        'hdfs.h',
        'interfaces.h',
        'memory.h',
        'mman.h',
        'slow.h',
        'stdio.h',
        'transform.h',
        'type_fwd.h',
    ],
    subdir: 'arrow/io',
)
