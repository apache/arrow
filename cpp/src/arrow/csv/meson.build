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

csv_test_sources = [
    'chunker_test.cc',
    'column_builder_test.cc',
    'column_decoder_test.cc',
    'converter_test.cc',
    'parser_test.cc',
    'reader_test.cc',
    'writer_test.cc',
]

exc = executable(
    'arrow-csv-test',
    sources: csv_test_sources,
    dependencies: [arrow_test_dep],
)
test('arrow-csv-test', exc)

csv_benchmarks = ['converter_benchmark', 'parser_benchmark', 'writer_benchmark']

foreach csv_benchmark : csv_benchmarks
    benchmark_name = 'arrow-csv-@0@'.format(csv_benchmark.replace('_', '-'))
    exc = executable(
        benchmark_name,
        sources: '@0@.cc'.format(csv_benchmark),
        dependencies: [arrow_benchmark_dep],
    )
    benchmark(benchmark_name, exc)
endforeach

install_headers(
    [
        'api.h',
        'chunker.h',
        'column_builder.h',
        'column_decoder.h',
        'converter.h',
        'invalid_row.h',
        'options.h',
        'parser.h',
        'reader.h',
        'test_common.h',
        'type_fwd.h',
        'writer.h',
    ],
    subdir: 'arrow/csv',
)

arrow_csv_dep = declare_dependency(
    include_directories: include_directories('.'),
    dependencies: arrow_dep,
)
meson.override_dependency('arrow-csv', arrow_csv_dep)

pkg.generate(
    filebase: 'arrow-csv',
    name: 'Apache Arrow CSV',
    description: 'CSV reader module for Apache Arrow',
    requires: 'arrow',
)
