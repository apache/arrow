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

exc = executable(
    'arrow-json-test',
    sources: [
        'chunked_builder_test.cc',
        'chunker_test.cc',
        'converter_test.cc',
        'from_string_test.cc',
        'parser_test.cc',
        'reader_test.cc',
    ],
    dependencies: [arrow_test_dep, rapidjson_dep],
)
test('arrow-json-test', exc)

exc = executable(
    'arrow-json-parser-benchmark',
    sources: ['parser_benchmark.cc'],
    dependencies: [arrow_benchmark_dep, rapidjson_dep],
)
benchmark('arrow-json-parser-benchmark', exc)

install_headers(
    [
        'api.h',
        'chunked_builder.h',
        'chunker.h',
        'converter.h',
        'from_string.h',
        'object_parser.h',
        'object_writer.h',
        'options.h',
        'parser.h',
        'rapidjson_defs.h',
        'reader.h',
        'test_common.h',
        'type_fwd.h',
    ],
    subdir: 'arrow/json',
)

arrow_json_dep = declare_dependency(
    include_directories: include_directories('.'),
    dependencies: arrow_dep,
)
meson.override_dependency('arrow-json', arrow_json_dep)

pkg.generate(
    filebase: 'arrow-json',
    name: 'Apache Arrow JSON',
    description: 'JSON reader module for Apache Arrow',
    requires: ['arrow'],
)
