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
    'arrow-feather-test',
    sources: ['feather_test.cc'],
    dependencies: [arrow_ipc_deps, arrow_test_dep],
)
test('arrow-feather-test', exc)

ipc_tests = ['message_internal_test', 'read_write_test', 'tensor_test']

foreach ipc_test : ipc_tests
    test_name = 'arrow-ipc-@0@'.format(ipc_test.replace('_', '-'))
    exc = executable(
        test_name,
        sources: ['@0@.cc'.format(ipc_test)],
        dependencies: [arrow_ipc_deps, arrow_test_dep],
    )
    test(test_name, exc)
endforeach

install_headers(
    [
        'api.h',
        'dictionary.h',
        'feather.h',
        'message.h',
        'options.h',
        'reader.h',
        'test_common.h',
        'type_fwd.h',
        'util.h',
        'writer.h',
    ],
    subdir: 'arrow/ipc',
)

if needs_utilities or needs_integration
    file_to_stream_exc = executable(
        'arrow-file-to-stream',
        sources: ['file_to_stream.cc'],
        dependencies: [arrow_dep],
        install: needs_utilities,
    )

    stream_to_file_exc = executable(
        'arrow-stream-to-file',
        sources: ['stream_to_file.cc'],
        dependencies: [arrow_dep],
        install: needs_utilities,
    )
endif

exc = executable(
    'arrow-ipc-read-write-benchmark',
    sources: ['read_write_benchmark.cc'],
    dependencies: [arrow_benchmark_dep],
)
benchmark('arrow-ipc-read-write-benchmark', exc)

if needs_fuzzing or (needs_utilities
    and needs_testing
    and needs_lz4
    and needs_zstd
)
    fuzz_corpus_exc = executable(
        'arrow-ipc-generate-fuzz-corpus',
        sources: ['generate_fuzz_corpus.cc'],
        dependencies: [arrow_test_dep_no_main],
    )

    tensor_fuzz_corpus_exc = executable(
        'arrow-ipc-generate-tensor-fuzz-corpus',
        sources: ['generate_tensor_fuzz_corpus.cc'],
        dependencies: [arrow_test_dep_no_main],
    )
endif

ipc_fuzz_targets = ['file_fuzz', 'stream_fuzz', 'tensor_stream_fuzz']

if needs_fuzzing
    if meson.version() < '1.8.0'
        error(
            ' Meson >= 1.8.0 is required for fuzzing support, found @0@'.format(
                meson.version(),
            ),
        )
    endif

    foreach ipc_fuzz_target : ipc_fuzz_targets
        target_name = 'arrow-ipc-@0@'.format(ipc_fuzz_target.replace('_', '-'))
        executable(
            target_name,
            sources: ['@0@.cc'.format(ipc_fuzz_target)],
            dependencies: [arrow_dep],
            override_options: ['-Db_sanitize=fuzzer'],
        )
    endforeach
endif
