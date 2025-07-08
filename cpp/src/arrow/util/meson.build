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

conf_data = configuration_data()

conf_data.set('ARROW_VERSION_MAJOR', version_major)
conf_data.set('ARROW_VERSION_MINOR', version_minor)
conf_data.set('ARROW_VERSION_PATCH', version_patch)

conf_data.set('ARROW_VERSION', arrow_version)

conf_data.set('ARROW_SO_VERSION', arrow_so_version)
conf_data.set('ARROW_FULL_SO_VERSION', arrow_full_so_version)

conf_data.set('CMAKE_CXX_COMPILER_ID', cpp_compiler.get_id())
conf_data.set('CMAKE_CXX_COMPILER_VERSION', cpp_compiler.version())
conf_data.set(
    'CMAKE_CXX_FLAGS',
    ' '.join(cpp_compiler.get_supported_arguments()),
)

conf_data.set('UPPERCASE_BUILD_TYPE', get_option('buildtype').to_upper())

conf_data.set('ARROW_PACKAGE_KIND', get_option('package_kind'))

conf_data.set('ARROW_COMPUTE', needs_compute)
conf_data.set('ARROW_CSV', false)
conf_data.set('ARROW_CUDA', false)
conf_data.set('ARROW_DATASET', false)
conf_data.set('ARROW_FILESYSTEM', false)
conf_data.set('ARROW_FLIGHT', false)
conf_data.set('ARROW_FLIGHT_SQL', false)
conf_data.set('ARROW_IPC', needs_ipc)
conf_data.set('ARROW_JEMALLOC', false)
conf_data.set('ARROW_JEMALLOC_VENDORED', false)
conf_data.set('ARROW_JSON', needs_json)
conf_data.set('ARROW_MIMALLOC', false)
conf_data.set('ARROW_ORC', false)
conf_data.set('ARROW_PARQUET', false)
conf_data.set('ARROW_SUBSTRAIT', false)
conf_data.set('ARROW_AZURE', false)
conf_data.set('ARROW_ENABLE_THREADING', true)
conf_data.set('ARROW_GCS', false)
conf_data.set('ARROW_HDFS', false)
conf_data.set('ARROW_S3', false)
conf_data.set('ARROW_USE_GLOG', false)

has_int128 = cpp_compiler.has_define('__SIZEOF_INT128__')
conf_data.set('ARROW_USE_NATIVE_INT128', has_int128)

conf_data.set('ARROW_WITH_BROTLI', needs_brotli)
conf_data.set('ARROW_WITH_BZ2', needs_bz2)
conf_data.set('ARROW_WITH_LZ4', needs_lz4)
conf_data.set('ARROW_WITH_MUSL', false)
conf_data.set('ARROW_WITH_OPENTELEMETRY', false)
conf_data.set('ARROW_WITH_RE2', false)
conf_data.set('ARROW_WITH_SNAPPY', needs_snappy)
conf_data.set('ARROW_WITH_UCX', false)
conf_data.set('ARROW_WITH_UTF8PROC', false)
conf_data.set('ARROW_WITH_ZLIB', needs_zlib)
conf_data.set('ARROW_WITH_ZSTD', needs_zstd)
conf_data.set('PARQUET_REQUIRE_ENCRYPTION', false)

configure_file(
    input: 'config.h.cmake',
    output: 'config.h',
    configuration: conf_data,
    format: 'cmake@',
    install: true,
    install_dir: 'arrow/util',
)

internal_conf_data = configuration_data()

internal_conf_data.set('ARROW_GIT_ID', git_id)
internal_conf_data.set('ARROW_GIT_DESCRIPTION', git_description)

configure_file(
    input: 'config_internal.h.cmake',
    output: 'config_internal.h',
    configuration: internal_conf_data,
    format: 'cmake@',
)

install_headers(
    [
        'algorithm.h',
        'aligned_storage.h',
        'align_util.h',
        'async_generator_fwd.h',
        'async_generator.h',
        'async_util.h',
        'base64.h',
        'basic_decimal.h',
        'benchmark_util.h',
        'binary_view_util.h',
        'bit_block_counter.h',
        'bitmap_builders.h',
        'bitmap_generate.h',
        'bitmap.h',
        'bitmap_ops.h',
        'bitmap_reader.h',
        'bitmap_visit.h',
        'bitmap_writer.h',
        'bit_run_reader.h',
        'bit_util.h',
        'byte_size.h',
        'cancel.h',
        'checked_cast.h',
        'compare.h',
        'compression.h',
        'concurrent_map.h',
        'converter.h',
        'cpu_info.h',
        'crc32.h',
        'debug.h',
        'decimal.h',
        'delimiting.h',
        'endian.h',
        'float16.h',
        'formatting.h',
        'functional.h',
        'future.h',
        'hashing.h',
        'hash_util.h',
        'int_util.h',
        'int_util_overflow.h',
        'io_util.h',
        'iterator.h',
        'key_value_metadata.h',
        'launder.h',
        'list_util.h',
        'logger.h',
        'logging.h',
        'macros.h',
        'math_constants.h',
        'mutex.h',
        'parallel.h',
        'pcg_random.h',
        'prefetch.h',
        'queue.h',
        'range.h',
        'ree_util.h',
        'regex.h',
        'rows_to_batches.h',
        'simd.h',
        'small_vector.h',
        'span.h',
        'string_util.h',
        'string.h',
        'task_group.h',
        'test_common.h',
        'thread_pool.h',
        'time.h',
        'tracing.h',
        'type_fwd.h',
        'type_traits.h',
        'ubsan.h',
        'union_util.h',
        'unreachable.h',
        'uri.h',
        'utf8.h',
        'value_parsing.h',
        'vector.h',
        'visibility.h',
        'windows_compatibility.h',
        'windows_fixup.h',
    ],
    subdir: 'arrow/util',
)

utility_test_srcs = [
    'align_util_test.cc',
    'atfork_test.cc',
    'byte_size_test.cc',
    'byte_stream_split_test.cc',
    'cache_test.cc',
    'checked_cast_test.cc',
    'compression_test.cc',
    'decimal_test.cc',
    'float16_test.cc',
    'fixed_width_test.cc',
    'formatting_util_test.cc',
    'key_value_metadata_test.cc',
    'hashing_test.cc',
    'int_util_test.cc',
    'io_util_test.cc',
    'iterator_test.cc',
    'list_util_test.cc',
    'logger_test.cc',
    'logging_test.cc',
    'math_test.cc',
    'queue_test.cc',
    'range_test.cc',
    'ree_util_test.cc',
    'reflection_test.cc',
    'rows_to_batches_test.cc',
    'small_vector_test.cc',
    'span_test.cc',
    'stl_util_test.cc',
    'string_test.cc',
    'tdigest_test.cc',
    'test_common.cc',
    'time_test.cc',
    'tracing_test.cc',
    'trie_test.cc',
    'uri_test.cc',
    'utf8_util_test.cc',
    'value_parsing_test.cc',
]

if host_machine.system() == 'windows'
    # This manifest enables long file paths on Windows 10+
    # See https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file#enable-long-paths-in-windows-10-version-1607-and-later
    if cpp_compiler.get_id() == 'msvc'
        utility_test_srcs += ['io_util_test.manifest']
    else
        utility_test_srcs += ['io_util_test.rc']
    endif
endif

exc = executable(
    'arrow-utility-test',
    sources: utility_test_srcs,
    dependencies: [arrow_dep, filesystem_dep, gtest_dep, gmock_dep],
    link_with: [arrow_test_lib],
    implicit_include_directories: false,
)
test('arrow-utility-test', exc)

util_tests = {
    'arrow-async-utility-test': {
        'sources': [
            'async_generator_test.cc',
            'async_util_test.cc',
            'test_common.cc',
        ],
    },
    'arrow-bit-utility-test': {
        'sources': [
            'bit_block_counter_test.cc',
            'bit_util_test.cc',
            'rle_encoding_test.cc',
        ],
    },
    'arrow-threading-utility-test': {
        'sources': [
            'cancel_test.cc',
            'counting_semaphore_test.cc',
            'future_test.cc',
            'task_group_test.cc',
            'test_common.cc',
            'thread_pool_test.cc',
        ],
    },
    'arrow-crc32-test': {'sources': ['crc32_test.cc']},
}

foreach key, val : util_tests
    exc = executable(
        key,
        sources: val['sources'],
        dependencies: [arrow_test_dep],
        implicit_include_directories: false,
    )
    test(key, exc)
endforeach

util_benchmarks = [
    'bit_block_counter',
    'bit_util',
    'bitmap_reader',
    'cache',
    'compression',
    'decimal',
    'hashing',
    'int_util',
    'machine',
    'queue',
    'range',
    'small_vector',
    'tdigest',
    'thread_pool',
    'trie',
]

foreach util_benchmark : util_benchmarks
    benchmark_name = '@0@-benchmark'.format(util_benchmark.replace('_', '-'))
    exc = executable(
        benchmark_name,
        sources: '@0@_benchmark.cc'.format(util_benchmark),
        dependencies: [arrow_benchmark_dep],
        implicit_include_directories: false,
    )
    benchmark(benchmark_name, exc)
endforeach

# TODO: XSimd benchmark. See GH-45823
