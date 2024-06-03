# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# CMake allows you to generate a configuration file from outside
# of the target directory. By contrast, Meson has a limitation that
# all outputs go to the build directory corresponding the source
# tree: https://github.com/mesonbuild/meson/issues/2320#issuecomment-466171587
# As such, we need to use our subdir('util') to generate the configuration
# file, but all other tests that util CMakeLists.txt generate need to be
# declared in this top level build coonfiguration
subdir('util')

arrow_lib_dependencies = []
root_dir = include_directories('..')
hadoop_dir = include_directories('../../thirdparty/hadoop/include')
flatbuf_dir = include_directories('../../thirdparty/flatbuffers/include')

needs_integration = get_option('tests') # or get_option('integration')
needs_ipc = get_option('ipc') or get_option('tests')
needs_testing = get_option('tests') # or get_option('testing')
needs_json = needs_testing # or get_option('json')

arrow_srcs = [
    'builder.cc',
    'buffer.cc',
    'chunked_array.cc',
    'chunk_resolver.cc',
    'compare.cc',
    'config.cc',
    'datum.cc',
    'device.cc',
    'extension_type.cc',
    'pretty_print.cc',
    'record_batch.cc',
    'result.cc',
    'scalar.cc',
    'sparse_tensor.cc',
    'status.cc',
    'table.cc',
    'table_builder.cc',
    'tensor.cc',
    'tensor/coo_converter.cc',
    'tensor/csf_converter.cc',
    'tensor/csx_converter.cc',
    'type.cc',
    'type_traits.cc',
    'visitor.cc',
    'c/bridge.cc',
    'c/dlpack.cc',
]

arrow_array_srcs = [
    'array/array_base.cc',
    'array/array_binary.cc',
    'array/array_decimal.cc',
    'array/array_dict.cc',
    'array/array_nested.cc',
    'array/array_primitive.cc',
    'array/array_run_end.cc',
    'array/builder_adaptive.cc',
    'array/builder_base.cc',
    'array/builder_binary.cc',
    'array/builder_decimal.cc',
    'array/builder_dict.cc',
    'array/builder_run_end.cc',
    'array/builder_nested.cc',
    'array/builder_primitive.cc',
    'array/builder_union.cc',
    'array/concatenate.cc',
    'array/data.cc',
    'array/diff.cc',
    'array/util.cc',
    'array/validate.cc',
]

arrow_io_srcs = [
    'io/buffered.cc',
    'io/caching.cc',
    'io/compressed.cc',
    'io/file.cc',
    'io/hdfs.cc',
    'io/hdfs_internal.cc',
    'io/interfaces.cc',
    'io/memory.cc',
    'io/slow.cc',
    'io/stdio.cc',
    'io/transform.cc',
]

arrow_memory_pool_srcs = [
    'memory_pool.cc',
]

arrow_vendored_srcs = [
    'vendored/base64.cpp',
    'vendored/datetime/tz.cpp',
    'vendored/double-conversion/bignum-dtoa.cc',
    'vendored/double-conversion/bignum.cc',
    'vendored/double-conversion/cached-powers.cc',
    'vendored/double-conversion/double-to-string.cc',
    'vendored/double-conversion/fast-dtoa.cc',
    'vendored/double-conversion/fixed-dtoa.cc',
    'vendored/double-conversion/string-to-double.cc',
    'vendored/double-conversion/strtod.cc',
    'vendored/musl/strptime.c',
    'vendored/uriparser/UriCommon.c',
    'vendored/uriparser/UriCompare.c',
    'vendored/uriparser/UriEscape.c',
    'vendored/uriparser/UriFile.c',
    'vendored/uriparser/UriIp4.c',
    'vendored/uriparser/UriIp4Base.c',
    'vendored/uriparser/UriMemory.c',
    'vendored/uriparser/UriNormalize.c',
    'vendored/uriparser/UriNormalizeBase.c',
    'vendored/uriparser/UriParse.c',
    'vendored/uriparser/UriParseBase.c',
    'vendored/uriparser/UriQuery.c',
    'vendored/uriparser/UriRecompose.c',
    'vendored/uriparser/UriResolve.c',
    'vendored/uriparser/UriShorten.c',
]

arrow_util_srcs = [
    'util/align_util.cc',
    'util/async_util.cc',
    'util/atfork_internal.cc',
    'util/basic_decimal.cc',
    'util/bit_block_counter.cc',
    'util/bit_run_reader.cc',
    'util/bit_util.cc',
    'util/bitmap.cc',
    'util/bitmap_builders.cc',
    'util/bitmap_ops.cc',
    'util/bpacking.cc',
    'util/byte_size.cc',
    'util/cancel.cc',
    'util/compression.cc',
    'util/counting_semaphore.cc',
    'util/cpu_info.cc',
    'util/crc32.cc',
    'util/debug.cc',
    'util/decimal.cc',
    'util/delimiting.cc',
    'util/dict_util.cc',
    'util/fixed_width_internal.cc',
    'util/float16.cc',
    'util/formatting.cc',
    'util/future.cc',
    'util/hashing.cc',
    'util/int_util.cc',
    'util/io_util.cc',
    'util/list_util.cc',
    'util/logging.cc',
    'util/key_value_metadata.cc',
    'util/memory.cc',
    'util/mutex.cc',
    'util/ree_util.cc',
    'util/string.cc',
    'util/string_builder.cc',
    'util/task_group.cc',
    'util/tdigest.cc',
    'util/thread_pool.cc',
    'util/time.cc',
    'util/tracing.cc',
    'util/trie.cc',
    'util/union_util.cc',
    'util/unreachable.cc',
    'util/uri.cc',
    'util/utf8.cc',
    'util/value_parsing.cc',
]

arrow_compute_srcs = [
    'compute/api_aggregate.cc',
    'compute/api_scalar.cc',
    'compute/api_vector.cc',
    'compute/cast.cc',
    'compute/exec.cc',
    'compute/expression.cc',
    'compute/function.cc',
    'compute/function_internal.cc',
    'compute/kernel.cc',
    'compute/key_hash_internal.cc',
    'compute/key_map_internal.cc',
    'compute/light_array_internal.cc',
    'compute/ordering.cc',
    'compute/registry.cc',
    'compute/kernels/codegen_internal.cc',
    'compute/kernels/row_encoder.cc',
    'compute/kernels/ree_util_internal.cc',
    'compute/kernels/scalar_cast_boolean.cc',
    'compute/kernels/scalar_cast_dictionary.cc',
    'compute/kernels/scalar_cast_extension.cc',
    'compute/kernels/scalar_cast_internal.cc',
    'compute/kernels/scalar_cast_nested.cc',
    'compute/kernels/scalar_cast_numeric.cc',
    'compute/kernels/scalar_cast_string.cc',
    'compute/kernels/scalar_cast_temporal.cc',
    'compute/kernels/util_internal.cc',
    'compute/kernels/vector_hash.cc',
    'compute/kernels/vector_selection.cc',
    'compute/kernels/vector_selection_filter_internal.cc',
    'compute/kernels/vector_selection_internal.cc',
    'compute/kernels/vector_selection_take_internal.cc',
    'compute/row/encode_internal.cc',
    'compute/row/compare_internal.cc',
    'compute/row/grouper.cc',
    'compute/row/row_internal.cc',
    'compute/util.cc',
    'compute/util_internal.cc',
]

if needs_integration
    arrow_integration_srcs = [
        'integration/c_data_integration_internal.cc',
        'integration/json_integration.cc',
        'integration/json_internal.cc',
    ]

    rapidjson_dep = dependency('rapidjson')
    arrow_lib_dependencies += rapidjson_dep
else
    arrow_integration_srcs = []
endif

if needs_ipc
    arrow_ipc_srcs = [
       'ipc/dictionary.cc',
       'ipc/feather.cc',
       'ipc/message.cc',
       'ipc/metadata_internal.cc',
       'ipc/options.cc',
       'ipc/reader.cc',
       'ipc/writer.cc',
    ]

    if needs_json
        arrow_ipc_srcs += 'ipc/json_simple.cc'
    endif
else
    arrow_ipc_srcs = []
endif

if needs_json
  arrow_json_srcs = [
      'extension/fixed_shape_tensor.cc',
      'json/options.cc',
      'json/chunked_builder.cc',
      'json/chunker.cc',
      'json/converter.cc',
      'json/object_parser.cc',
      'json/object_writer.cc',
      'json/parser.cc',
      'json/reader.cc',
  ]
else
  arrow_json_srcs = []
endif

arrow_lib = library(
    'arrow',
    sources: arrow_srcs
      + arrow_array_srcs
      + arrow_io_srcs
      + arrow_memory_pool_srcs
      + arrow_vendored_srcs
      + arrow_util_srcs
      + arrow_compute_srcs
      + arrow_ipc_srcs
      + arrow_json_srcs
      + arrow_integration_srcs,
    include_directories: [root_dir, hadoop_dir, flatbuf_dir],
    install: true,
    dependencies: [arrow_lib_dependencies, arrow_vcs_config_dep],
)

if needs_testing or get_option('benchmarks')
    gmock_dep = dependency('gmock')
endif

if needs_testing
    arrow_testing_srcs = [
        'io/test_common.cc',
        'ipc/test_common.cc',
        'testing/fixed_width_test_util.cc',
        'testing/gtest_util.cc',
        'testing/random.cc',
        'testing/generator.cc',
        'testing/util.cc',
    ]

    gtest_main_dep = dependency('gtest_main')

    arrow_testing_lib = library(
        'arrow_testing',
        arrow_testing_srcs,
        include_directories: [root_dir],
        install: true,
        dependencies: [rapidjson_dep, gtest_main_dep],
        link_with: [arrow_lib],
    )

    arrow_tests = {
        'array': {
            'sources': [
                'array/array_test.cc',
                'array/array_binary_test.cc',
                'array/array_dict_test.cc',
                'array/array_list_test.cc',
                'array/array_list_view_test.cc',
                'array/array_run_end_test.cc',
                'array/array_struct_test.cc',
                'array/array_union_test.cc',
                'array/array_view_test.cc',
            ],
            'deps': [gmock_dep],
        },
        'buffer': {
            'sources': ['buffer_test.cc'],
            'deps': [gtest_main_dep],
        },
        'misc': {
            'sources': [
                'datum_test.cc',
                'memory_pool_test.cc',
                'result_test.cc',
                'pretty_print_test.cc',
                'status_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'public-api': {
            'sources': ['public_api_test.cc'],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'scalar': {
            'sources': ['scalar_test.cc'],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'type': {
            'sources': [
                'field_ref_test.cc',
                'scalar_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'table': {
            'sources': [
                'chunked_array_test.cc',
                'record_batch_test.cc',
                'table_test.cc',
                'table_builder_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'tensor': {
            'sources': ['tensor_test.cc'],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'sparse-tensor': {
            'sources': ['sparse_tensor_test.cc'],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'stl': {
            'sources': [
                'stl_iterator_test.cc',
                'stl_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
    }

    foreach testname, test_config : arrow_tests
        exc = executable(
            'arrow-' + testname + '-test',
            sources: test_config['sources'],
            include_directories: [root_dir],
            link_with: [arrow_lib, arrow_testing_lib],
            dependencies: test_config['deps']
        )
        test('arrow-' + testname, exc)
    endforeach

    if needs_ipc
        extension_type_exec = executable(
            'arrow-extension-type-test',
            sources: ['extension_type_test.cc'],
            include_directories: [root_dir],
            link_with: [arrow_lib, arrow_testing_lib],
            dependencies: [gtest_main_dep],
        )

        test('arrow-extension-type', extension_type_exec)
    endif

    if get_option('benchmarks')
        gtest_dep = dependency('gtest')  # does not include main
        gbenchmark_dep = dependency('benchmark')
    endif

    subdir('integration')
    subdir('testing')

    subdir('array')
    subdir('c')
    if get_option('compute')
      subdir('compute')
    endif
    subdir('io')
    subdir('tensor')

    # CMake includes a vendored subdir - do we need that for meson?
    # subdir('vendored')

    # N.B. we cannot subdir('util') because we already used that
    # for the configure_file, and Meson has a stricter structure
    # than CMake for directory layout
    util_tests = {
        'utility': {
            'sources': [
                'util/align_util_test.cc',
                'util/atfork_test.cc',
                'util/byte_size_test.cc',
                'util/byte_stream_split_test.cc',
                'util/cache_test.cc',
                'util/checked_cast_test.cc',
                'util/compression_test.cc',
                'util/decimal_test.cc',
                'util/float16_test.cc',
                'util/fixed_width_test.cc',
                'util/formatting_util_test.cc',
                'util/key_value_metadata_test.cc',
                'util/hashing_test.cc',
                'util/int_util_test.cc',
                'util/iterator_test.cc',
                'util/list_util_test.cc',
                'util/logging_test.cc',
                'util/queue_test.cc',
                'util/range_test.cc',
                'util/ree_util_test.cc',
                'util/reflection_test.cc',
                'util/rows_to_batches_test.cc',
                'util/small_vector_test.cc',
                'util/span_test.cc',
                'util/stl_util_test.cc',
                'util/string_test.cc',
                'util/tdigest_test.cc',
                'util/test_common.cc',
                'util/time_test.cc',
                'util/tracing_test.cc',
                'util/trie_test.cc',
                'util/uri_test.cc',
                'util/utf8_util_test.cc',
                'util/value_parsing_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'async-utility': {
            'sources': [
                'util/async_generator_test.cc',
                'util/async_util_test.cc',
                'util/test_common.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'bit-utility': {
            'sources': [
                'util/bit_block_counter_test.cc',
                'util/bit_util_test.cc',
                'util/rle_encoding_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'threading-utility': {
            'sources': [
                'util/cancel_test.cc',
                'util/counting_semaphore_test.cc',
                'util/future_test.cc',
                'util/task_group_test.cc',
                'util/test_common.cc',
                'util/thread_pool_test.cc',
            ],
            'deps': [gtest_main_dep, gmock_dep],
        },
        'crc32': {
            'sources': ['util/crc32_test.cc'],
            'deps': [gtest_main_dep, gmock_dep],
        },
    }

    foreach testname, test_config : util_tests
        exc = executable(
            'arrow-' + testname + '-test',
            sources: test_config['sources'],
            include_directories: [root_dir],
            link_with: [arrow_lib, arrow_testing_lib],
            dependencies: test_config['deps'],
            install_dir: 'util/'
        )
        test('arrow-' + testname, exc)
    endforeach

    if get_option('benchmarks')
        gtest_dep = dependency('gtest')  # does not include main
        gbenchmark_dep = dependency('benchmark')
        util_benchmarks = {
            'bit-block-counter': {
                'src_name': 'bit_block_counter',
                'link_with': [arrow_lib, arrow_testing_lib],
            },
            'bit-util': {
                'src_name': 'bit_util',
                'link_with': [arrow_lib, arrow_testing_lib],
            },
            'bitmap-reader': {
                'src_name': 'bitmap_reader',
                'link_with': [arrow_lib, arrow_testing_lib],
            },
            'cache': {
                'src_name': 'cache',
                'link_with': [arrow_lib, arrow_testing_lib],
            },
            'compression': {
                'src_name': 'compression',
                'link_with': [arrow_lib],
            },
            'decimal': {
                'src_name': 'decimal',
                'link_with': [arrow_lib],
            },
            'hashing': {
                'src_name': 'hashing',
                'link_with': [arrow_lib],
            },
            'int-util': {
                'src_name': 'int_util',
                'link_with': [arrow_lib, arrow_testing_lib],
            },
            'machine': {
                'src_name': 'machine',
                'link_with': [arrow_lib],
            },
            'queue': {
                'src_name': 'queue',
                'link_with': [arrow_lib],
            },
            'range': {
                'src_name': 'range',
                'link_with': [arrow_lib],
            },
            'small-vector': {
                'src_name': 'small_vector',
                'link_with': [arrow_lib],
            },
            'tdigest': {
                'src_name': 'tdigest',
                'link_with': [arrow_lib],
            },
            'thread-pool': {
                'src_name': 'thread_pool',
                'link_with': [arrow_lib],
            },
            'trie': {
                'src_name': 'trie',
                'link_with': [arrow_lib],
            },
            'utf8-util': {
                'src_name': 'utf8_util',
                'link_with': [arrow_lib],
            },
            'value-parsing': {
                'src_name': 'value_parsing',
                'link_with': [arrow_lib],
            },
        }

        foreach nm, conf : util_benchmarks
            exc = executable(
                'arrow-' + nm + '-benchmark',
                'util/' + conf['src_name'] + '_benchmark.cc',
                include_directories: [root_dir],
                link_with: conf['link_with'],
                dependencies: [gbenchmark_dep, gtest_dep],
                install_dir: 'util/',
            )
            benchmark('arrow-' + nm, exc)
        endforeach
    endif

    if needs_ipc
        subdir('ipc')
    endif

    if needs_json
        subdir('json')
        subdir('extension')
    endif
endif
