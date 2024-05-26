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

subdir('util')

arrow_lib_dependencies = []
root_dir = include_directories('..')
hadoop_dir = include_directories('../../thirdparty/hadoop/include')
flatbuf_dir = include_directories('../../thirdparty/flatbuffers/include')

needs_integration = get_option('ARROW_BUILD_TESTS') # or get_option('ARROW_BUILD_INTEGRATION')
needs_ipc = get_option('ARROW_IPC') or get_option('ARROW_BUILD_TESTS')
needs_testing = get_option('ARROW_BUILD_TESTS') # or get_option('ARROW_TESTING')
needs_json = needs_testing # or get_option('ARROW_JSON')

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

    gflags_dep = dependency('gflags')
    gtest_dep = dependency('gtest', fallback: ['gtest', 'gtest_main_dep'])
    gmock_dep = dependency('gmock')    
    
    arrow_testing_lib = library(
        'arrow_testing',
        arrow_testing_srcs,
        include_directories: [root_dir],
        install: true,
        dependencies: [rapidjson_dep, gtest_dep],
        link_with: [arrow_lib],
    )

    # TODO: direct invocation of this test gives:
    # undefined symbol: _ZN5arrow7PrintToERKNS_5ArrayEPSo
    # even in existing Cmake configuration
    array_test = executable(
        'array_test',
        'array/array_test.cc',
        'array/array_binary_test.cc',
        'array/array_dict_test.cc',
        'array/array_list_test.cc',
        'array/array_list_view_test.cc',
        'array/array_run_end_test.cc',
        'array/array_struct_test.cc',
        'array/array_union_test.cc',
        'array/array_view_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gmock_dep],
    )
    test('array', array_test)

    buffer_test = executable(
        'buffer_test',
        'buffer_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep],
    )
    test('buffer', buffer_test)

    if needs_ipc
        extension_type_test = executable(
            'extension_type_test',
            'extension_type_test.cc',
            include_directories: [root_dir],
            link_with: [arrow_lib, arrow_testing_lib],
            dependencies: [gtest_dep],
        )
        test('extension_type', extension_type_test)
    endif

    misc_test = executable(
        'misc_test',
        'datum_test.cc',
        'memory_pool_test.cc',
        'result_test.cc',
        'pretty_print_test.cc',
        'status_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('misc', misc_test)

    public_api_test = executable(
        'public_api_test',
        'public_api_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('public_api', public_api_test)

    scalar_test = executable(
        'scalar_test',
        'scalar_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('scalar', scalar_test)

    type_test = executable(
        'type_test',
        'field_ref_test.cc',
        'type_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('type', type_test)

    table_test = executable(
        'table_test',
        'chunked_array_test.cc',
        'record_batch_test.cc',
        'table_test.cc',
        'table_builder_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('table', table_test)

    tensor_test = executable(
        'tensor_test',
        'tensor_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('tensor', tensor_test)

    sparse_tensor_test = executable(
        'sparse_tensor_test',
        'sparse_tensor_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('sparse_tensor', sparse_tensor_test)

    stl_test = executable(
        'stl_test',
        'stl_iterator_test.cc',
        'stl_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_dep, gmock_dep],        
    )
    test('stl', stl_test)    

    subdir('testing')
    subdir('array')
    # TODO: continue down the line of subdir here - follow CMakeLists.txt:1200 onwards
endif
