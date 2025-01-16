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

include_dir = include_directories('..')

dl_dep = dependency('dl')

# Meson does not natively support object libraries
# https://github.com/mesonbuild/meson/issues/13843
objlib_sources = {
    'arrow_array': {
        'sources': [
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
            'array/statistics.cc',
            'array/util.cc',
            'array/validate.cc',
        ],
        'include_dirs': [include_dir],
        'dependencies': [],
    },
    'arrow_io': {
        'sources': [
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
        ],
        'include_dirs': [
            include_dir,
            include_directories('../../thirdparty/hadoop/include'),
        ],
        'dependencies': [dl_dep],
    },
    'arrow_util': {
        'sources': [
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
            'util/logger.cc',
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
        ],
        'include_dirs': [include_dir],
        'dependencies': [],
    },
    'memory_pool': {
        'sources': ['memory_pool.cc'],
        'include_dirs': [include_dir],
        'dependencies': [],
    },
    'vendored': {
        'sources': [
            'vendored/base64.cpp',
            'vendored/datetime.cpp',
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
        ],
        'include_dirs': [include_dir],
        'dependencies': [],
    },
}

if needs_ipc
    flatbuffers_dep = dependency('flatbuffers', include_type: 'system')
    ipc_deps = [flatbuffers_dep]

    ipc_sources = [
        'ipc/dictionary.cc',
        'ipc/feather.cc',
        'ipc/message.cc',
        'ipc/metadata_internal.cc',
        'ipc/options.cc',
        'ipc/reader.cc',
        'ipc/writer.cc',
    ]

    if needs_json
        ipc_sources += ['ipc/json_simple.cc']
        rapidjson_dep = dependency('rapidjson', include_type: 'system')
        ipc_deps += [rapidjson_dep]
    endif

    objlib_sources += {
        'arrow_ipc': {
            'sources': ipc_sources,
            'include_dirs': [
                include_dir,
                include_directories('../../thirdparty/flatbuffers/include'),
            ],
            'dependencies': ipc_deps,
        },
    }
endif

if needs_json
    objlib_sources += {
        'arrow_json': {
            'sources': [
                'extension/fixed_shape_tensor.cc',
                'extension/opaque.cc',
                'json/options.cc',
                'json/chunked_builder.cc',
                'json/chunker.cc',
                'json/converter.cc',
                'json/object_parser.cc',
                'json/object_writer.cc',
                'json/parser.cc',
                'json/reader.cc',
            ],
            'include_dirs': [include_dir],
            'dependencies': [],
        },
    }
endif

compute_srcs = [
    'compute/api_aggregate.cc',
    'compute/api_scalar.cc',
    'compute/api_vector.cc',
    'compute/cast.cc',
    'compute/exec.cc',
    'compute/expression.cc',
    'compute/function.cc',
    'compute/function_internal.cc',
    'compute/kernel.cc',
    'compute/ordering.cc',
    'compute/registry.cc',
    'compute/kernels/chunked_internal.cc',
    'compute/kernels/codegen_internal.cc',
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
]

if needs_compute
    compute_srcs += [
        'compute/kernels/aggregate_basic.cc',
        'compute/kernels/aggregate_mode.cc',
        'compute/kernels/aggregate_quantile.cc',
        'compute/kernels/aggregate_tdigest.cc',
        'compute/kernels/aggregate_var_std.cc',
        'compute/kernels/hash_aggregate.cc',
        'compute/kernels/scalar_arithmetic.cc',
        'compute/kernels/scalar_boolean.cc',
        'compute/kernels/scalar_compare.cc',
        'compute/kernels/scalar_if_else.cc',
        'compute/kernels/scalar_nested.cc',
        'compute/kernels/scalar_random.cc',
        'compute/kernels/scalar_round.cc',
        'compute/kernels/scalar_set_lookup.cc',
        'compute/kernels/scalar_string_ascii.cc',
        'compute/kernels/scalar_string_utf8.cc',
        'compute/kernels/scalar_temporal_binary.cc',
        'compute/kernels/scalar_temporal_unary.cc',
        'compute/kernels/scalar_validity.cc',
        'compute/kernels/vector_array_sort.cc',
        'compute/kernels/vector_cumulative_ops.cc',
        'compute/kernels/vector_nested.cc',
        'compute/kernels/vector_pairwise.cc',
        'compute/kernels/vector_rank.cc',
        'compute/kernels/vector_replace.cc',
        'compute/kernels/vector_run_end_encode.cc',
        'compute/kernels/vector_select_k.cc',
        'compute/kernels/vector_sort.cc',
        'compute/kernels/vector_swizzle.cc',
        'compute/key_hash_internal.cc',
        'compute/key_map_internal.cc',
        'compute/light_array_internal.cc',
        'compute/row/encode_internal.cc',
        'compute/row/compare_internal.cc',
        'compute/row/grouper.cc',
        'compute/row/row_encoder_internal.cc',
        'compute/row/row_internal.cc',
        'compute/util.cc',
        'compute/util_internal.cc',
    ]
endif

objlib_sources += {
    'arrow_compute': {
        'sources': compute_srcs,
        'include_dirs': [include_dir],
        'dependencies': [],
    },
}

objlibs = []
foreach key, val : objlib_sources
    slib = static_library(
        key,
        sources: val['sources'],
        include_directories: val['include_dirs'],
        dependencies: val['dependencies'],
        build_by_default: false,
    )
    objlibs += slib.extract_all_objects(recursive: true)
endforeach

arrow_srcs = [
    'builder.cc',
    'buffer.cc',
    'chunked_array.cc',
    'chunk_resolver.cc',
    'compare.cc',
    'config.cc',
    'datum.cc',
    'device.cc',
    'device_allocation_type_set.cc',
    'extension_type.cc',
    'extension/bool8.cc',
    'extension/json.cc',
    'extension/uuid.cc',
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

arrow_lib = library(
    'arrow',
    sources: [arrow_srcs],
    objects: objlibs,
    include_directories: [include_dir],
)

if needs_tests
    gtest_dep = dependency('gtest_main')
    gmock_dep = dependency('gmock')
    boost_dep = dependency('boost', modules: ['filesystem'])

    arrow_test_dep = declare_dependency(
        sources: [
            'io/test_common.cc',
            'ipc/test_common.cc',
            'testing/fixed_width_test_util.cc',
            'testing/generator.cc',
            'testing/gtest_util.cc',
            'testing/math.cc',
            'testing/process.cc',
            'testing/random.cc',
            'testing/util.cc',
        ],
        include_directories: [include_dir],
        dependencies: [gtest_dep, gmock_dep, boost_dep],
        link_with: [arrow_lib],
    )

    tests = {
        'arrow-array-test': {
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
                'array/statistics_test.cc',
            ],
        },
        'arrow-buffer-test': {'sources': ['buffer_test.cc']},
        'arrow-misc-test': {
            'sources': [
                'datum_test.cc',
                'memory_pool_test.cc',
                'result_test.cc',
                'pretty_print_test.cc',
                'status_test.cc',
            ],
        },
        'arrow-public-api-test': {'sources': ['public_api_test.cc']},
        'arrow-scalar-test': {'sources': ['scalar_test.cc']},
        'arrow-type-test': {'sources': ['field_ref_test.cc', 'type_test.cc']},
        'arrow-table-test': {
            'sources': [
                'chunked_array_test.cc',
                'record_batch_test.cc',
                'table_test.cc',
                'table_builder_test.cc',
            ],
        },
        'arrow-tensor-test': {'sources': ['tensor_test.cc']},
        'arrow-sparse-tensor-test': {'sources': ['sparse_tensor_test.cc']},
        'arrow-stl-test': {'sources': ['stl_iterator_test.cc', 'stl_test.cc']},
    }

    if needs_ipc
        tests += {
            'arrow-extension-type-test': {'sources': ['extension_type_test.cc']},
        }
    endif

    foreach key, val : tests
        exc = executable(
            key,
            sources: val['sources'],
            dependencies: [arrow_test_dep],
        )
        test(key, exc)
    endforeach
endif

# TODO: in CMake configuration we add benchmarks here


subdir('testing')
subdir('array')
subdir('c')
subdir('compute')
subdir('extension')
subdir('io')
subdir('tensor')
subdir('util')

if needs_tests  # or build_integration, when added
    subdir('integration')
endif

if needs_ipc
    subdir('ipc')
endif

if needs_json
    subdir('json')
endif

