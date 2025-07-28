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

dl_dep = dependency('dl')
threads_dep = dependency('threads')

arrow_components = {
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
    },
    'arrow_compute': {
        'sources': [
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
        ],
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
        'include_dirs': [include_directories('../../thirdparty/hadoop/include')],
        'dependencies': [dl_dep],
    },
    'memory_pool': {'sources': ['memory_pool.cc']},
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
    },
    'arrow_base': {
        'sources': [
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
        ],
    },
}

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
    'util/logger.cc',
    'util/logging.cc',
    'util/key_value_metadata.cc',
    'util/math_internal.cc',
    'util/memory.cc',
    'util/mutex.cc',
    'util/ree_util.cc',
    'util/string.cc',
    'util/string_util.cc',
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

arrow_util_deps = [threads_dep]

if needs_brotli
    arrow_util_srcs += ['util/compression_brotli.cc']
    arrow_util_deps += [dependency('libbrotlidec'), dependency('libbrotlienc')]
endif

if needs_bz2
    arrow_util_srcs += ['util/compression_bz2.cc']
    arrow_util_deps += dependency('bzip2')
endif

if needs_lz4
    arrow_util_srcs += ['util/compression_lz4.cc']
    arrow_util_deps += dependency('liblz4')
endif

if needs_snappy
    arrow_util_srcs += ['util/compression_snappy.cc']
    arrow_util_deps += dependency('snappy')
endif

if needs_zlib
    arrow_util_srcs += ['util/compression_zlib.cc']
    arrow_util_deps += dependency('zlib')
endif

if needs_zstd
    arrow_util_srcs += ['util/compression_zstd.cc']
    arrow_util_deps += dependency('libzstd')
endif

arrow_components += {
    'arrow_util': {'sources': arrow_util_srcs, 'dependencies': arrow_util_deps},
}

arrow_testing_srcs = [
    'io/test_common.cc',
    'ipc/test_common.cc',
    'testing/fixed_width_test_util.cc',
    'testing/generator.cc',
    'testing/gtest_util.cc',
    'testing/math.cc',
    'testing/process.cc',
    'testing/random.cc',
    'testing/util.cc',
]

if needs_integration or needs_tests
    arrow_components += {
        'arrow_integration': {
            'sources': [
                'integration/c_data_integration_internal.cc',
                'integration/json_integration.cc',
                'integration/json_internal.cc',
            ],
        },
    }
endif

if needs_csv
    arrow_components += {
        'arrow_csv': {
            'sources': [
                'csv/converter.cc',
                'csv/chunker.cc',
                'csv/column_builder.cc',
                'csv/column_decoder.cc',
                'csv/options.cc',
                'csv/parser.cc',
                'csv/reader.cc',
                'csv/writer.cc',
            ],
        },
    }

    arrow_testing_srcs += ['csv/test_common.cc']
endif

if needs_json or needs_integration
    rapidjson_dep = dependency('rapidjson', include_type: 'system')
else
    rapidjson_dep = disabler()
endif

azure_dep = disabler()
gcs_dep = disabler()
s3_dep = disabler()
if needs_filesystem
    arrow_filesystem_srcs = [
        'filesystem/filesystem.cc',
        'filesystem/localfs.cc',
        'filesystem/mockfs.cc',
        'filesystem/path_util.cc',
        'filesystem/util_internal.cc',
    ]

    arrow_filesystem_deps = []

    if needs_azure
        arrow_filesystem_srcs += ['filesystem/azurefs.cc']
        cmake = import('cmake')
        azure_opt = cmake.subproject_options()
        azure_opt.add_cmake_defines(
            {'BUILD_PERFORMANCE_TESTS': 'FALSE'},
            {'BUILD_SAMPLES': 'FALSE'},
            {'BUILD_TESTING': 'FALSE'},
            {'BUILD_WINDOWS_UWP': 'TRUE'},
            {'CMAKE_UNITY_BUILD': 'FALSE'},
            {'DISABLE_AZURE_CORE_OPENTELEMETRY': 'TRUE'},
            {'ENV{AZURE_SDK_DISABLE_AUTO_VCPKG}': 'TRUE'},
            {'WARNINGS_AS_ERRORS': 'FALSE'},
        )
        azure_opt.append_compile_args('cpp', '-fPIC')
        azure_proj = cmake.subproject('azure', options: azure_opt)

        azure_dep = declare_dependency(
            dependencies: [
                azure_proj.dependency('azure-core'),
                azure_proj.dependency('azure-identity'),
                azure_proj.dependency('azure-storage-blobs'),
                azure_proj.dependency('azure-storage-common'),
                azure_proj.dependency('azure-storage-files-datalake'),
            ],
        )
        arrow_filesystem_deps += [azure_dep]
    endif

    if needs_gcs
        error('gcs filesystem support is not yet implemented in Meson')
    endif

    if needs_hdfs
        arrow_filesystem_srcs += ['filesystem/hdfs.cc']
    endif

    if needs_s3
        error('s3 filesystem support is not yet implemented in Meson')
    endif

    arrow_components += {
        'arrow_filesystem': {
            'sources': arrow_filesystem_srcs,
            'dependencies': arrow_filesystem_deps,
        },
    }

    arrow_testing_srcs += ['filesystem/test_util.cc']
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

    flatbuffers_incdir = include_directories(
        '../../thirdparty/flatbuffers/include',
    )
    flatbuffers_dep = declare_dependency(include_directories: flatbuffers_incdir)
    arrow_ipc_deps = [flatbuffers_dep]

    arrow_components += {
        'arrow_ipc': {'sources': arrow_ipc_srcs, 'dependencies': arrow_ipc_deps},
    }
endif

if needs_json
    arrow_components += {
        'arrow_json': {
            'sources': [
                'extension/fixed_shape_tensor.cc',
                'extension/opaque.cc',
                'json/options.cc',
                'json/chunked_builder.cc',
                'json/chunker.cc',
                'json/converter.cc',
                'json/from_string.cc',
                'json/object_parser.cc',
                'json/object_writer.cc',
                'json/parser.cc',
                'json/reader.cc',
            ],
            'dependencies': [rapidjson_dep],
        },
    }
endif

arrow_srcs = []
include_dir = include_directories('..')
arrow_includes = [include_dir]
arrow_deps = []
foreach key, val : arrow_components
    arrow_srcs += val['sources']
    arrow_includes += val.get('include_dirs', [])
    arrow_deps += val.get('dependencies', [])
endforeach

arrow_lib = library(
    'arrow',
    sources: arrow_srcs,
    include_directories: arrow_includes,
    dependencies: arrow_deps,
    install: true,
    gnu_symbol_visibility: 'hidden',
    cpp_shared_args: ['-DARROW_EXPORTING'],
)

arrow_dep = declare_dependency(
    include_directories: [include_dir],
    link_with: arrow_lib,
)

if needs_compute
    arrow_compute_lib_sources = [
        'compute/initialize.cc',
        'compute/kernels/aggregate_basic.cc',
        'compute/kernels/aggregate_mode.cc',
        'compute/kernels/aggregate_pivot.cc',
        'compute/kernels/aggregate_quantile.cc',
        'compute/kernels/aggregate_tdigest.cc',
        'compute/kernels/aggregate_var_std.cc',
        'compute/kernels/hash_aggregate.cc',
        'compute/kernels/hash_aggregate_numeric.cc',
        'compute/kernels/hash_aggregate_pivot.cc',
        'compute/kernels/pivot_internal.cc',
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
        'compute/kernels/util_internal.cc',
        'compute/kernels/vector_array_sort.cc',
        'compute/kernels/vector_cumulative_ops.cc',
        'compute/kernels/vector_nested.cc',
        'compute/kernels/vector_pairwise.cc',
        'compute/kernels/vector_rank.cc',
        'compute/kernels/vector_replace.cc',
        'compute/kernels/vector_run_end_encode.cc',
        'compute/kernels/vector_select_k.cc',
        'compute/kernels/vector_sort.cc',
        'compute/kernels/vector_statistics.cc',
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

    arrow_compute_lib = library(
        'arrow-compute',
        sources: arrow_compute_lib_sources,
        dependencies: arrow_dep,
        install: true,
        cpp_shared_args: ['-DARROW_COMPUTE_EXPORTING'],
    )
    arrow_compute_dep = declare_dependency(
        link_with: arrow_compute_lib,
        include_directories: include_dir,
        dependencies: arrow_dep,
    )
else
    arrow_compute_dep = disabler()
endif

# Meson does not allow you to glob for headers to install. See also
# https://mesonbuild.com/FAQ.html#why-cant-i-specify-target-files-with-a-wildcard
# install_subdir would be usable if the directory only contained headers
install_headers(
    [
        'api.h',
        'array.h',
        'buffer_builder.h',
        'buffer.h',
        'builder.h',
        'chunked_array.h',
        'chunk_resolver.h',
        'compare.h',
        'config.h',
        'datum.h',
        'device_allocation_type_set.h',
        'device.h',
        'extension_type.h',
        'memory_pool.h',
        'pretty_print.h',
        'record_batch.h',
        'result.h',
        'scalar.h',
        'sparse_tensor.h',
        'status.h',
        'stl.h',
        'stl_allocator.h',
        'stl_iterator.h',
        'table_builder.h',
        'table.h',
        'tensor.h',
        'type_fwd.h',
        'type.h',
        'type_traits.h',
        'visit_array_inline.h',
        'visit_data_inline.h',
        'visitor_generate.h',
        'visitor.h',
        'visit_scalar_inline.h',
        'visit_type_inline.h',
    ],
    subdir: 'arrow',
)

if needs_testing
    filesystem_dep = dependency(
        'boost',
        modules: ['filesystem'],
        required: false,
    )
    if not filesystem_dep.found()
        cmake = import('cmake')
        boost_opt = cmake.subproject_options()
        boost_opt.add_cmake_defines(
            {'BOOST_INCLUDE_LIBRARIES': 'filesystem;system'},
        )
        boost_proj = cmake.subproject('boost', options: boost_opt)
        filesystem_dep = boost_proj.dependency('boost_filesystem')
    endif

    gtest_dep = dependency('gtest')
    gtest_main_dep = dependency('gtest_main')
    gtest_dep = dependency('gtest')
    gmock_dep = dependency('gmock')
else
    filesystem_dep = disabler()
    gtest_dep = disabler()
    gtest_main_dep = disabler()
    gtest_dep = disabler()
    gmock_dep = disabler()
endif

arrow_test_lib = static_library(
    'arrow_testing',
    sources: arrow_testing_srcs,
    dependencies: [arrow_dep, filesystem_dep, gmock_dep, gtest_main_dep],
)

if needs_tests
    arrow_test_dep = declare_dependency(
        link_with: [arrow_test_lib],
        dependencies: [arrow_dep, filesystem_dep, gmock_dep, gtest_main_dep],
    )
    arrow_test_dep_no_main = declare_dependency(
        link_with: [arrow_test_lib],
        dependencies: [arrow_dep, filesystem_dep, gmock_dep, gtest_dep],
    )
else
    arrow_test_dep = disabler()
    arrow_test_dep_no_main = disabler()
endif

arrow_tests = {
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
    arrow_tests += {
        'arrow-extension-type-test': {
            'sources': ['extension_type_test.cc'],
            'dependencies': [arrow_ipc_deps],
        },
    }
endif

foreach key, val : arrow_tests
    exc = executable(
        key,
        sources: val['sources'],
        dependencies: [arrow_test_dep, val.get('dependencies', [])],
    )
    test(key, exc)
endforeach

if needs_benchmarks
    benchmark_main_dep = dependency(
        'benchmark-main',
        default_options: {'tests': 'disabled'},
    )

    arrow_benchmark_dep = declare_dependency(
        link_with: [arrow_test_lib],
        dependencies: [arrow_dep, benchmark_main_dep, gtest_dep],
    )
else
    arrow_benchmark_dep = disabler()
endif

arrow_benchmarks = [
    'builder_benchmark',
    'chunk_resolver_benchmark',
    'compare_benchmark',
    'memory_pool_benchmark',
    'type_benchmark',
    'tensor_benchmark',
]

foreach benchmark : arrow_benchmarks
    benchmark_name = 'arrow-@0@'.format(benchmark.replace('_', '-'))
    exc = executable(
        benchmark_name,
        sources: '@0@.cc'.format(benchmark),
        dependencies: [arrow_benchmark_dep],
    )

    benchmark(benchmark_name, exc)
endforeach

version = meson.project_version()

version_no_snapshot = version.split('-SNAPSHOT')[0]
components = version_no_snapshot.split('.')
assert(
    components.length() >= 3,
    'The version does not contain major, minor and patch',
)
version_major = components[0]
version_minor = components[1]
version_patch = components[2]

arrow_version = '@0@.@1@.@2@'.format(version_major, version_minor, version_patch)
arrow_so_version = (version_major.to_int() * 100 + version_minor.to_int()).to_string()
arrow_full_so_version = '@0@.@1@.@2@'.format(arrow_so_version, version_patch, 0)

pkg = import('pkgconfig')
pkg.generate(
    arrow_lib,
    filebase: 'arrow',
    name: 'Apache Arrow',
    description: 'Arrow is a set of technologies that enable big-data systems to process and move data fast.',
    variables: {
        'so_version': arrow_so_version,
        'abi_version': arrow_so_version,
        'full_so_version': arrow_full_so_version,
    },
)

subdir('testing')

subdir('array')
subdir('compute')
subdir('c')
subdir('extension')
subdir('io')
subdir('tensor')
subdir('util')
subdir('vendored')

gflags_dep = dependency('gflags', include_type: 'system')
if needs_integration or needs_tests
    subdir('integration')
endif

if needs_csv
    subdir('csv')
endif

if needs_acero
    subdir('acero')
endif

if needs_filesystem
    subdir('filesystem')
endif

if needs_flight
    subdir('flight')
endif

if needs_json
    subdir('json')
endif

if needs_ipc
    subdir('ipc')
endif

if get_option('tensorflow').enabled()
    subdir('adapters/tensorflow')
endif
