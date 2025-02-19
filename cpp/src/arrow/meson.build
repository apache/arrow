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
        'include_dirs': [],
        'dependencies': [],
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
        'include_dirs': [],
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
        'include_dirs': [include_directories('../../thirdparty/hadoop/include')],
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
        'include_dirs': [],
        'dependencies': [threads_dep],
    },
    'memory_pool': {
        'sources': ['memory_pool.cc'],
        'include_dirs': [],
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
        'include_dirs': [],
        'dependencies': [],
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
        'include_dirs': [],
        'dependencies': [],
    },
}

arrow_srcs = []
include_dir = include_directories('..')
arrow_includes = [include_dir]
arrow_deps = []
foreach key, val : arrow_components
    arrow_srcs += val['sources']
    arrow_includes += val['include_dirs']
    arrow_deps += val['dependencies']
endforeach

arrow_lib = library(
    'arrow',
    sources: arrow_srcs,
    include_directories: arrow_includes,
    dependencies: arrow_deps,
    install: true,
)

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
    install_dir: 'arrow',
)

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

arrow_version = (version_major.to_int() * 1000 + version_minor.to_int()) * 1000 + version_patch.to_int()
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

subdir('util')
