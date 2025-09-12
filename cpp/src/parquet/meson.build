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

parquet_srcs = files(
    '../generated/parquet_types.cpp',
    'arrow/path_internal.cc',
    'arrow/reader.cc',
    'arrow/reader_internal.cc',
    'arrow/schema.cc',
    'arrow/schema_internal.cc',
    'arrow/variant_internal.cc',
    'arrow/writer.cc',
    'bloom_filter.cc',
    'bloom_filter_reader.cc',
    'chunker_internal.cc',
    'column_reader.cc',
    'column_scanner.cc',
    'column_writer.cc',
    'decoder.cc',
    'encoder.cc',
    'encryption/encryption.cc',
    'encryption/internal_file_decryptor.cc',
    'encryption/internal_file_encryptor.cc',
    'exception.cc',
    'file_reader.cc',
    'file_writer.cc',
    'geospatial/statistics.cc',
    'geospatial/util_internal.cc',
    'geospatial/util_json_internal.cc',
    'level_comparison.cc',
    'level_conversion.cc',
    'metadata.cc',
    'page_index.cc',
    'platform.cc',
    'printer.cc',
    'properties.cc',
    'schema.cc',
    'size_statistics.cc',
    'statistics.cc',
    'stream_reader.cc',
    'stream_writer.cc',
    'types.cc',
    'xxhasher.cc',
)

thrift_dep = dependency('thrift', allow_fallback: false, required: false)
if not thrift_dep.found()
    cmake = import('cmake')
    thrift_opts = cmake.subproject_options()
    thrift_opts.add_cmake_defines(
        {
            'BUILD_COMPILER': 'OFF',
            'BUILD_EXAMPLES': 'OFF',
            'BUILD_TUTORIALS': 'OFF',
            'CMAKE_UNITY_BUILD': 'OFF',
            'WITH_AS3': 'OFF',
            'WITH_CPP': 'ON',
            'WITH_C_GLIB': 'OFF',
            'WITH_JAVA': 'OFF',
            'WITH_JAVASCRIPT': 'OFF',
            'WITH_LIBEVENT': 'OFF',
            'WITH_NODEJS': 'OFF',
            'WITH_PYTHON': 'OFF',
            'WITH_QT5': 'OFF',
            'WITH_ZLIB': 'OFF',
            'CMAKE_POSITION_INDEPENDENT_CODE': 'ON',
            'CMAKE_POLICY_VERSION_MINIMUM': '3.5',
            # dummy value to avoid https://github.com/mesonbuild/meson/issues/13390
            'THRIFT_COMPILER': 'foo',
        },
    )
    thrift_proj = cmake.subproject('thrift', options: thrift_opts)
    thrift_dep = thrift_proj.dependency('thrift')
endif

parquet_deps = [arrow_dep, rapidjson_dep, thrift_dep]

if needs_parquet_encryption or get_option('parquet_require_encryption') == 'auto'
    openssl_dep = dependency('openssl', required: needs_parquet_encryption)
else
    openssl_dep = disabler()
endif

if openssl_dep.found()
    parquet_deps += openssl_dep

    parquet_srcs += files(
        'encryption/crypto_factory.cc',
        'encryption/encryption_internal.cc',
        'encryption/file_key_unwrapper.cc',
        'encryption/file_key_wrapper.cc',
        'encryption/file_system_key_material_store.cc',
        'encryption/key_material.cc',
        'encryption/key_metadata.cc',
        'encryption/key_toolkit.cc',
        'encryption/key_toolkit_internal.cc',
        'encryption/kms_client.cc',
        'encryption/local_wrap_kms_client.cc',
        'encryption/openssl_internal.cc',
    )
else
    parquet_srcs += files('encryption/encryption_internal_nossl.cc')
endif

parquet_lib = library(
    'arrow-parquet',
    sources: parquet_srcs,
    dependencies: parquet_deps,
    # TODO: enable hidden visibility by default
    #gnu_symbol_visibility: 'inlineshidden',
)

parquet_dep = declare_dependency(link_with: parquet_lib)

subdir('api')
subdir('arrow')
subdir('encryption')
subdir('geospatial')

install_headers(
    [
        'benchmark_util.h',
        'bloom_filter.h',
        'bloom_filter_reader.h',
        'column_page.h',
        'column_reader.h',
        'column_scanner.h',
        'column_writer.h',
        'encoding.h',
        'exception.h',
        'file_reader.h',
        'file_writer.h',
        'hasher.h',
        'level_comparison.h',
        'level_comparison_inc.h',
        'level_conversion.h',
        'level_conversion_inc.h',
        'metadata.h',
        'page_index.h',
        'platform.h',
        'printer.h',
        'properties.h',
        'schema.h',
        'size_statistics.h',
        'statistics.h',
        'stream_reader.h',
        'stream_writer.h',
        'test_util.h',
        'type_fwd.h',
        'types.h',
        'windows_compatibility.h',
        'windows_fixup.h',
        'xxhasher.h',
    ],
    subdir: 'parquet',
)

conf_data = configuration_data()

conf_data.set('ARROW_VERSION_MAJOR', version_major)
conf_data.set('ARROW_VERSION_MINOR', version_minor)
conf_data.set('ARROW_VERSION_PATCH', version_patch)

conf_data.set('ARROW_VERSION', arrow_version)

conf_data.set('ARROW_SO_VERSION', arrow_so_version)
conf_data.set('ARROW_FULL_SO_VERSION', arrow_full_so_version)

configure_file(
    input: 'parquet_version.h.in',
    output: 'parquet_version.h',
    configuration: conf_data,
    install: true,
    install_dir: 'parquet',
)

parquet_tests = {
    'internals-test': {
        'sources': files(
            'bloom_filter_reader_test.cc',
            'bloom_filter_test.cc',
            'encoding_test.cc',
            'geospatial/statistics_test.cc',
            'geospatial/util_internal_test.cc',
            'metadata_test.cc',
            'page_index_test.cc',
            'properties_test.cc',
            'public_api_test.cc',
            'size_statistics_test.cc',
            'statistics_test.cc',
            'types_test.cc',
        ),
    },
    'reader-test': {
        'sources': files(
            'column_reader_test.cc',
            'column_scanner_test.cc',
            'level_conversion_test.cc',
            'reader_test.cc',
            'stream_reader_test.cc',
        ),
    },
    'writer-test': {
        'sources': files(
            'column_writer_test.cc',
            'file_serialize_test.cc',
            'stream_writer_test.cc',
        ),
    },
    'chunker-test': {'sources': files('chunker_internal_test.cc')},
    'arrow-reader-writer-test': {
        'sources': files(
            'arrow/arrow_reader_writer_test.cc',
            'arrow/arrow_statistics_test.cc',
            'arrow/variant_test.cc',
        ),
    },
    'arrow-internals-test': {
        'sources': files(
            'arrow/path_internal_test.cc',
            'arrow/reconstruct_internal_test.cc',
        ),
    },
    'arrow-metadata-test': {
        'sources': files(
            'arrow/arrow_metadata_test.cc',
            'arrow/arrow_schema_test.cc',
        ),
    },
    'file_deserialize_test': {'sources': files('file_deserialize_test.cc')},
    'schema_test': {'sources': files('schema_test.cc')},
}

if needs_parquet_encryption
    parquet_tests += {
        'encryption-test': {
            'sources': files(
                'encryption/encryption_internal_test.cc',
                'encryption/properties_test.cc',
                'encryption/read_configurations_test.cc',
                'encryption/test_encryption_util.cc',
                'encryption/write_configurations_test.cc',
            ),
        },
        'encryption-key-management-test': {
            'sources': files(
                'encryption/key_management_test.cc',
                'encryption/key_metadata_test.cc',
                'encryption/key_wrapping_test.cc',
                'encryption/test_encryption_util.cc',
                'encryption/test_in_memory_kms.cc',
                'encryption/two_level_cache_with_expiration_test.cc',
            ),
        },
    }
endif

parquet_test_dep = [parquet_dep, arrow_test_dep, thrift_dep]

foreach key, val : parquet_tests
    test_name = 'parquet-@0@'.format(key)
    exc = executable(
        test_name,
        sources: val['sources'] + files('test_util.cc'),
        dependencies: parquet_test_dep,
    )
    test(test_name, exc)
endforeach

parquet_benchmarks = {
    'bloom_filter_benchmark': {
        'sources': files('benchmark_util.cc', 'bloom_filter_benchmark.cc'),
    },
    'column_reader_benchmark': {'sources': files('column_reader_benchmark.cc')},
    'column_io_benchmark': {'sources': files('column_io_benchmark.cc')},
    'encoding_benchmark': {'sources': files('encoding_benchmark.cc')},
    'level_conversion_benchmark': {
        'sources': files('level_conversion_benchmark.cc'),
    },
    'metadata_benchmark': {'sources': files('metadata_benchmark.cc')},
    'page_index_benchmark': {
        'sources': files('benchmark_util.cc', 'page_index_benchmark.cc'),
    },
    'reader_writer_benchmark': {
        'sources': files('arrow/reader_writer_benchmark.cc'),
    },
    'size_stats_benchmark': {'sources': files('arrow/size_stats_benchmark.cc')},
}

parquet_benchmark_dep = [parquet_dep, arrow_benchmark_dep, thrift_dep]

foreach key, val : parquet_benchmarks
    benchmark_name = 'parquet-@0@'.format(key)
    exc = executable(
        benchmark_name,
        sources: val['sources'],
        dependencies: parquet_benchmark_dep,
    )
    benchmark(benchmark_name, exc)
endforeach
