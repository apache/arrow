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

# arrow_install_all_headers

conf_data = configuration_data()

version = meson.project_version()
# Remove any pre-release / build identifiers
version_no_pre_release = version.split('-')[0]
version_no_build = version_no_pre_release.split('+')[0]
components = version_no_build.split('.')
assert(
    components.length() >= 3,
    'The version does not contain major, minor and patch',
)
ver_major = components[0]
ver_minor = components[1]
ver_patch = components[2]
conf_data.set('ARROW_VERSION_MAJOR', ver_major)
conf_data.set('ARROW_VERSION_MINOR', ver_minor)
conf_data.set('ARROW_VERSION_PATCH', ver_patch)

conf_data.set('ARROW_VERSION_STRING', version)

# conf_data.set('ARROW_SO_VERSION', ...)
# conf_data.set('ARROW_FULL_SO_VERSION', ...)

# conf_data.set('ARROW_CXX_COMPILER_ID', ...)
# conf_data.set('ARROW_CXX_COMPILER_VERSION', ...)
# conf_data.set('ARROW_CXX_COMPILER_FLAGS', ...)

conf_data.set('UPPERCASE_BUILD_TYPE', get_option('buildtype').to_upper())

# conf_data.set('ARROW_PACKAGE_KIND', ...)

configure_file(
    input: 'config.h.cmake',
    output: 'config.h',
    configuration: conf_data,
    # https://mesonbuild.com/Reference-manual_functions.html#arguments13
    # TODO: need to bridge #cmakedefines somehow
    format: 'cmake@',
    install: true,
    install_dir: '.',
)

internal_conf_data = configuration_data()

# internal_conf_data.set('ARROW_GIT_ID', ...)
# internal_conf_data.set('ARROW_GIT_DESCRIPTION', ...)

configure_file(
    input: 'config_internal.h.cmake',
    output: 'config_internal.h',
    configuration: internal_conf_data,
    # https://mesonbuild.com/Reference-manual_functions.html#arguments13
    # TODO: need to bridge #cmakedefines somehow
    format: 'cmake@',
    install: true,
    install_dir: '.',
)

io_util_test_sources = ['io_util_test.cc']
if meson.get_compiler('cpp').get_id() == 'msvc'
    io_util_test_sources += ['io_util_test.manifest']
elif host_machine.system() == 'windows'
    io_util_test_sources += ['io_util_test.rc']
endif

if needs_tests
    util_tests = [
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
    ] + io_util_test_sources + [
        'iterator_test.cc',
        'list_util_test.cc',
        'logger_test.cc',
        'logging_test.cc',
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
    tests = {

        'arrow-bit-utility-test': {
            'sources': [
                'bit_block_counter_test.cc',
                'bit_util_test.cc',
                'rle_encoding_test.cc',
            ],
        },
    }

    foreach key, val : tests
        exc = executable(
            key,
            sources: val['sources'],
            dependencies: [arrow_test_dep],
            implicit_include_directories: false,  # algorithm.h conflicts with stdlib
        )
        test(key, exc)
    endforeach
endif

# TODO: add_benchmarks
