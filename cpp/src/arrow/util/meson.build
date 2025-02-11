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

# TODO: While the CMake configuration substitutes the @ARROW_VERSION@
# macro directly in the configuration file, the Meson wrapper to the
# CMake configuration file seems to still want to include that
# variable as part of the configuration data
arrow_version = ver_major.to_int() * 1000 + ver_minor.to_int() * 1000 + ver_patch.to_int()
conf_data.set('ARROW_VERSION', arrow_version)
conf_data.set('ARROW_VERSION_STRING', arrow_version)

arrow_so_version = ver_major.to_int() * 100 + ver_minor.to_int()
conf_data.set('ARROW_SO_VERSION', arrow_so_version)
arrow_full_so_version = '@0@.@1@.@2@'.format(arrow_so_version, ver_patch, 0)
conf_data.set('ARROW_FULL_SO_VERSION', arrow_full_so_version)

# TODO: we may want to give Meson its own configuration file rather
# than backfitting onto CMake's
conf_data.set('CMAKE_CXX_COMPILER_ID', cpp_compiler.get_id())
conf_data.set('CMAKE_CXX_COMPILER_VERSION', cpp_compiler.version())
conf_data.set(
    'CMAKE_CXX_FLAGS',
    ' '.join(cpp_compiler.get_supported_arguments()),
)

conf_data.set('UPPERCASE_BUILD_TYPE', get_option('buildtype').to_upper())

conf_data.set('ARROW_PACKAGE_KIND', get_option('package_kind'))

foreach cmakedefine : [
    'ARROW_COMPUTE',
    'ARROW_CSV',
    'ARROW_CUDA',
    'ARROW_DATASET',
    'ARROW_FILESYSTEM',
    'ARROW_FLIGHT',
    'ARROW_FLIGHT_SQL',
    'ARROW_IPC',
    'ARROW_JEMALLOC',
    'ARROW_JEMALLOC_VENDORED',
    'ARROW_JSON',
    'ARROW_MIMALLOC',
    'ARROW_ORC',
    'ARROW_PARQUET',
    'ARROW_SUBSTRAIT',
    'ARROW_AZURE',
    'ARROW_ENABLE_THREADING',
    'ARROW_GCS',
    'ARROW_HDFS',
    'ARROW_S3',
    'ARROW_USE_GLOG',
    'ARROW_USE_NATIVE_INT128',
    'ARROW_WITH_BROTLI',
    'ARROW_WITH_BZ2',
    'ARROW_WITH_LZ4',
    'ARROW_WITH_MUSL',
    'ARROW_WITH_OPENTELEMETRY',
    'ARROW_WITH_RE2',
    'ARROW_WITH_SNAPPY',
    'ARROW_WITH_UCX',
    'ARROW_WITH_UTF8PROC',
    'ARROW_WITH_ZLIB',
    'ARROW_WITH_ZSTD',
    'PARQUET_REQUIRE_ENCRYPTION',
]
    conf_data.set(cmakedefine, false)
endforeach

configure_file(
    input: 'config.h.cmake',
    output: 'config.h',
    configuration: conf_data,
    format: 'cmake@',
    install: true,
    install_dir: '.',
)

internal_conf_data = configuration_data()

internal_conf_data.set('ARROW_GIT_ID', git_id)
internal_conf_data.set('ARROW_GIT_DESCRIPTION', git_description)

configure_file(
    input: 'config_internal.h.cmake',
    output: 'config_internal.h',
    configuration: internal_conf_data,
    format: 'cmake@',
    install: true,
    install_dir: '.',
)
