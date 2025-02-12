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
conf_data.set('ARROW_VERSION_STRING', arrow_version)

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

conf_data.set('ARROW_COMPUTE', false)
conf_data.set('ARROW_CSV', false)
conf_data.set('ARROW_CUDA', false)
conf_data.set('ARROW_DATASET', false)
conf_data.set('ARROW_FILESYSTEM', false)
conf_data.set('ARROW_FLIGHT', false)
conf_data.set('ARROW_FLIGHT_SQL', false)
conf_data.set('ARROW_IPC', false)
conf_data.set('ARROW_JEMALLOC', false)
conf_data.set('ARROW_JEMALLOC_VENDORED', false)
conf_data.set('ARROW_JSON', false)
conf_data.set('ARROW_MIMALLOC', false)
conf_data.set('ARROW_ORC', false)
conf_data.set('ARROW_PARQUET', false)
conf_data.set('ARROW_SUBSTRAIT', false)
conf_data.set('ARROW_AZURE', false)
conf_data.set('ARROW_ENABLE_THREADING', false)
conf_data.set('ARROW_GCS', false)
conf_data.set('ARROW_HDFS', false)
conf_data.set('ARROW_S3', false)
conf_data.set('ARROW_USE_GLOG', false)
conf_data.set('ARROW_USE_NATIVE_INT128', false)
conf_data.set('ARROW_WITH_BROTLI', false)
conf_data.set('ARROW_WITH_BZ2', false)
conf_data.set('ARROW_WITH_LZ4', false)
conf_data.set('ARROW_WITH_MUSL', false)
conf_data.set('ARROW_WITH_OPENTELEMETRY', false)
conf_data.set('ARROW_WITH_RE2', false)
conf_data.set('ARROW_WITH_SNAPPY', false)
conf_data.set('ARROW_WITH_UCX', false)
conf_data.set('ARROW_WITH_UTF8PROC', false)
conf_data.set('ARROW_WITH_ZLIB', false)
conf_data.set('ARROW_WITH_ZSTD', false)
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
