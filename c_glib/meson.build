# -*- indent-tabs-mode: nil -*-
#
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

project(
    'arrow-glib',
    'c',
    'cpp',
    default_options: ['c_std=c99', 'cpp_std=c++17'],
    license: 'Apache-2.0',
    # Debian:
    #   https://packages.debian.org/search?keywords=meson
    #
    #   * bookworm: 1.0.0
    #
    # Ubuntu:
    #   https://packages.ubuntu.com/search?keywords=meson
    #
    #   * 22.04: 0.61.2
    #   * 24.04: 1.3.2
    meson_version: '>=0.61.2',
    version: '21.0.0-SNAPSHOT',
)

version = meson.project_version()
if version.endswith('-SNAPSHOT')
    version_numbers = version.split('-')[0].split('.')
    version_tag = version.split('-')[1]
else
    version_numbers = version.split('.')
    version_tag = ''
endif
version_major = version_numbers[0].to_int()
version_minor = version_numbers[1].to_int()
version_micro = version_numbers[2].to_int()
version_no_snapshot = '.'.join(version_numbers)

api_version = '1.0'
so_version = version_major * 100 + version_minor
so_version_patch = version_micro
library_version = '@0@.@1@.@2@'.format(so_version, so_version_patch, 0)

project_build_root = meson.project_build_root()
project_source_root = meson.project_source_root()

prefix = get_option('prefix')
include_dir = join_paths(prefix, get_option('includedir'))
data_dir = join_paths(prefix, get_option('datadir'))
doc_dir = join_paths(data_dir, 'doc')
gir_dir = join_paths(data_dir, 'gir-1.0')
vapi_dir = join_paths(data_dir, 'vala', 'vapi')

gnome = import('gnome')
pkgconfig = import('pkgconfig')
pkgconfig_variables = []

base_include_directories = [include_directories('.')]

generate_gi_common_args = {'install': true, 'nsversion': api_version}
if get_option('werror')
    generate_gi_common_args += {'fatal_warnings': true}
endif
have_gi = dependency('gobject-introspection-1.0', required: false).found()
if have_gi
    pkgconfig_variables += ['girdir=@0@'.format(gir_dir)]
endif
generate_vapi = have_gi and get_option('vapi')
if generate_vapi
    pkgconfig_variables += ['vapidir=@0@'.format(vapi_dir)]
    add_languages('vala', native: false)
endif

arrow_cpp_build_dir = get_option('arrow_cpp_build_dir')
arrow_cpp_build_type = get_option('arrow_cpp_build_type')
if arrow_cpp_build_dir == ''
    arrow_cpp_build_lib_dir = ''
else
    arrow_cpp_build_lib_dir = join_paths(
        project_source_root,
        arrow_cpp_build_dir,
        arrow_cpp_build_type.to_lower(),
    )
endif

if arrow_cpp_build_lib_dir == ''
    common_args = {'version': [f'>=@version_no_snapshot@']}
    arrow = dependency(
        'arrow',
        'Arrow',
        kwargs: common_args,
        modules: ['Arrow::arrow_shared'],
    )
    # They are just for checking required modules are enabled. They are built into
    # libarrow.so. So we don't need additional build flags for them.
    if arrow.type_name() == 'cmake'
        assert(
            arrow.get_variable('ARROW_COMPUTE', default_value: 'OFF') == 'ON',
            'compute module must be enabled',
        )
        assert(
            arrow.get_variable('ARROW_CSV', default_value: 'OFF') == 'ON',
            'CSV module must be enabled',
        )
        assert(
            arrow.get_variable('ARROW_FILESYSTEM', default_value: 'OFF') == 'ON',
            'filesystem module must be enabled',
        )
        assert(
            arrow.get_variable('ARROW_JSON', default_value: 'OFF') == 'ON',
            'JSON module must be enabled',
        )
    else
        dependency('arrow-compute', kwargs: common_args)
        dependency('arrow-csv', kwargs: common_args)
        dependency('arrow-filesystem', kwargs: common_args)
        dependency('arrow-json', kwargs: common_args)
    endif

    if arrow.type_name() == 'cmake'
        have_arrow_orc = (arrow.get_variable('ARROW_ORC', default_value: 'OFF') == 'ON')
    else
        have_arrow_orc = dependency(
            'arrow-orc',
            kwargs: common_args,
            required: false,
        ).found()
    endif
    arrow_cuda = dependency(
        'arrow-cuda',
        'ArrowCUDA',
        kwargs: common_args,
        modules: ['ArrowCUDA::arrow_cuda_shared'],
        required: false,
    )
    # we do not support compiling GLib without Compute and Acero engine
    arrow_compute = dependency(
        'arrow-compute',
        'ArrowCompute',
        kwargs: common_args,
        modules: ['ArrowCompute::arrow_compute_shared'],
    )
    arrow_acero = dependency(
        'arrow-acero',
        'ArrowAcero',
        kwargs: common_args,
        modules: ['ArrowAcero::arrow_acero_shared'],
    )
    arrow_dataset = dependency(
        'arrow-dataset',
        'ArrowDataset',
        kwargs: common_args,
        modules: ['ArrowDataset::arrow_dataset_shared'],
        required: false,
    )
    arrow_flight = dependency(
        'arrow-flight',
        'ArrowFlight',
        kwargs: common_args,
        modules: ['ArrowFlight::arrow_flight_shared'],
        required: false,
    )
    arrow_flight_sql = dependency(
        'arrow-flight-sql',
        'ArrowFlightSql',
        kwargs: common_args,
        modules: ['ArrowFlightSql::arrow_flight_sql_shared'],
        required: false,
    )
    gandiva = dependency(
        'gandiva',
        'Gandiva',
        kwargs: common_args,
        modules: ['Gandiva::gandiva_shared'],
        required: false,
    )
    parquet = dependency(
        'parquet',
        'Parquet',
        kwargs: common_args,
        modules: ['Parquet::parquet_shared'],
    )
else
    base_include_directories += [
        include_directories(join_paths(arrow_cpp_build_dir, 'src')),
        include_directories('../cpp/src'),
    ]
    cpp_compiler = meson.get_compiler('cpp')
    arrow = cpp_compiler.find_library('arrow', dirs: [arrow_cpp_build_lib_dir])
    arrow_orc_code = '''
#include <arrow/adapters/orc/adapter.h>

int
main(void)
{
  arrow::adapters::orc::ORCFileWriter::Open(nullptr);
  return 0;
}
  '''
    have_arrow_orc = cpp_compiler.links(
        arrow_orc_code,
        include_directories: base_include_directories,
        dependencies: [arrow],
    )
    arrow_cuda = cpp_compiler.find_library(
        'arrow_cuda',
        dirs: [arrow_cpp_build_lib_dir],
        required: false,
    )
    arrow_compute = cpp_compiler.find_library(
        'arrow_compute',
        dirs: [arrow_cpp_build_lib_dir],
        required: true,
    )
    arrow_acero = cpp_compiler.find_library(
        'arrow_acero',
        dirs: [arrow_cpp_build_lib_dir],
        required: true,
    )
    arrow_dataset = cpp_compiler.find_library(
        'arrow_dataset',
        dirs: [arrow_cpp_build_lib_dir],
        required: false,
    )
    arrow_flight = cpp_compiler.find_library(
        'arrow_flight',
        dirs: [arrow_cpp_build_lib_dir],
        required: false,
    )
    arrow_flight_sql = cpp_compiler.find_library(
        'arrow_flight_sql',
        dirs: [arrow_cpp_build_lib_dir],
        required: false,
    )
    gandiva = cpp_compiler.find_library(
        'gandiva',
        dirs: [arrow_cpp_build_lib_dir],
        required: false,
    )
    parquet = cpp_compiler.find_library(
        'parquet',
        dirs: [arrow_cpp_build_lib_dir],
        required: false,
    )
endif

cxx = meson.get_compiler('cpp')
cxx_flags = []
if cxx.get_id() != 'msvc'
    cxx_flags += ['-Wmissing-declarations']
endif
add_project_arguments(cxx.get_supported_arguments(cxx_flags), language: 'cpp')

python = import('python')
python3 = python.find_installation('python3')
generate_version_header_py = project_source_root / 'tool' / 'generate-version-header.py'

c_compiler = meson.get_compiler('c')
# Specify -fPIE explicitly for g-ir-scanner because PIE is disabled by
# default on AlmaLinux 9 RPM build by
# LDFLAGS="-specs=/usr/lib/rpm/redhat/redhat-hardened-ld".
gir_scanner_cflags = c_compiler.get_supported_arguments('-fPIE')
if gir_scanner_cflags.length() == 0
    gir_scanner_extra_args = []
else
    gir_scanner_extra_args = ['--cflags-begin'] + gir_scanner_cflags + [
        '--cflags-end',
    ]
endif

subdir('arrow-glib')
if arrow_cuda.found()
    subdir('arrow-cuda-glib')
endif
if arrow_dataset.found()
    subdir('arrow-dataset-glib')
endif
if arrow_flight.found()
    subdir('arrow-flight-glib')
endif
if arrow_flight_sql.found()
    subdir('arrow-flight-sql-glib')
endif
if gandiva.found()
    subdir('gandiva-glib')
endif
if parquet.found()
    subdir('parquet-glib')
endif
subdir('example')

if get_option('doc')
    subdir('doc')
endif

install_data(
    '../LICENSE.txt',
    'README.md',
    install_dir: data_dir / meson.project_name(),
)

run_test = find_program('test/run-test.sh')
test(
    'unit test',
    run_test,
    env: [
        'ARROW_CUDA_GLIB_TYPELIB_DIR=@0@/arrow-cuda-glib'.format(
            project_build_root,
        ),
        'ARROW_DATASET_GLIB_TYPELIB_DIR=@0@/arrow-dataset-glib'.format(
            project_build_root,
        ),
        'ARROW_FLIGHT_GLIB_TYPELIB_DIR=@0@/arrow-flight-glib'.format(
            project_build_root,
        ),
        'ARROW_FLIGHT_SQL_GLIB_TYPELIB_DIR=@0@/arrow-flight-sql-glib'.format(
            project_build_root,
        ),
        'ARROW_GLIB_TYPELIB_DIR=@0@/arrow-glib'.format(project_build_root),
        'GANDIVA_GLIB_TYPELIB_DIR=@0@/gandiva-glib'.format(project_build_root),
        'PARQUET_GLIB_TYPELIB_DIR=@0@/parquet-glib'.format(project_build_root),
    ],
)
