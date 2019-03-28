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

project('arrow-glib', 'c', 'cpp',
        license: 'Apache-2.0',
        default_options: [
          'cpp_std=c++11',
        ])

version = '0.13.0'
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

api_version = '1.0'
so_version = version_minor
so_version_patch = version_micro
library_version = '@0@.@1@.@2@'.format(so_version, so_version_patch, 0)

prefix = get_option('prefix')
include_dir = join_paths(prefix, get_option('includedir'))
data_dir = join_paths(prefix, get_option('datadir'))

gnome = import('gnome')
pkgconfig = import('pkgconfig')

base_include_directories = [
  include_directories('.')
]

arrow_cpp_build_dir = get_option('arrow_cpp_build_dir')
arrow_cpp_build_type = get_option('arrow_cpp_build_type')
if arrow_cpp_build_dir == ''
  arrow_cpp_build_lib_dir = ''
else
  arrow_cpp_build_lib_dir = join_paths(arrow_cpp_build_dir,
                                       arrow_cpp_build_type.to_lower())
endif

if arrow_cpp_build_lib_dir == ''
  arrow = dependency('arrow')
  have_arrow_orc = dependency('arrow-orc', required: false).found()
  arrow_cuda = dependency('arrow-cuda', required: false)
  gandiva = dependency('gandiva', required: false)
  parquet = dependency('parquet', required: false)
  plasma = dependency('plasma', required: false)
else
  base_include_directories += [
    include_directories(join_paths(arrow_cpp_build_dir, 'src')),
    include_directories('../cpp/src'),
  ]
  cpp_compiler = meson.get_compiler('cpp')
  arrow = cpp_compiler.find_library('arrow',
                                    dirs: [arrow_cpp_build_lib_dir])
  arrow_orc_code = '''
#include <arrow/adapters/orc/adapter.h>

int
main(void)
{
  arrow::adapters::orc::ORCFileReader::Open(nullptr, nullptr, nullptr);
  return 0;
}
  '''
  have_arrow_orc = cpp_compiler.links(arrow_orc_code,
                                      include_directories: base_include_directories,
                                      dependencies: [arrow])
  arrow_cuda = cpp_compiler.find_library('arrow_cuda',
                                         dirs: [arrow_cpp_build_lib_dir],
                                         required: false)
  gandiva = cpp_compiler.find_library('gandiva',
                                      dirs: [arrow_cpp_build_lib_dir],
                                      required: false)
  parquet = cpp_compiler.find_library('parquet',
                                      dirs: [arrow_cpp_build_lib_dir],
                                      required: false)
  plasma = cpp_compiler.find_library('plasma',
                                      dirs: [arrow_cpp_build_lib_dir],
                                      required: false)
endif

subdir('arrow-glib')
if arrow_cuda.found()
  subdir('arrow-cuda-glib')
endif
if gandiva.found()
  subdir('gandiva-glib')
endif
if parquet.found()
  subdir('parquet-glib')
endif
if plasma.found()
  subdir('plasma-glib')
endif
subdir('example')

if get_option('gtk_doc')
  subdir('doc/arrow-glib')
  if gandiva.found()
    subdir('doc/gandiva-glib')
  endif
  if parquet.found()
    subdir('doc/parquet-glib')
  endif
  if plasma.found()
    subdir('doc/plasma-glib')
  endif
endif

run_test = find_program('test/run-test.sh')
test('unit test',
     run_test,
     env: [
       'ARROW_GLIB_TYPELIB_DIR=@0@/arrow-glib'.format(meson.build_root()),
       'ARROW_CUDA_GLIB_TYPELIB_DIR=@0@/arrow-cuda-glib'.format(meson.build_root()),
       'GANDIVA_GLIB_TYPELIB_DIR=@0@/gandiva-glib'.format(meson.build_root()),
       'PARQUET_GLIB_TYPELIB_DIR=@0@/parquet-glib'.format(meson.build_root()),
       'PARQUET_GLIB_TYPELIB_DIR=@0@/plasma-glib'.format(meson.build_root()),
     ])
