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

version = run_command('python', 'tool/get-version.py').stdout().strip()
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
so_version = 0
library_version = '@0@.@1@.@2@'.format(so_version, 0, 0)

prefix = get_option('prefix')
include_dir = join_paths(prefix, get_option('includedir'))
data_dir = join_paths(prefix, get_option('datadir'))

gnome = import('gnome')
pkgconfig = import('pkgconfig')

root_inc = include_directories('.')

subdir('arrow-glib')
subdir('example')

if get_option('enable_gtk_doc')
  subdir('doc/reference')
endif

run_test = find_program('test/run-test.sh')
test('unit test',
     run_test,
     env: ['ARROW_GLIB_TYPELIB_DIR=@0@/arrow-glib'.format(meson.build_root())])
