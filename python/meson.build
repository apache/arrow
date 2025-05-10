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
    'pyarrow',
    'cython',
    'cpp',
    version: run_command(
        'python',
        '-m',
        'setuptools_scm',
        '--force-write-version-files',
        check: true,
    ).stdout().strip(),
    license: 'Apache-2.0',
    license_files: ['../LICENSE.txt'],
    meson_version: '>=1.4.0',
    default_options: ['buildtype=release', 'cpp_std=c++17'],
)

install_data('../NOTICE.txt', install_dir: py.get_install_dir() / 'pyarrow')

arrow_dep = dependency(
    'arrow',
    'Arrow',
    modules: ['Arrow::arrow_shared'],
    required: false,  # optional so sdist will work without finding
)

cc = meson.get_compiler('cpp')

subdir('pyarrow')
