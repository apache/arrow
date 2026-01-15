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
    version: run_command(
        'python',
        '-m',
        'setuptools_scm',
        '--force-write-version-files',
        check: true,
    ).stdout().strip(),
    license: 'Apache-2.0',
    license_files: ['LICENSE.txt'],
    meson_version: '>=1.4.0',
    default_options: ['buildtype=release', 'cpp_std=c++20'],
)

if not get_option('sdist')
    add_languages('cpp', native: false)
    cc = meson.get_compiler('cpp')
endif

py = import('python').find_installation(pure: false)

install_data('NOTICE.txt', install_dir: py.get_install_dir() / 'pyarrow')
# Meson-python suggests using a dist script to resolve symlinks to physical
# files when creating the sdist. See
# https://github.com/mesonbuild/meson-python/discussions/823#discussioncomment-15078932
meson.add_dist_script(
    py,
    meson.project_source_root() / 'scripts/generate_dist.py',
)

if get_option('default_library') == 'static'
    cmake_suffix = 'static'
else
    cmake_suffix = 'shared'
endif

# https://github.com/mesonbuild/meson-python/issues/647
if get_option('sdist')
    arrow_dep = []
else
    arrow_dep = dependency(
        'arrow',
        'Arrow',
        modules: [f'Arrow::arrow_@cmake_suffix@'],
    )
endif

# When NumPy 2.0 becomes the minimum we can remove the
# custom location check
numpy_dep = dependency('numpy', required: false)
if not numpy_dep.found()
    incdir_numpy = run_command(
        py,
        [
            '-c',
            '''import os
import numpy as np
try:
    # Check if include directory is inside the pyarrow dir
    # e.g. a venv created inside the pyarrow dir
    # If so, convert it to a relative path
    incdir = os.path.relpath(np.get_include())
except Exception:
    incdir = np.get_include()
print(incdir)
''',
        ],
        check: true,
    ).stdout().strip()

    numpy_dep = declare_dependency(include_directories: incdir_numpy)
endif

subdir('pyarrow')
