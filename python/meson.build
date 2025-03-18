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
        'python3',
        '-m',
        'setuptools_scm',
        '--force-write-version-files',
        check: true,
    ).stdout().strip(),
    license: 'Apache-2.0',
    meson_version: '>=1.4.0',
    default_options: ['buildtype=release', 'warning_level=0', 'cpp_std=c++17'],
)

arrow_dep = dependency(
    'arrow',
    'Arrow',
    modules: ['Arrow::arrow_shared'],
    required: false,  # optional so sdist will work without finding
)

# TODO: uncomment and fix below implementation to allow the Python subproject
# to build Arrow from source, if not detected on system
'''
arrow_dep = dependency('', required: false)

if get_option('wrap_mode') != 'forcefallback'
    arrow_dep = dependency(
        'arrow',
        'Arrow',
        modules: ['Arrow::arrow_shared'],
        required: false,
    )
endif

if not arrow_dep.found()
    cmake = import('cmake')
    cmake_opt = cmake.subproject_options()

    if get_option('buildtype') == 'debug'
        cmake_build_type = 'Debug'
    elif get_option('buildtype') == 'debugoptimized'
        cmake_build_type = 'RelWithDebInfo'
    else
        cmake_build_type = 'Release'
    endif

    cmake_opt.add_cmake_defines(
        {'CMAKE_BUILD_TYPE': cmake_build_type},
        {'ARROW_ACERO': 'OFF'},  # disabled due to broken COMPUTE module
        {'ARROW_COMPUTE': 'OFF'},  # broken linkage to re2 and utf8proc
        {'ARROW_CSV': 'ON'},
        {'ARROW_DATASET': 'ON'},
        {'ARROW_FILESYSTEM': 'ON'},
        {'ARROW_HDFS': 'ON'},
        {'ARROW_JSON': 'ON'},
        # /ld: cannot find mimalloc_ep/src/mimalloc_ep/lib/mimalloc-2.0/libmimalloc-debug.a: No such file or directory
        {'ARROW_MIMALLOC': 'OFF'},
        {'ARROW_PARQUET': 'OFF'},  # issue with Thrift linkage
        {'ARROW_SUBSTRAIT': 'OFF'},
        {'ARROW_WITH_BROTLI': 'ON'},
        {'ARROW_WITH_BZ2': 'ON'},
        {'ARROW_WITH_LZ4': 'ON'},
        {'ARROW_WITH_RE2': 'ON'},
        {'ARROW_WITH_SNAPPY': 'ON'},
        {'ARROW_WITH_UTF8PROC': 'ON'},
        {'ARROW_WITH_ZLIB': 'ON'},
        {'ARROW_WITH_ZSTD': 'ON'},
        {'ARROW_WITH_RE2': 'OFF'},
        {'ARROW_WITH_UTF8PROC': 'OFF'},
    )

    arrow_proj = cmake.subproject('arrow', options: cmake_opt)

    # static does not work with
    # Can't link non-PIC static library 'arrow_static' into shared library
    # 'lib.cpython-312-x86_64-linux-gnu'. Use the 'pic' option to static_library to build with PIC.
    arrow_dep = arrow_proj.dependency('arrow_shared')
endif
'''

subdir('pyarrow')
