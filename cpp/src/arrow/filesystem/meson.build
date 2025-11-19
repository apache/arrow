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

install_headers(
    [
        'api.h',
        'azurefs.h',
        'filesystem.h',
        'filesystem_library.h',
        'gcsfs.h',
        'hdfs.h',
        'localfs.h',
        'mockfs.h',
        'path_util.h',
        's3fs.h',
        's3_test_util.h',
        'test_util.h',
        'type_fwd.h',
    ],
    subdir: 'arrow/filesystem',
)

arrow_filesystem_dep = declare_dependency(
    include_directories: include_directories('.'),
    dependencies: arrow_dep,
)
meson.override_dependency('arrow-filesystem', arrow_filesystem_dep)

pkg.generate(
    filebase: 'arrow-filesystem',
    name: 'Apache Arrow Filesystem',
    description: 'Filesystem API for accessing local and remote filesystems',
    requires: ['arrow'],
)

test_cpp_arg = '-DARROW_FILESYSTEM_EXAMPLE_LIBPATH="@0@"'.format(
    arrow_filesystem_example.full_path(),
)
exc = executable(
    'arrow-filesystem-test',
    sources: ['filesystem_test.cc', 'localfs_test.cc'],
    dependencies: [arrow_test_dep],
    cpp_args: test_cpp_arg,
)
test('arrow-filesystem-test', exc)

exc = executable(
    'arrow-filesystem-localfs-benchmark',
    sources: ['localfs_benchmark.cc'],
    dependencies: [arrow_benchmark_dep],
)
benchmark('arrow-filesystem-localfs-benchmark', exc)

exc = executable(
    'arrow-gcsfs-test',
    sources: ['gcsfs_test.cc'],
    dependencies: [arrow_test_dep, gcs_dep],
)
test('arrow-gcsfs-test', exc)

exc = executable(
    'arrow-azurefs-test',
    sources: ['azurefs_test.cc'],
    dependencies: [arrow_test_dep, azure_dep],
)
test('arrow-azurefs-test', exc)

if needs_hdfs
    exc = executable(
        'arrow-hdfs-test',
        sources: ['hdfs_test.cc'],
        dependencies: [arrow_test_dep],
    )
    test('arrow-hdfs-test', exc)
endif
