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

cuda_dep = dependency('cuda', modules: ['cuda'])

arrow_cuda_srcs = [
    'cuda_arrow_ipc.cc',
    'cuda_context.cc',
    'cuda_internal.cc',
    'cuda_memory.cc',
]

arrow_cuda_lib = library(
    'arrow-cuda',
    sources: [arrow_cuda_srcs],
    dependencies: [arrow_dep, cuda_dep, flatbuffers_dep],
    cpp_args: ['-DARROW_CUDA_EXPORTING'],
)

arrow_cuda_dep = declare_dependency(
    link_with: [arrow_cuda_lib],
    dependencies: [cuda_dep],
)

cuda_version = cuda_dep.version()
cuda_split_version = cuda_version.split('.')
cuda_conf_data = configuration_data()
cuda_conf_data.set('CUDA_VERSION_MAJOR', cuda_split_version[0])
cuda_conf_data.set('CUDA_VERSION_MINOR', cuda_split_version[1])

configure_file(
    input: 'cuda_version.h.in',
    output: 'cuda_version.h',
    configuration: cuda_conf_data,
    install_dir: 'arrow/gpu',
)

install_headers(
    [
        'cuda_api.h',
        'cuda_arrow_ipc.h',
        'cuda_context.h',
        'cuda_internal.h',
        'cuda_memory.h',
        'visibility.h',
    ],
    subdir: 'arrow/gpu',
)

exc = executable(
    'arrow-cuda-test',
    sources: ['cuda_test.cc'],
    dependencies: [arrow_test_dep, arrow_cuda_dep],
)
test('arrow-cuda-test', exc)

exc = executable(
    'arrow-cuda-benchmark',
    sources: ['cuda_benchmark.cc'],
    dependencies: [arrow_benchmark_dep, arrow_cuda_dep],
)
benchmark('arrow-gpu-cuda-benchmark', exc)
