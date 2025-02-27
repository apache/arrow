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

sources = files('cuda.cpp')

c_headers = files('arrow-cuda-glib.h', 'cuda.h')

cpp_headers = files('arrow-cuda-glib.hpp', 'cuda.hpp')

version_h = configure_file(
    input: 'version.h.in',
    output: 'version.h',
    command: [
        python3,
        generate_version_header_py,
        '--library',
        'GARROW_CUDA',
        '--version',
        version,
        '--input',
        '@INPUT@',
        '--output',
        '@OUTPUT@',
    ],
)

c_headers += version_h

headers = c_headers + cpp_headers
install_headers(headers, subdir: 'arrow-cuda-glib')

dependencies = [arrow_cuda, arrow_glib]
libarrow_cuda_glib = library(
    'arrow-cuda-glib',
    sources: sources,
    install: true,
    dependencies: dependencies,
    implicit_include_directories: false,
    include_directories: base_include_directories,
    cpp_args: ['-DGARROW_CUDA_COMPILATION'],
    soversion: so_version,
    version: library_version,
)
arrow_cuda_glib = declare_dependency(
    link_with: libarrow_cuda_glib,
    include_directories: base_include_directories,
    dependencies: dependencies,
)
if target_machine.system() != 'windows'
    pkgconfig.generate(
        libarrow_cuda_glib,
        description: 'C API for Apache Arrow CUDA based on GLib',
        filebase: 'arrow-cuda-glib',
        name: 'Apache Arrow CUDA GLib',
        requires: ['arrow-glib', 'arrow-cuda'],
        variables: pkgconfig_variables,
        version: version,
    )
endif

if have_gi
    gir_dependencies = [declare_dependency(sources: arrow_glib_gir)]
    gir_extra_args = [
        '--warn-all',
        '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
    ]
    arrow_cuda_glib_gir = gnome.generate_gir(
        libarrow_cuda_glib,
        dependencies: gir_dependencies,
        export_packages: 'arrow-cuda-glib',
        extra_args: gir_extra_args,
        header: 'arrow-cuda-glib/arrow-cuda-glib.h',
        identifier_prefix: 'GArrowCUDA',
        includes: ['Arrow-1.0'],
        kwargs: generate_gi_common_args,
        namespace: 'ArrowCUDA',
        sources: sources + c_headers,
        symbol_prefix: 'garrow_cuda',
    )

    if generate_vapi
        arrow_cuda_glib_vapi = gnome.generate_vapi(
            'arrow-cuda-glib',
            install: true,
            packages: [arrow_glib_vapi, 'gio-2.0'],
            sources: [arrow_cuda_glib_gir[0]],
        )
    endif
endif
