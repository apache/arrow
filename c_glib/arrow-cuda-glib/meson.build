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

sources = files(
  'cuda.cpp',
)

c_headers = files(
  'arrow-cuda-glib.h',
  'cuda.h',
)

cpp_headers = files(
  'arrow-cuda-glib.hpp',
  'cuda.hpp',
)

headers = c_headers + cpp_headers
install_headers(headers, subdir: 'arrow-cuda-glib')


dependencies = [
  arrow_cuda,
  arrow_glib,
]
libarrow_cuda_glib = library('arrow-cuda-glib',
                             sources: sources,
                             install: true,
                             dependencies: dependencies,
                             include_directories: base_include_directories,
                             soversion: so_version,
                             version: library_version)
arrow_cuda_glib = declare_dependency(link_with: libarrow_cuda_glib,
                                     include_directories: base_include_directories,
                                     dependencies: dependencies)

pkgconfig.generate(filebase: 'arrow-cuda-glib',
                   name: 'Apache Arrow CUDA GLib',
                   description: 'C API for Apache Arrow CUDA based on GLib',
                   version: version,
                   requires: ['arrow-glib', 'arrow-cuda'],
                   libraries: [libarrow_cuda_glib])

gir_dependencies = [
  declare_dependency(sources: arrow_glib_gir),
]
gir_extra_args = [
  '--warn-all',
  '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
]
arrow_cuda_glib_gir = gnome.generate_gir(libarrow_cuda_glib,
                                         dependencies: gir_dependencies,
                                         sources: sources + c_headers,
                                         namespace: 'ArrowCUDA',
                                         nsversion: api_version,
                                         identifier_prefix: 'GArrowCUDA',
                                         symbol_prefix: 'garrow_cuda',
                                         export_packages: 'arrow-cuda-glib',
                                         includes: [
                                           'Arrow-1.0',
                                         ],
                                         install: true,
                                         extra_args: gir_extra_args)
