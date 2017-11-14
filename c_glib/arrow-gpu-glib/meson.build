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
  'arrow-gpu-glib.h',
  'cuda.h',
)

cpp_headers = files(
  'arrow-gpu-glib.hpp',
  'cuda.hpp',
)

headers = c_headers + cpp_headers
install_headers(headers, subdir: 'arrow-gpu-glib')


dependencies = [
  arrow_gpu_dependency,
  libarrow_glib_dependency,
]
libarrow_gpu_glib = library('arrow-gpu-glib',
                            sources: sources,
                            install: true,
                            dependencies: dependencies,
                            include_directories: [
                              root_inc,
                            ],
                            soversion: so_version,
                            version: library_version)
libarrow_gpu_glib_dependency = declare_dependency(link_with: libarrow_gpu_glib,
                                                  include_directories: [
                                                    root_inc,
                                                  ],
                                                  dependencies: dependencies)

pkgconfig.generate(filebase: 'arrow-gpu-glib',
                   name: 'Apache Arrow GPU GLib',
                   description: 'C API for Apache Arrow GPU based on GLib',
                   version: version,
                   requires: ['arrow-glib', 'arrow-gpu'],
                   libraries: [libarrow_gpu_glib],
                   subdirs: ['arrow-gpu-glib'])

gnome.generate_gir(libarrow_gpu_glib,
                   dependencies: arrow_glib_gir_dependency,
                   sources: sources + c_headers,
                   namespace: 'ArrowGPU',
                   nsversion: api_version,
                   identifier_prefix: 'GArrowGPU',
                   symbol_prefix: 'garrow_gpu',
                   export_packages: 'arrow-gpu-glib',
                   includes: [
                     'Arrow-1.0',
                   ],
                   install: true,
                   extra_args: [
                     '--warn-all',
                     '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
                   ])
