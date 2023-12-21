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
  'dataset-factory.cpp',
  'dataset.cpp',
  'file-format.cpp',
  'fragment.cpp',
  'partitioning.cpp',
  'scanner.cpp',
)

c_headers = files(
  'arrow-dataset-glib.h',
  'dataset-definition.h',
  'dataset-factory.h',
  'dataset.h',
  'file-format.h',
  'fragment.h',
  'partitioning.h',
  'scanner.h',
)

cpp_headers = files(
  'arrow-dataset-glib.hpp',
  'dataset-factory.hpp',
  'dataset.hpp',
  'file-format.hpp',
  'fragment.hpp',
  'partitioning.hpp',
  'scanner.hpp',
)

enums = gnome.mkenums('enums',
                      sources: c_headers,
                      identifier_prefix: 'GADataset',
                      symbol_prefix: 'gadataset',
                      c_template: 'enums.c.template',
                      h_template: 'enums.h.template',
                      install_dir: join_paths(include_dir, 'arrow-dataset-glib'),
                      install_header: true)
enums_source = enums[0]
enums_header = enums[1]


headers = c_headers + cpp_headers
install_headers(headers, subdir: 'arrow-dataset-glib')

dependencies = [
  arrow_dataset,
  arrow_glib,
]
libarrow_dataset_glib = library('arrow-dataset-glib',
                                sources: sources + enums,
                                install: true,
                                dependencies: dependencies,
                                implicit_include_directories: false,
                                include_directories: base_include_directories,
                                soversion: so_version,
                                version: library_version)
arrow_dataset_glib = declare_dependency(link_with: libarrow_dataset_glib,
                                        include_directories: base_include_directories,
                                        dependencies: dependencies,
                                        sources: enums_header)

pkgconfig.generate(libarrow_dataset_glib,
                   description: 'C API for Apache Arrow Dataset based on GLib',
                   filebase: 'arrow-dataset-glib',
                   name: 'Apache Arrow Dataset GLib',
                   requires: ['arrow-glib', 'arrow-dataset'],
                   variables: pkgconfig_variables,
                   version: version)

if have_gi
  dataset_glib_gir = \
    gnome.generate_gir(libarrow_dataset_glib,
                       dependencies: declare_dependency(sources: arrow_glib_gir),
                       export_packages: 'arrow-dataset-glib',
                       extra_args: [
                         '--warn-all',
                         '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
                       ],
                       fatal_warnings: gi_fatal_warnings,
                       header: 'arrow-dataset-glib/arrow-dataset-glib.h',
                       identifier_prefix: 'GADataset',
                       includes: [
                         'Arrow-1.0',
                       ],
                       install: true,
                       namespace: 'ArrowDataset',
                       nsversion: api_version,
                       sources: sources + c_headers + enums,
                       symbol_prefix: 'gadataset')

  if generate_vapi
    gnome.generate_vapi('arrow-dataset-glib',
                        install: true,
                        packages: [
                          arrow_glib_vapi,
                          'gio-2.0',
                        ],
                        sources: [dataset_glib_gir[0]])
  endif
endif
