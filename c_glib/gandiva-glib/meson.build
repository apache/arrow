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

project_name = 'gandiva-glib'

sources = files(
  'expression.cpp',
  'function-registry.cpp',
  'function-signature.cpp',
  'node.cpp',
  'native-function.cpp',
  'projector.cpp',
)

c_headers = files(
  'expression.h',
  'function-registry.h',
  'function-signature.h',
  'gandiva-glib.h',
  'node.h',
  'native-function.h',
  'projector.h',
)

cpp_headers = files(
  'expression.hpp',
  'function-signature.hpp',
  'gandiva-glib.hpp',
  'node.hpp',
  'native-function.hpp',
  'projector.hpp',
)

enums = gnome.mkenums('enums',
                      sources: c_headers,
                      identifier_prefix: 'GGandiva',
                      symbol_prefix: 'ggandiva',
                      c_template: 'enums.c.template',
                      h_template: 'enums.h.template',
                      install_dir: join_paths(include_dir, meson.project_name()),
                      install_header: true)
enums_source = enums[0]
enums_header = enums[1]

headers = c_headers + cpp_headers
install_headers(headers, subdir: project_name)

dependencies = [
  gandiva,
  arrow_glib,
]
libgandiva_glib = library('gandiva-glib',
                          sources: sources + enums,
                          install: true,
                          dependencies: dependencies,
                          include_directories: base_include_directories,
                          soversion: so_version,
                          version: library_version)
gandiva_glib = declare_dependency(link_with: libgandiva_glib,
                                  include_directories: base_include_directories,
                                  dependencies: dependencies,
                                  sources: enums_header)

pkgconfig.generate(libgandiva_glib,
                   filebase: project_name,
                   name: 'Apache Arrow Gandiva GLib',
                   description: 'C API for Apache Arrow Gandiva based on GLib',
                   version: version,
                   requires: ['gandiva', 'arrow-glib'])

gnome.generate_gir(libgandiva_glib,
                   dependencies: declare_dependency(sources: arrow_glib_gir),
                   sources: sources + c_headers + enums,
                   namespace: 'Gandiva',
                   nsversion: api_version,
                   identifier_prefix: 'GGandiva',
                   symbol_prefix: 'ggandiva',
                   export_packages: 'gandiva-glib',
                   includes: [
                     'Arrow-1.0'
                   ],
                   install: true,
                   extra_args: [
                     '--warn-all',
                     '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
                   ])
