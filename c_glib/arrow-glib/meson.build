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
  'array.cpp',
  'array-builder.cpp',
  'basic-data-type.cpp',
  'buffer.cpp',
  'chunked-array.cpp',
  'column.cpp',
  'composite-data-type.cpp',
  'error.cpp',
  'field.cpp',
  'record-batch.cpp',
  'schema.cpp',
  'table.cpp',
  'table-builder.cpp',
  'tensor.cpp',
  'type.cpp',
)

sources += files(
  'file.cpp',
  'file-mode.cpp',
  'input-stream.cpp',
  'output-stream.cpp',
  'readable.cpp',
  'writeable.cpp',
  'writeable-file.cpp',
)

sources += files(
  'metadata-version.cpp',
  'reader.cpp',
  'writer.cpp',
)

sources += files(
  'compute.cpp',
)

c_headers = files(
  'array.h',
  'array-builder.h',
  'arrow-glib.h',
  'basic-data-type.h',
  'buffer.h',
  'chunked-array.h',
  'column.h',
  'composite-data-type.h',
  'data-type.h',
  'error.h',
  'field.h',
  'gobject-type.h',
  'record-batch.h',
  'schema.h',
  'table.h',
  'table-builder.h',
  'tensor.h',
  'type.h',
)


c_headers += files(
  'file.h',
  'file-mode.h',
  'input-stream.h',
  'output-stream.h',
  'readable.h',
  'writeable.h',
  'writeable-file.h',
)

c_headers += files(
  'metadata-version.h',
  'reader.h',
  'writer.h',
)

c_headers += files(
  'compute.h',
)


cpp_headers = files(
  'array.hpp',
  'array-builder.hpp',
  'arrow-glib.hpp',
  'basic-data-type.hpp',
  'buffer.hpp',
  'chunked-array.hpp',
  'column.hpp',
  'data-type.hpp',
  'error.hpp',
  'field.hpp',
  'record-batch.hpp',
  'schema.hpp',
  'table.hpp',
  'table-builder.hpp',
  'tensor.hpp',
  'type.hpp',
)

cpp_headers += files(
  'file.hpp',
  'file-mode.hpp',
  'input-stream.hpp',
  'output-stream.hpp',
  'readable.hpp',
  'writeable.hpp',
  'writeable-file.hpp',
)

cpp_headers += files(
  'metadata-version.hpp',
  'reader.hpp',
  'writer.hpp',
)

cpp_headers += files(
  'compute.hpp',
)


enums = gnome.mkenums('enums',
                      sources: c_headers,
                      identifier_prefix: 'GArrow',
                      symbol_prefix: 'garrow',
                      c_template: 'enums.c.template',
                      h_template: 'enums.h.template',
                      install_dir: join_paths(include_dir, meson.project_name()),
                      install_header: true)
enums_source = enums[0]
enums_header = enums[1]


headers = c_headers + cpp_headers
install_headers(headers, subdir: meson.project_name())


dependencies = [
  dependency('arrow'),
  dependency('gobject-2.0'),
  dependency('gio-2.0'),
]
libarrow_glib = library('arrow-glib',
                        sources: sources + enums,
                        install: true,
                        dependencies: dependencies,
                        include_directories: [
                          root_inc,
                        ],
                       soversion: so_version,
                       version: library_version)
libarrow_glib_dependency = declare_dependency(link_with: libarrow_glib,
                                              include_directories: [
                                                root_inc,
                                              ],
                                              dependencies: dependencies,
                                              sources: enums_header)

pkgconfig.generate(filebase: meson.project_name(),
                   name: 'Apache Arrow GLib',
                   description: 'C API for Apache Arrow based on GLib',
                   version: version,
                   requires: ['gobject-2.0', 'arrow'],
                   libraries: [libarrow_glib],
                   subdirs: ['arrow-glib'])

gnome.generate_gir(libarrow_glib,
                   sources: sources + c_headers + enums,
                   namespace: 'Arrow',
                   nsversion: api_version,
                   identifier_prefix: 'GArrow',
                   symbol_prefix: 'garrow',
                   export_packages: 'arrow-glib',
                   includes: [
                     'GObject-2.0',
                     'Gio-2.0',
                   ],
                   install: true,
                   extra_args: [
                     '--warn-all',
                   ])
