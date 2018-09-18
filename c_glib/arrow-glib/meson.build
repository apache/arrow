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
  'array-builder.cpp',
  'basic-array.cpp',
  'basic-data-type.cpp',
  'buffer.cpp',
  'chunked-array.cpp',
  'column.cpp',
  'composite-array.cpp',
  'composite-data-type.cpp',
  'decimal.cpp',
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
  'writable.cpp',
  'writable-file.cpp',
)

sources += files(
  'metadata-version.cpp',
  'reader.cpp',
  'writer.cpp',
)

sources += files(
  'compute.cpp',
)

if arrow_orc_dependency.found()
  sources += files(
    'orc-file-reader.cpp',
  )
endif

c_headers = files(
  'array.h',
  'array-builder.h',
  'arrow-glib.h',
  'basic-array.h',
  'basic-data-type.h',
  'buffer.h',
  'chunked-array.h',
  'column.h',
  'composite-array.h',
  'composite-data-type.h',
  'data-type.h',
  'decimal.h',
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
  'writable.h',
  'writable-file.h',
)

c_headers += files(
  'metadata-version.h',
  'reader.h',
  'writer.h',
)

c_headers += files(
  'compute.h',
)

if arrow_orc_dependency.found()
  c_headers += files(
    'orc-file-reader.h',
  )
endif


cpp_headers = files(
  'array.hpp',
  'array-builder.hpp',
  'arrow-glib.hpp',
  'basic-array.hpp',
  'basic-data-type.hpp',
  'buffer.hpp',
  'chunked-array.hpp',
  'column.hpp',
  'data-type.hpp',
  'decimal.hpp',
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
  'writable.hpp',
  'writable-file.hpp',
)

cpp_headers += files(
  'metadata-version.hpp',
  'reader.hpp',
  'writer.hpp',
)

cpp_headers += files(
  'compute.hpp',
)

if arrow_orc_dependency.found()
  cpp_headers += files(
    'orc-file-reader.hpp',
  )
endif

version_h_conf = configuration_data()
version_h_conf.set('GARROW_VERSION_MAJOR', version_major)
version_h_conf.set('GARROW_VERSION_MINOR', version_minor)
version_h_conf.set('GARROW_VERSION_MICRO', version_micro)
version_h_conf.set('GARROW_VERSION_TAG', version_tag)
version_h = configure_file(input: 'version.h.in',
                           output: 'version.h',
                           configuration: version_h_conf)
c_headers += version_h

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
                   requires: ['gio-2.0', 'arrow'],
                   libraries: [libarrow_glib])
if arrow_orc_dependency.found()
  pkgconfig.generate(filebase: 'arrow-orc-glib',
                     name: 'Apache Arrow GLib ORC',
                     description: 'ORC modules for Apache Arrow GLib',
                     version: version,
                     requires: ['arrow-glib'])
endif

arrow_glib_gir = gnome.generate_gir(libarrow_glib,
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
arrow_glib_gir_dependency = declare_dependency(sources: arrow_glib_gir)
