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

project_name = 'plasma-glib'

sources = files(
  'client.cpp',
)

c_headers = files(
  'client.h',
  'plasma-glib.h',
)

cpp_headers = files(
  'client.hpp',
  'plasma-glib.hpp',
)

headers = c_headers + cpp_headers
install_headers(headers, subdir: project_name)


dependencies = [
  plasma,
  arrow_glib,
]
libplasma_glib = library('plasma-glib',
                          sources: sources,
                          install: true,
                          dependencies: dependencies,
                          include_directories: base_include_directories,
                          soversion: so_version,
                          version: library_version)
plasma_glib = declare_dependency(link_with: libplasma_glib,
                                 include_directories: base_include_directories,
                                 dependencies: dependencies)

pkgconfig.generate(filebase: project_name,
                   name: 'Apache Arrow Plasma GLib',
                   description: 'C API for Apache Arrow Plasma based on GLib',
                   version: version,
                   requires: ['plasma', 'arrow-glib'],
                   libraries: [libplasma_glib])

gnome.generate_gir(libplasma_glib,
                   dependencies: declare_dependency(sources: arrow_glib_gir),
                   sources: sources + c_headers,
                   namespace: 'Plasma',
                   nsversion: api_version,
                   identifier_prefix: 'GPlasma',
                   symbol_prefix: 'gplasma',
                   export_packages: 'plasma-glib',
                   includes: [
                     'Arrow-1.0',
                   ],
                   install: true,
                   extra_args: [
                     '--warn-all',
                     '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
                   ])
