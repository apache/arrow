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
  'client.cpp',
  'common.cpp',
  'server.cpp',
)

c_headers = files(
  'arrow-flight-glib.h',
  'client.h',
  'common.h',
  'server.h',
)

cpp_headers = files(
  'arrow-flight-glib.hpp',
  'client.hpp',
  'common.hpp',
  'server.hpp',
)

headers = c_headers + cpp_headers
install_headers(headers, subdir: 'arrow-flight-glib')

dependencies = [
  arrow_flight,
  arrow_glib,
]
libarrow_flight_glib = library('arrow-flight-glib',
                               sources: sources,
                               install: true,
                               dependencies: dependencies,
                               include_directories: base_include_directories,
                               soversion: so_version,
                               version: library_version)
arrow_flight_glib = declare_dependency(link_with: libarrow_flight_glib,
                                       include_directories: base_include_directories,
                                       dependencies: dependencies)

pkgconfig.generate(libarrow_flight_glib,
                   filebase: 'arrow-flight-glib',
                   name: 'Apache Arrow Flight GLib',
                   description: 'C API for Apache Arrow Flight based on GLib',
                   version: version,
                   requires: ['arrow-glib', 'arrow-flight'])

if have_gi
  gnome.generate_gir(libarrow_flight_glib,
                     dependencies: declare_dependency(sources: arrow_glib_gir),
                     sources: sources + c_headers,
                     namespace: 'ArrowFlight',
                     nsversion: api_version,
                     identifier_prefix: 'GAFlight',
                     symbol_prefix: 'gaflight',
                     export_packages: 'arrow-flight-glib',
                     includes: [
                       'Arrow-1.0',
                     ],
                     install: true,
                     extra_args: [
                       '--warn-all',
                       '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
                     ])
endif
