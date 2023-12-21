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
  'server.cpp',
)

c_headers = files(
  'arrow-flight-sql-glib.h',
  'client.h',
  'server.h',
)

cpp_headers = files(
  'arrow-flight-sql-glib.hpp',
  'client.hpp',
  'server.hpp',
)

headers = c_headers + cpp_headers
install_headers(headers, subdir: 'arrow-flight-sql-glib')

dependencies = [
  arrow_flight_sql,
  arrow_flight_glib,
]
libarrow_flight_sql_glib = library('arrow-flight-sql-glib',
                                   sources: sources,
                                   install: true,
                                   dependencies: dependencies,
                                   implicit_include_directories: false,
                                   include_directories: base_include_directories,
                                   soversion: so_version,
                                   version: library_version)
arrow_flight_sql_glib = \
  declare_dependency(link_with: libarrow_flight_sql_glib,
                     include_directories: base_include_directories,
                     dependencies: dependencies)

pkgconfig.generate(libarrow_flight_sql_glib,
                   description: 'C API for Apache Arrow Flight SQL based on GLib',
                   filebase: 'arrow-flight-sql-glib',
                   name: 'Apache Arrow Flight SQL GLib',
                   requires: ['arrow-flight-glib', 'arrow-flight-sql'],
                   variables: pkgconfig_variables,
                   version: version)

if have_gi
  flight_sql_glib_gir_dependencies = \
    declare_dependency(sources: [arrow_glib_gir, flight_glib_gir])
  flight_sql_glib_gir = \
    gnome.generate_gir(libarrow_flight_sql_glib,
                       dependencies: flight_sql_glib_gir_dependencies,
                       export_packages: 'arrow-flight-sql-glib',
                       extra_args: [
                         '--warn-all',
                         '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
                         '--include-uninstalled=./arrow-flight-glib/ArrowFlight-1.0.gir',
                       ],
                       fatal_warnings: gi_fatal_warnings,
                       header: 'arrow-flight-sql-glib/arrow-flight-sql-glib.h',
                       identifier_prefix: 'GAFlightSQL',
                       includes: [
                         'Arrow-1.0',
                         'ArrowFlight-1.0',
                       ],
                       install: true,
                       namespace: 'ArrowFlightSQL',
                       nsversion: api_version,
                       sources: sources + c_headers,
                       symbol_prefix: 'gaflightsql')

  if generate_vapi
    gnome.generate_vapi('arrow-flight-sql-glib',
                        install: true,
                        packages: [
                          flight_glib_vapi,
                          arrow_glib_vapi,
                          'gio-2.0',
                        ],
                        sources: [flight_sql_glib_gir[0]])
  endif
endif
