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

sources = files('client.cpp', 'common.cpp', 'server.cpp')

c_headers = files('arrow-flight-glib.h', 'client.h', 'common.h', 'server.h')

cpp_headers = files(
    'arrow-flight-glib.hpp',
    'client.hpp',
    'common.hpp',
    'server.hpp',
)

version_h = configure_file(
    input: 'version.h.in',
    output: 'version.h',
    command: [
        python3,
        generate_version_header_py,
        '--library',
        'GAFLIGHT',
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
install_headers(headers, subdir: 'arrow-flight-glib')

dependencies = [arrow_flight, arrow_glib]
libarrow_flight_glib = library(
    'arrow-flight-glib',
    sources: sources,
    install: true,
    dependencies: dependencies,
    implicit_include_directories: false,
    include_directories: base_include_directories,
    cpp_args: ['-DGAFLIGHT_COMPILATION'],
    soversion: so_version,
    version: library_version,
)
arrow_flight_glib = declare_dependency(
    link_with: libarrow_flight_glib,
    include_directories: base_include_directories,
    dependencies: dependencies,
)

pkgconfig.generate(
    libarrow_flight_glib,
    description: 'C API for Apache Arrow Flight based on GLib',
    filebase: 'arrow-flight-glib',
    name: 'Apache Arrow Flight GLib',
    requires: ['arrow-glib', 'arrow-flight'],
    variables: pkgconfig_variables,
    version: version,
)

if have_gi
    arrow_flight_glib_gir = gnome.generate_gir(
        libarrow_flight_glib,
        dependencies: declare_dependency(sources: arrow_glib_gir),
        export_packages: 'arrow-flight-glib',
        extra_args: gir_scanner_extra_args + [
            '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
        ],
        header: 'arrow-flight-glib/arrow-flight-glib.h',
        identifier_prefix: 'GAFlight',
        includes: ['Arrow-1.0'],
        kwargs: generate_gi_common_args,
        namespace: 'ArrowFlight',
        sources: sources + c_headers,
        symbol_prefix: 'gaflight',
    )

    if generate_vapi
        arrow_flight_glib_vapi = gnome.generate_vapi(
            'arrow-flight-glib',
            install: true,
            packages: [arrow_glib_vapi, 'gio-2.0'],
            sources: [arrow_flight_glib_gir[0]],
        )
    endif
endif
