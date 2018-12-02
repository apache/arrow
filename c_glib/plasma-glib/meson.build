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
  'object.cpp',
)

c_headers = files(
  'client.h',
  'object.h',
  'plasma-glib.h',
)

cpp_headers = files(
  'client.hpp',
  'object.hpp',
  'plasma-glib.hpp',
)

headers = c_headers + cpp_headers
install_headers(headers, subdir: project_name)


dependencies = [
  plasma,
  arrow_glib,
]
cpp_args = [
  '-DG_LOG_DOMAIN="Plasma"',
]
pkg_config_requires = [
  'plasma',
  'arrow-glib',
]
gir_dependencies = [
  declare_dependency(sources: arrow_glib_gir),
]
gir_includes = [
  'Arrow-1.0',
]
gir_extra_args = [
  '--warn-all',
  '--include-uninstalled=./arrow-glib/Arrow-1.0.gir',
]
if arrow_gpu.found()
  dependencies += [arrow_gpu_glib]
  cpp_args += ['-DHAVE_ARROW_GPU']
  pkg_config_requires += ['arrow-gpu-glib']
  gir_dependencies += [declare_dependency(sources: arrow_gpu_glib_gir)]
  gir_includes += ['ArrowGPU-1.0']
  gir_extra_args += ['--include-uninstalled=./arrow-gpu-glib/ArrowGPU-1.0.gir']
endif
libplasma_glib = library('plasma-glib',
                         sources: sources,
                         install: true,
                         dependencies: dependencies,
                         include_directories: base_include_directories,
                         cpp_args: cpp_args,
                         soversion: so_version,
                         version: library_version)
plasma_glib = declare_dependency(link_with: libplasma_glib,
                                 include_directories: base_include_directories,
                                 dependencies: dependencies)

pkgconfig.generate(filebase: project_name,
                   name: 'Apache Arrow Plasma GLib',
                   description: 'C API for Apache Arrow Plasma based on GLib',
                   version: version,
                   requires: pkg_config_requires,
                   libraries: [libplasma_glib])

gnome.generate_gir(libplasma_glib,
                   dependencies: gir_dependencies,
                   sources: sources + c_headers,
                   namespace: 'Plasma',
                   nsversion: api_version,
                   identifier_prefix: 'GPlasma',
                   symbol_prefix: 'gplasma',
                   export_packages: 'plasma-glib',
                   includes: gir_includes,
                   install: true,
                   extra_args: gir_extra_args)
