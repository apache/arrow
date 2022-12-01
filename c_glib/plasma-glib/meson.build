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

warning('Apache Arrow Plasma C GLib is deprecated since 10.0.0. ' +
        'This will be removed from 12.0.0 or so.')

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
  '-D_PLASMA_NO_DEPRECATE',
]
pkg_config_requires = [
  'plasma',
  'arrow-glib',
]
if have_gi
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
  if generate_vapi
    vapi_packages = [
      arrow_glib_vapi,
      'gio-2.0',
    ]
  endif
endif
if arrow_cuda.found()
  dependencies += [arrow_cuda_glib]
  cpp_args += ['-DHAVE_ARROW_CUDA']
  pkg_config_requires += ['arrow-cuda-glib']
  if have_gi
    gir_dependencies += [declare_dependency(sources: arrow_cuda_glib_gir)]
    gir_includes += ['ArrowCUDA-1.0']
    gir_extra_args += ['--include-uninstalled=./arrow-cuda-glib/ArrowCUDA-1.0.gir']
    if generate_vapi
      vapi_packages += [arrow_cuda_glib_vapi]
    endif
  endif
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

pkgconfig.generate(libplasma_glib,
                   description: 'C API for Apache Arrow Plasma based on GLib',
                   filebase: project_name,
                   name: 'Apache Arrow Plasma GLib',
                   requires: pkg_config_requires,
                   variables: pkgconfig_variables,
                   version: version)

if have_gi
  plasma_glib_gir = \
    gnome.generate_gir(libplasma_glib,
                       dependencies: gir_dependencies,
                       export_packages: 'plasma-glib',
                       extra_args: gir_extra_args,
                       fatal_warnings: gi_fatal_warnings,
                       header: 'plasma-glib/plasma-glib.h',
                       identifier_prefix: 'GPlasma',
                       includes: gir_includes,
                       install: true,
                       namespace: 'Plasma',
                       nsversion: api_version,
                       sources: sources + c_headers,
                       symbol_prefix: 'gplasma')

  if generate_vapi
    gnome.generate_vapi('plasma-glib',
                        install: true,
                        packages: vapi_packages,
                        sources: plasma_glib_gir[0])
  endif
endif
