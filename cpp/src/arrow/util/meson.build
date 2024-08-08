# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

version = meson.project_version()
# Remove any pre-release / build identifiers
version_no_pre_release = version.split('-')[0]
version_no_build = version_no_pre_release.split('+')[0]
components = version_no_build.split('.')
assert(components.length() >= 3,
       'The version does not contain major, minor and patch')
ver_major = components[0]
ver_minor = components[1]
ver_patch = components[2]

conf_data = configuration_data()

conf_data.set('ARROW_VERSION', version)
conf_data.set('ARROW_VERSION_MAJOR', ver_major)
conf_data.set('ARROW_VERSION_MINOR', ver_minor)
conf_data.set('ARROW_VERSION_PATCH', ver_patch)

arrow_so_version = ver_major.to_int() * 100 + ver_minor.to_int()
conf_data.set('ARROW_SO_VERSION', arrow_so_version)
conf_data.set('ARROW_FULL_SO_VERSION',
              arrow_so_version.to_string()
              + '.'
              + ver_patch
              + '.0')

compiler = meson.get_compiler('cpp')
# TODO: the meson compiler names are not the same as CMake
conf_data.set('ARROW_CXX_COMPILER_ID', compiler.get_id())
conf_data.set('ARROW_CXX_COMPILER_VERSION', compiler.version())
conf_data.set('ARROW_CXX_COMPILER_FLAGS', '')

conf_data.set('ARROW_BUILD_TYPE', get_option('buildtype').to_upper())
conf_data.set('ARROW_PACKAGE_KIND', '')  # TODO: add option for this

configure_file(input: 'meson_config.h.in',
               output: 'config.h',
               configuration: conf_data)

arrow_vcs_file = vcs_tag(
    input: 'meson_config_internal.h.in',
    output: 'config_internal.h'
)

# dependency required to avoid race condition between config generation
# and arrow library. See https://github.com/mesonbuild/meson/issues/8123
arrow_vcs_config_dep = declare_dependency(sources: arrow_vcs_file)
