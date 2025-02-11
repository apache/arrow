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

# arrow_install_all_headers

conf_data = configuration_data()

version = meson.project_version()
# Remove any pre-release / build identifiers
version_no_pre_release = version.split('-')[0]
version_no_build = version_no_pre_release.split('+')[0]
components = version_no_build.split('.')
assert(
    components.length() >= 3,
    'The version does not contain major, minor and patch',
)
ver_major = components[0]
ver_minor = components[1]
ver_patch = components[2]
conf_data.set('ARROW_VERSION_MAJOR', ver_major)
conf_data.set('ARROW_VERSION_MINOR', ver_minor)
conf_data.set('ARROW_VERSION_PATCH', ver_patch)

conf_data.set('ARROW_VERSION_STRING', version)

# conf_data.set('ARROW_SO_VERSION', ...)
# conf_data.set('ARROW_FULL_SO_VERSION', ...)

# conf_data.set('ARROW_CXX_COMPILER_ID', ...)
# conf_data.set('ARROW_CXX_COMPILER_VERSION', ...)
# conf_data.set('ARROW_CXX_COMPILER_FLAGS', ...)

conf_data.set('UPPERCASE_BUILD_TYPE', get_option('buildtype').to_upper())

# conf_data.set('ARROW_PACKAGE_KIND', ...)

configure_file(
    input: 'config.h.cmake',
    output: 'config.h',
    configuration: conf_data,
    # https://mesonbuild.com/Reference-manual_functions.html#arguments13
    # TODO: need to bridge #cmakedefines somehow
    format: 'cmake@',
    install: true,
    install_dir: '.',
)

internal_conf_data = configuration_data()

# internal_conf_data.set('ARROW_GIT_ID', ...)
# internal_conf_data.set('ARROW_GIT_DESCRIPTION', ...)

configure_file(
    input: 'config_internal.h.cmake',
    output: 'config_internal.h',
    configuration: internal_conf_data,
    # https://mesonbuild.com/Reference-manual_functions.html#arguments13
    # TODO: need to bridge #cmakedefines somehow
    format: 'cmake@',
    install: true,
    install_dir: '.',
)
