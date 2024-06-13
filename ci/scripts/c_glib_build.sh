#!/usr/bin/env bash
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

set -ex

source_dir=${1}/c_glib
build_dir=${2}/c_glib
build_root=${2}

: ${ARROW_GLIB_WERROR:=false}
: ${ARROW_GLIB_VAPI:=true}
: ${BUILD_DOCS_C_GLIB:=OFF}
with_doc=$([ "${BUILD_DOCS_C_GLIB}" == "ON" ] && echo "true" || echo "false")


if [ -n "${MSYSTEM:-}" ]; then
  # Fix ARROW_HOME when running under MSYS2
  export ARROW_HOME="$(cygpath --unix "${ARROW_HOME}")"
fi

export PATH="${ARROW_HOME}/bin:${PATH}"

meson_pkg_config_path="${ARROW_HOME}/lib/pkgconfig"

mkdir -p ${build_dir}

if [ -n "${VCPKG_ROOT:-}" ]; then
  vcpkg_install_root="${build_root}/vcpkg_installed"
  vcpkg_triplet="x64-windows"
  $VCPKG_ROOT/vcpkg install --x-manifest-root=${source_dir} --x-install-root=${vcpkg_install_root}
  export PKG_CONFIG="${vcpkg_install_root}/${vcpkg_triplet}/tools/pkgconf/pkgconf.exe"
  meson_pkg_config_path="${vcpkg_install_root}/${vcpkg_triplet}/lib/pkgconfig:${meson_pkg_config_path}"
  # Configure PATH for libraries required by the gobject-introspection generated binary
  cpp_vcpkg_install_root="${build_root}/cpp/vcpkg_installed"
  export PATH="${vcpkg_install_root}/${vcpkg_triplet}/bin:${cpp_vcpkg_install_root}/${vcpkg_triplet}/bin:${cpp_vcpkg_install_root}/${vcpkg_triplet}/debug/bin:${PATH}"
fi

if [ -n "${VCToolsInstallDir:-}" -a -n "${MSYSTEM:-}" ]; then
  # Meson finds the gnu link.exe instead of MSVC link.exe when running in MSYS2/git bash,
  # so we need to make sure the MSCV link.exe is first in $PATH
  export PATH="$(cygpath --unix "${VCToolsInstallDir}")/bin/HostX64/x64:${PATH}"
fi

# Build with Meson
meson setup \
      --backend=ninja \
      --prefix=$ARROW_HOME \
      --libdir=lib \
      --pkg-config-path="${meson_pkg_config_path}" \
      -Ddoc=${with_doc} \
      -Dvapi=${ARROW_GLIB_VAPI} \
      -Dwerror=${ARROW_GLIB_WERROR} \
      ${build_dir} \
      ${source_dir}

pushd ${build_dir}
ninja
ninja install
popd

if [ "${BUILD_DOCS_C_GLIB}" == "ON" ]; then
  mkdir -p ${build_root}/docs/c_glib
  cp -a ${ARROW_HOME}/share/doc/*-glib/ ${build_root}/docs/c_glib/
fi
