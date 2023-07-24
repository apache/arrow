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
with_gtk_doc=$([ "${BUILD_DOCS_C_GLIB}" == "ON" ] && echo "true" || echo "false")

export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig

export CFLAGS="-DARROW_NO_DEPRECATED_API"
export CXXFLAGS="-DARROW_NO_DEPRECATED_API"

mkdir -p ${build_dir}

# Build with Meson
meson setup \
      --prefix=$ARROW_HOME \
      --libdir=lib \
      -Dgtk_doc=${with_gtk_doc} \
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
  rsync -a ${ARROW_HOME}/share/gtk-doc/html/ ${build_root}/docs/c_glib
fi
