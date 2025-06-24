#!/usr/bin/env bash
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

: "${R_BIN:=R}"
source_dir=${1}/r
build_dir=${2}

: "${BUILD_DOCS_R:=OFF}"

R_INSTALL_ARGS=()
for arg in ${INSTALL_ARGS:-}; do
  R_INSTALL_ARGS+=("${arg}")
done

# https://github.com/apache/arrow/issues/41429
# TODO: We want to out-of-source build. This is a workaround. We copy
# all needed files to the build directory from the source directory
# and build in the build directory.
rm -rf "${build_dir}/r"
cp -aL "${source_dir}" "${build_dir}/r"
pushd "${build_dir}/r"

# build first so that any stray compiled files in r/src are ignored
${R_BIN} CMD build .
if [ -x "$(command -v sudo)" ]; then
  SUDO=sudo
else
  SUDO=
fi
${SUDO} \
  env \
    PKG_CONFIG_PATH="${ARROW_HOME}/lib/pkgconfig:${PKG_CONFIG_PATH}" \
      "${R_BIN}" CMD INSTALL "${R_INSTALL_ARGS[@]}" arrow*.tar.gz

if [ "${BUILD_DOCS_R}" == "ON" ]; then
  ${R_BIN} -e "pkgdown::build_site(install = FALSE)"
  rsync -a docs/ "${build_dir}/docs/r"
fi

popd
