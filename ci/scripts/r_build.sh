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

: ${R_BIN:=R}
source_dir=${1}/r
build_dir=${2}

: ${BUILD_DOCS_R:=OFF}

pushd ${source_dir}

${R_BIN} CMD INSTALL ${INSTALL_ARGS} .

if [ "${BUILD_DOCS_R}" == "ON" ]; then
  ${R_BIN} -e "pkgdown::build_site(install = FALSE)"
  rsync -a ${source_dir}/docs/ ${build_dir}/docs/r
fi

popd
