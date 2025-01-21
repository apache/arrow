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

arrow_dir=${1}
source_dir=${arrow_dir}/js
build_dir=${2}

: ${BUILD_DOCS_JS:=OFF}

# https://github.com/apache/arrow/issues/41429
# TODO: We want to out-of-source build. This is a workaround. We copy
# all needed files to the build directory from the source directory
# and build in the build directory.
rm -rf ${build_dir}/js
mkdir -p ${build_dir}
cp -aL ${arrow_dir}/LICENSE.txt ${build_dir}/
cp -aL ${arrow_dir}/NOTICE.txt ${build_dir}/
cp -aL ${source_dir} ${build_dir}/js
pushd ${build_dir}/js

yarn --immutable
yarn lint:ci
yarn build

if [ "${BUILD_DOCS_JS}" == "ON" ]; then
  # If apache or upstream are defined use those as remote.
  # Otherwise use origin which could be a fork on PRs.
  if [ "$(git -C ${arrow_dir} config --get remote.apache.url)" == "git@github.com:apache/arrow.git" ]; then
    yarn doc --gitRemote apache
  elif [[ "$(git -C ${arrow_dir}config --get remote.upstream.url)" =~ "https://github.com/apache/arrow" ]]; then
    yarn doc --gitRemote upstream
  elif [[ "$(basename -s .git $(git -C ${arrow_dir} config --get remote.origin.url))" == "arrow" ]]; then
    yarn doc
  else
    echo "Failed to build docs because the remote is not set correctly. Please set the origin or upstream remote to https://github.com/apache/arrow.git or the apache remote to git@github.com:apache/arrow.git."
    exit 0
  fi
  mkdir -p ${build_dir}/docs/js
  rsync -a doc/ ${build_dir}/docs/js
fi

popd
