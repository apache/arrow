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

pushd ${source_dir}

yarn --immutable
yarn lint:ci
yarn build

if [ "${BUILD_DOCS_JS}" == "ON" ]; then
  if [ "$(git config --get remote.origin.url)" == "https://github.com/apache/arrow.git" ]; then
    yarn doc
  elif [ "$(git config --get remote.upstream.url)" == "https://github.com/apache/arrow.git" ]; then
    yarn doc --gitRemote upstream
  elif [ "$(git config --get remote.apache.url)" == "git@github.com:apache/arrow.git" ]; then
    yarn doc --gitRemote apache
  else
    echo "Failed to build docs because the remote is not set correctly. Please set the origin or upstream remote to https://github.com/apache/arrow.git or the apache remote to git@github.com:apache/arrow.git."
    exit 0
  fi
  mkdir -p ${build_dir}/docs/js
  rsync -a ${arrow_dir}/js/doc/ ${build_dir}/docs/js
fi

popd
