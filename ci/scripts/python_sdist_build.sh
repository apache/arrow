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

set -eux

source_dir=${1}/python

pushd "${source_dir}"
export SETUPTOOLS_SCM_PRETEND_VERSION=${PYARROW_VERSION:-}
# The mounted source directory is already a git repository, so mark it as safe
git config --global --add safe.directory "${1}"
${PYTHON:-python} -m pip install build
${PYTHON:-python} -m build --sdist . -Csetup-args="-Dsdist=true"
popd
