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

arrow_dir=${1}

export ARROW_SOURCE_DIR=${arrow_dir}
export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${arrow_dir}/cpp/submodules/parquet-testing/data

export PYARROW_CMAKE_GENERATOR=${CMAKE_GENERATOR:-Ninja}
export PYARROW_BUILD_TYPE=${CMAKE_BUILD_TYPE:-debug}
export PYARROW_WITH_ACERO=${ARROW_ACERO:-ON}
export PYARROW_WITH_S3=${ARROW_S3:-OFF}
export PYARROW_WITH_ORC=${ARROW_ORC:-OFF}
export PYARROW_WITH_CUDA=${ARROW_CUDA:-OFF}
export PYARROW_WITH_HDFS=${ARROW_HDFS:-OFF}
export PYARROW_WITH_FLIGHT=${ARROW_FLIGHT:-OFF}
export PYARROW_WITH_GANDIVA=${ARROW_GANDIVA:-OFF}
export PYARROW_WITH_PARQUET=${ARROW_PARQUET:-OFF}
export PYARROW_WITH_PARQUET_ENCRYPTION=${ARROW_PARQUET:-OFF}
export PYARROW_WITH_DATASET=${ARROW_DATASET:-OFF}

# TODO: Users should not require ARROW_HOME and pkg-config to find Arrow C++.
# Related: ARROW-9171
# unset ARROW_HOME
# apt purge -y pkg-config

# ARROW-12619
if command -v git &> /dev/null; then
  echo "Git exists, remove it from PATH before executing this script."
  exit 1
fi

if [ -n "${PYARROW_VERSION:-}" ]; then
  sdist="${arrow_dir}/python/dist/pyarrow-${PYARROW_VERSION}.tar.gz"
else
  sdist=$(ls ${arrow_dir}/python/dist/pyarrow-*.tar.gz | sort -r | head -n1)
fi
${PYTHON:-python} -m pip install ${sdist}

pytest -r s ${PYTEST_ARGS:-} --pyargs pyarrow
