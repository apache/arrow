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

set -e

# ASV doesn't activate its conda environment for us
if [ -z "$ASV_ENV_DIR" ]; then exit 1; fi
if [ -z "$CONDA_HOME" ]; then
  echo "Please set \$CONDA_HOME to point to your root conda installation"
  exit 1;
fi

eval "$($CONDA_HOME/bin/conda shell.bash hook)"
conda activate $ASV_ENV_DIR

echo "== Conda Prefix for benchmarks: " $CONDA_PREFIX " =="

# Build Arrow C++ libraries
export ARROW_BUILD_TYPE=debug
export ARROW_DATASET=ON
export ARROW_DEPENDENCY_SOURCE=CONDA
export ARROW_FLIGHT=ON
export ARROW_GANDIVA=OFF
export ARROW_HOME=$CONDA_PREFIX
export ARROW_PARQUET=ON
export ARROW_PLASMA=ON
export ARROW_PYTHON=ON
export ARROW_S3=OFF
export ARROW_USE_GLOG=OFF
export ARROW_USE_CCACHE=ON
export PARQUET_HOME=$CONDA_PREFIX
export SETUPTOOLS_SCM_PRETEND_VERSION=2.0.0

source_dir="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"
build_dir=$ASV_BUILD_DIR/build

"${source_dir}/ci/scripts/cpp_build.sh" ${source_dir} ${build_dir}
"${source_dir}/ci/scripts/python_build.sh" ${source_dir} ${build_dir}
