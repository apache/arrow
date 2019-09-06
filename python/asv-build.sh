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
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_HOME=$CONDA_PREFIX
export ORC_HOME=$CONDA_PREFIX
export PROTOBUF_HOME=$CONDA_PREFIX
export BOOST_ROOT=$CONDA_PREFIX

pushd ../cpp
mkdir -p build
pushd build

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=release \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_CXXFLAGS=$CXXFLAGS \
      -DARROW_USE_GLOG=off \
      -DARROW_FLIGHT=on \
      -DARROW_ORC=on \
      -DARROW_PARQUET=on \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DARROW_S3=on \
      -DARROW_BUILD_TESTS=off \
      ..
cmake --build . --target install

popd
popd

# Build pyarrow wrappers
export SETUPTOOLS_SCM_PRETEND_VERSION=0.0.1
export PYARROW_BUILD_TYPE=release
export PYARROW_PARALLEL=8
export PYARROW_WITH_FLIGHT=1
export PYARROW_WITH_ORC=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_PLASMA=1

python setup.py clean
find pyarrow -name "*.so" -delete
python setup.py develop
