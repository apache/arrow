#!/bin/sh

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
source activate $ASV_ENV_PATH

conda install -c conda-forge zlib=1.2.11=0

# Build Arrow C++ libraries
export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
export ARROW_HOME=$CONDA_PREFIX

echo $CONDA_PREFIX

pushd ../cpp
mkdir -p build
pushd build

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=release \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_CXXFLAGS=$CXXFLAGS \
      -DARROW_PYTHON=ON \
      -DARROW_PLASMA=ON \
      -DARROW_BUILD_TESTS=OFF \
      ..
cmake --build . --target install

popd
popd

# Build pyarrow wrappers
export SETUPTOOLS_SCM_PRETEND_VERSION=0.0.1
export PYARROW_BUILD_TYPE=release
export PYARROW_WITH_PLASMA=1

python setup.py clean
find pyarrow -name "*.so" -delete
python setup.py develop
