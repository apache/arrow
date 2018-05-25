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

export ARROW_BUILD_TYPE=debug
export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_HOME=$CONDA_PREFIX

pushd ../cpp/build

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_PYTHON=ON \
      -DARROW_PLASMA=ON \
      -DARROW_BUILD_TESTS=OFF \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=YES \
      ..
ninja
ninja install

popd

pushd ../../parquet-cpp/build

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=OFF \
      -DPARQUET_BUILD_EXECUTABLES=OFF \
      -DPARQUET_BUILD_TESTS=OFF \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=YES \
      ..

ninja
ninja install

popd

python setup.py build_ext --build-type=$ARROW_BUILD_TYPE --with-parquet --with-plasma --inplace
py.test -sv "$@"
