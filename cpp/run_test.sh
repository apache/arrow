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
# export LD_LIBRARY_PATH=$CONDA_PREFIX/lib

mkdir -p cpp/build
pushd cpp/build
pwd

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DARROW_PARQUET=ON \
      -DARROW_BUILD_TESTS=ON \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=YES \
      ..

ninja

debug/parquet-column_reader-test
# ninja test

popd

# python setup.py build_ext --build-type=$ARROW_BUILD_TYPE --with-parquet --with-plasma --inplace
# py.test -sv "$@"


# 5f0d3d26c27973e99c658d84f56e13fb8a809c8b first issue
