#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Exit on any error
set -e

# cwd is mounted from host machine to
# and contains both arrow and parquet-cpp

export ARROW_BUILD_TYPE=debug
export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_HOME=$CONDA_PREFIX

export CC=gcc-4.9
export CXX=g++-4.9

# install arrow
mkdir -p arrow/cpp/build
pushd arrow/cpp/build

rm -rf ./*

cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DARROW_HDFS=on \
      ..
make -j4
make install
popd

# install parquet-cpp
mkdir -p parquet-cpp/build
pushd parquet-cpp/build

rm -rf ./*

cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=on \
      ..

make -j4
make install
popd

# install pyarrow
pushd arrow/python

python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
    --with-parquet --with-plasma --inplace

popd


arrow/cpp/build/debug/io-hdfs-test

python -m pytest -vv -r sxX -s arrow/python/pyarrow --parquet --hdfs
