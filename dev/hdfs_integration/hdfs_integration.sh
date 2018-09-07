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

# Activate conda environment
conda activate pyarrow-dev

# Arrow build variables
export ARROW_BUILD_TYPE=debug
export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_HOME=$CONDA_PREFIX

# Hadoop variables
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

# For newer GCC per https://arrow.apache.org/docs/python/development.html#known-issues
export CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"
export PYARROW_CXXFLAGS=$CXXFLAGS
export PYARROW_CMAKE_GENERATOR=Ninja

_PWD=`pwd`
ARROW_CPP_BUILD_DIR=$_PWD/arrow/cpp/hdfs-integration-build
PARQUET_CPP_BUILD_DIR=$_PWD/parquet-cpp/hdfs-integration-build

# Run tests
export LIBHDFS3_CONF=$_PWD/arrow/dev/hdfs_integration/libhdfs3-client-config.xml

function cleanup {
    rm -rf $ARROW_CPP_BUILD_DIR
    rm -rf $PARQUET_CPP_BUILD_DIR
    pushd $_PWD/arrow/python
    git clean -fdx .
    popd
}

trap cleanup EXIT

# Install arrow-cpp
mkdir -p $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_PYTHON=ON \
      -DARROW_PLASMA=ON \
      -DARROW_HDFS=ON \
      -DARROW_BUILD_TESTS=ON \
      -DCMAKE_CXX_FLAGS=$CXXFLAGS \
      ..
ninja
ninja install

# Run C++ unit tests
debug/io-hdfs-test

popd

# Install parquet-cpp
mkdir -p $PARQUET_CPP_BUILD_DIR
pushd $PARQUET_CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=OFF \
      -DPARQUET_BUILD_EXECUTABLES=OFF \
      -DPARQUET_BUILD_TESTS=OFF \
      -DCMAKE_CXX_FLAGS=$CXXFLAGS \
      ..
ninja
ninja install

popd

# Install pyarrow
pushd arrow/python

# Clear the build directory so we are guaranteed a fresh set of extensions
rm -rf build/

python setup.py build_ext \
    --build-type=$ARROW_BUILD_TYPE \
    --with-parquet \
    --with-plasma \
    --inplace

# Python
python -m pytest -vv -r sxX -s pyarrow \
       --only-parquet --only-hdfs

popd
