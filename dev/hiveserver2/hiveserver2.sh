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
export ARROW_HOME=$CONDA_PREFIX

# For newer GCC per https://arrow.apache.org/docs/python/development.html#known-issues
export CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"
export PYARROW_CXXFLAGS=$CXXFLAGS
export PYARROW_CMAKE_GENERATOR=Ninja

_PWD=`pwd`
ARROW_CPP_BUILD_DIR=$_PWD/arrow/cpp/hiveserver2-build
DOCKER_COMMON_DIR=$_PWD/arrow/dev/docker_common

function cleanup {
    rm -rf $ARROW_CPP_BUILD_DIR
}

trap cleanup EXIT

# Install arrow-cpp
mkdir -p $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_HIVESERVER2=ON \
      -DARROW_BUILD_TESTS=ON \
      -DCMAKE_CXX_FLAGS=$CXXFLAGS \
      ..
ninja hiveserver2-test

$DOCKER_COMMON_DIR/wait-for-it.sh impala:21050 -t 300 -s -- echo "impala is up"

# Run C++ unit tests
export ARROW_HIVESERVER2_TEST_HOST=impala
debug/hiveserver2-test

popd
