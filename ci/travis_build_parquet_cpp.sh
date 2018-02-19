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

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

source $TRAVIS_BUILD_DIR/ci/travis_install_toolchain.sh

export PARQUET_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN
export ARROW_HOME=$ARROW_CPP_INSTALL

PARQUET_DIR=$TRAVIS_BUILD_DIR/parquet
mkdir -p $PARQUET_DIR

git clone -q https://github.com/apache/parquet-cpp.git $PARQUET_DIR

pushd $PARQUET_DIR
mkdir build-dir
cd build-dir

cmake \
    -GNinja \
    -DCMAKE_BUILD_TYPE=debug \
    -DCMAKE_INSTALL_PREFIX=$ARROW_PYTHON_PARQUET_HOME \
    -DPARQUET_BOOST_USE_SHARED=off \
    -DPARQUET_BUILD_BENCHMARKS=off \
    -DPARQUET_BUILD_EXECUTABLES=off \
    -DPARQUET_BUILD_TESTS=off \
    ..

ninja
ninja install

popd
