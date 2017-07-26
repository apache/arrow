#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

use_gcc() {
  export CC=gcc-4.9
  export CXX=g++-4.9
}

use_clang() {
  export CC=clang-4.0
  export CXX=clang++-4.0
}

build_arrow() {
  mkdir -p $ARROW_CPP_BUILD_DIR
  pushd $ARROW_CPP_BUILD_DIR

  cmake -GNinja \
        -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
        -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        -DARROW_NO_DEPRECATED_API=ON \
        -DARROW_PLASMA=ON \
        -DARROW_BOOST_USE_SHARED=off \
        $ARROW_CPP_DIR

  ninja
  ninja install
  popd
}

build_parquet() {
  PARQUET_DIR=$BUILD_DIR/parquet
  mkdir -p $PARQUET_DIR

  git clone https://github.com/apache/parquet-cpp.git $PARQUET_DIR

  pushd $PARQUET_DIR
  mkdir build-dir
  cd build-dir

  cmake \
      -GNinja \
      -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BOOST_USE_SHARED=off \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=off \
      ..

  ninja
  ninja install

  popd
}
