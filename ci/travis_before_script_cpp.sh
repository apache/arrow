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


set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

if [ "$1" == "--only-library" ]; then
  only_library_mode=yes
else
  only_library_mode=no
  source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh
fi

if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  # Set up C++ toolchain from conda-forge packages for faster builds
  conda create -y -q -p $CPP_TOOLCHAIN python=2.7 \
        jemalloc=4.4.0 \
        nomkl \
        boost-cpp \
        rapidjson \
        flatbuffers \
        gflags \
        lz4-c \
        snappy \
        zstd \
        brotli \
        zlib \
        cmake \
        curl \
        thrift-cpp \
        ninja
fi

if [ $TRAVIS_OS_NAME == "osx" ]; then
  brew update > /dev/null
  brew install jemalloc
  brew install ccache
fi

mkdir $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

CMAKE_COMMON_FLAGS="\
-DARROW_BUILD_BENCHMARKS=ON \
-DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL \
-DARROW_NO_DEPRECATED_API=ON \
-DARROW_EXTRA_ERROR_CONTEXT=ON"
CMAKE_LINUX_FLAGS=""
CMAKE_OSX_FLAGS=""

if [ $only_library_mode == "yes" ]; then
  CMAKE_COMMON_FLAGS="\
$CMAKE_COMMON_FLAGS \
-DARROW_BUILD_TESTS=OFF \
-DARROW_BUILD_UTILITIES=OFF \
-DARROW_INSTALL_NAME_RPATH=OFF"
fi

# Use Ninja for faster builds when using toolchain
if [ $ARROW_TRAVIS_USE_TOOLCHAIN == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
fi

if [ $ARROW_TRAVIS_PLASMA == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_PLASMA=ON"
fi

if [ $ARROW_TRAVIS_VALGRIND == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_TEST_MEMCHECK=ON"
fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_LINUX_FLAGS \
          -DARROW_CXXFLAGS="-Wconversion -Wno-sign-conversion -Werror" \
          $ARROW_CPP_DIR
else
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_OSX_FLAGS \
          -DARROW_CXXFLAGS=-Werror \
          $ARROW_CPP_DIR
fi

$TRAVIS_MAKE -j4
$TRAVIS_MAKE install

popd
