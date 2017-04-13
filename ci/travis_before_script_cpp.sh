#!/usr/bin/env bash

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.


set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh
source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

# Set up C++ toolchain from conda-forge packages for faster builds
conda create -y -q -p $CPP_TOOLCHAIN python=2.7 flatbuffers rapidjson

if [ $TRAVIS_OS_NAME == "osx" ]; then
  brew update > /dev/null
  brew install jemalloc
  brew install ccache
fi

mkdir $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

CMAKE_COMMON_FLAGS="\
-DARROW_BUILD_BENCHMARKS=ON \
-DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL"

if [ $TRAVIS_OS_NAME == "linux" ]; then
    cmake -DARROW_TEST_MEMCHECK=on \
          $CMAKE_COMMON_FLAGS \
          -DARROW_CXXFLAGS="-Wconversion -Werror" \
          $ARROW_CPP_DIR
else
    cmake $CMAKE_COMMON_FLAGS \
          -DARROW_CXXFLAGS=-Werror \
          $ARROW_CPP_DIR
fi

make -j4
make install

popd
