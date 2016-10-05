#!/bin/bash

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

set -e
set -x

cd $RECIPE_DIR

# Build dependencies
export FLATBUFFERS_HOME=$PREFIX
export PARQUET_HOME=$PREFIX

if [ "$(uname)" == "Darwin" ]; then
  # C++11 finagling for Mac OSX
  export CC=clang
  export CXX=clang++
  export MACOSX_VERSION_MIN="10.7"
  CXXFLAGS="${CXXFLAGS} -mmacosx-version-min=${MACOSX_VERSION_MIN}"
  CXXFLAGS="${CXXFLAGS} -stdlib=libc++ -std=c++11"
  export LDFLAGS="${LDFLAGS} -mmacosx-version-min=${MACOSX_VERSION_MIN}"
  export LDFLAGS="${LDFLAGS} -stdlib=libc++ -std=c++11"
  export LINKFLAGS="${LDFLAGS}"
  export MACOSX_DEPLOYMENT_TARGET=10.7
fi

cd ..

rm -rf conda-build
mkdir conda-build

cp -r thirdparty conda-build/

cd conda-build
pwd

# Build googletest for running unit tests
./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh gtest

source thirdparty/versions.sh
export GTEST_HOME=`pwd`/thirdparty/$GTEST_BASEDIR

# if [ `uname` == Linux ]; then
#     SHARED_LINKER_FLAGS='-static-libstdc++'
# elif [ `uname` == Darwin ]; then
#     SHARED_LINKER_FLAGS=''
# fi

# -DCMAKE_SHARED_LINKER_FLAGS=$SHARED_LINKER_FLAGS \

cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DARROW_HDFS=on \
    -DARROW_IPC=on \
    -DARROW_PARQUET=on \
    ..

make
ctest -L unittest
make install
