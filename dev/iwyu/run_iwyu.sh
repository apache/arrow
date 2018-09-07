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

# Set up environment and working directory
CLANG_VERSION=6.0
IWYU_BUILD_DIR=`pwd`/arrow/cpp/docker-iwyu
IWYU_SH=`pwd`/arrow/cpp/build-support/iwyu/iwyu.sh
IWYU_URL=https://github.com/include-what-you-use/include-what-you-use/archive/clang_$CLANG_VERSION.tar.gz

rm -rf $IWYU_BUILD_DIR
mkdir -p $IWYU_BUILD_DIR
pushd $IWYU_BUILD_DIR

function cleanup {
    popd
    rm -rf $IWYU_BUILD_DIR
}

trap cleanup EXIT

# Build IWYU
wget -O iwyu.tar.gz $IWYU_URL
tar xzf iwyu.tar.gz
rm -f iwyu.tar.gz

IWYU_SRC=`pwd`/include-what-you-use-clang_$CLANG_VERSION

export CC=clang-$CLANG_VERSION
export CXX=clang++-$CLANG_VERSION

mkdir -p iwyu-build
pushd iwyu-build

# iwyu needs this
apt-get install -y zlib1g-dev

cmake -G "Unix Makefiles" -DIWYU_LLVM_ROOT_PATH=/usr/lib/llvm-$CLANG_VERSION $IWYU_SRC
make -j4
popd

# Add iwyu and iwyu_tool.py to path
export PATH=$IWYU_BUILD_DIR/iwyu-build:$PATH

conda activate pyarrow-dev

cmake -GNinja -DARROW_PYTHON=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..

# Make so that vendored bits are built
ninja

$IWYU_SH all
