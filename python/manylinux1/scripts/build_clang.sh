#!/bin/bash -ex
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

source /multibuild/manylinux_utils.sh

export LLVM_VERSION="6.0.0"
curl -sL http://releases.llvm.org/${LLVM_VERSION}/cfe-${LLVM_VERSION}.src.tar.xz -o cfe-${LLVM_VERSION}.src.tar.xz
unxz cfe-${LLVM_VERSION}.src.tar.xz
tar xf cfe-${LLVM_VERSION}.src.tar
pushd cfe-${LLVM_VERSION}.src
mkdir build
pushd build
cmake  \
    -DCMAKE_BUILD_TYPE=Release \
    -DCLANG_INCLUDE_TESTS=OFF \
    -DCLANG_INCLUDE_DOCS=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_DOCS=OFF \
    -GNinja \
    ..
ninja install
popd
popd
rm -rf cfe-${LLVM_VERSION}.src.tar.xz cfe-${LLVM_VERSION}.src.tar cfe-${LLVM_VERSION}.src
