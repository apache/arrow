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

# Versions more recent of llvm and clang are not compatible with the
# kernel version of the docker version
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-8.0.1/llvm-8.0.1.src.tar.xz
unxz llvm-8.0.1.src.tar.xz
tar xf llvm-8.0.1.src.tar
pushd llvm-8.0.1.src
mkdir build
pushd build
cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DLLVM_TARGETS_TO_BUILD=host \
    -DLLVM_INCLUDE_DOCS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_UTILS=OFF \
    -DLLVM_ENABLE_TERMINFO=OFF \
    -DLLVM_ENABLE_ASSERTIONS=ON \
    -DLLVM_ENABLE_RTTI=ON \
    -DLLVM_ENABLE_OCAMLDOC=OFF \
    -DLLVM_USE_INTEL_JITEVENTS=ON \
    -DLLVM_TEMPORARILY_ALLOW_OLD_TOOLCHAIN=ON \
    -DPYTHON_EXECUTABLE="$(cpython_path 3.6)/bin/python" \
    -GNinja \
    ..
ninja install
popd
popd
rm -rf llvm-8.0.1.src.tar.xz llvm-8.0.1.src.tar llvm-8.0.1.src

# clang is only used to precompile Gandiva bitcode
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-8.0.1/cfe-8.0.1.src.tar.xz
unxz cfe-8.0.1.src.tar.xz
tar xf cfe-8.0.1.src.tar
pushd cfe-8.0.1.src
mkdir build
pushd build
cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DCLANG_INCLUDE_TESTS=OFF \
    -DCLANG_INCLUDE_DOCS=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_DOCS=OFF \
    -DLLVM_TEMPORARILY_ALLOW_OLD_TOOLCHAIN=ON \
    -GNinja \
    ..
ninja -w dupbuild=warn install # both clang and llvm builds generate llvm-config file
popd
popd
rm -rf cfe-8.0.1.src.tar.xz cfe-8.0.1.src.tar cfe-8.0.1.src