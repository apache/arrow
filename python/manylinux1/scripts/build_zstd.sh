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

export ZSTD_VERSION="1.4.0"

curl -sL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz" -o zstd-${ZSTD_VERSION}.tar.gz
tar xf zstd-${ZSTD_VERSION}.tar.gz
pushd zstd-${ZSTD_VERSION}
mkdir build_cmake
pushd build_cmake

cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DZSTD_BUILD_PROGRAMS=off \
    -DZSTD_BUILD_SHARED=off \
    -DZSTD_BUILD_STATIC=on \
    -DZSTD_MULTITHREAD_SUPPORT=off \
    -DCMAKE_POSITION_INDEPENDENT_CODE=1 \
    ../build/cmake
ninja install

popd
popd
rm -rf zstd-${ZSTD_VERSION}.tar.gz zstd-${ZSTD_VERSION}
