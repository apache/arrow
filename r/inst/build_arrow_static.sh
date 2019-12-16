#!/bin/bash
#
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

# Quit on failure
set -e

# Print commands for debugging
set -x

cd r

ARROW_BUILD_DIR="$(pwd)/libarrow/dist"
mkdir -p "${ARROW_BUILD_DIR}"
pushd "${ARROW_BUILD_DIR}"
cmake -DCMAKE_BUILD_TYPE=Release \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DCMAKE_INSTALL_PREFIX= \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_BOOST_USE_SHARED=OFF \
    -DARROW_JEMALLOC=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_DATASET=ON \
    -DARROW_ORC=OFF \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_BROTLI=ON \
    -DOPENSSL_USE_STATIC_LIBS=ON \
    -G "${CMAKE_GENERATOR:-Ninja}" \
    ../cpp
cmake --build . --target install

# Copy the bundled static libs from the build to the install dir
find . -regex .*/lib/.*\\.a\$ | xargs -I{} cp -u {} ./lib

zip -r libarrow.zip lib/*.a include/

popd
