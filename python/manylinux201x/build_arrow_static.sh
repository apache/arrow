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
#
# Build upon the scripts in https://github.com/matthew-brett/manylinux-builds
# * Copyright (c) 2013-2019, Matt Terry and Matthew Brett (BSD 2-clause)
#
# Usage:
#   either build:
#     $ docker-compose build centos-python-manylinux2010
#   or pull:
#     $ docker-compose pull centos-python-manylinux2010
#   and then run:
#     $ docker-compose run -e PYTHON_VERSION=3.7 centos-python-manylinux2010
# Can use either manylinux2010 or manylinux2014

source /multibuild/manylinux_utils.sh

# Quit on failure
set -e

# Print commands for debugging
# set -x

cd /arrow/python

export PKG_CONFIG_PATH=/usr/lib/pkgconfig:/arrow-dist/lib/pkgconfig

# Ensure the target directory exists
mkdir -p /io/dist

ARROW_BUILD_DIR=/io/dist
mkdir -p "${ARROW_BUILD_DIR}"
pushd "${ARROW_BUILD_DIR}"
CC=gcc CXX=g++ cmake -DCMAKE_BUILD_TYPE=Release \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DZLIB_ROOT=/usr/local \
    -DCMAKE_INSTALL_PREFIX=/io/dist \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_BOOST_USE_SHARED=OFF \
    -DARROW_GANDIVA_PC_CXX_FLAGS="-isystem;/opt/rh/devtoolset-8/root/usr/include/c++/8/;-isystem;/opt/rh/devtoolset-8/root/usr/include/c++/8/x86_64-redhat-linux/" \
    -DARROW_JEMALLOC=ON \
    -DARROW_RPATH_ORIGIN=ON \
    -DARROW_PYTHON=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_DATASET=ON \
    -DARROW_PLASMA=OFF \
    -DARROW_TENSORFLOW=OFF \
    -DARROW_ORC=OFF \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_GANDIVA_JAVA=OFF \
    -DBoost_NAMESPACE=arrow_boost \
    -DBOOST_ROOT=/arrow_boost_dist \
    -DOPENSSL_USE_STATIC_LIBS=ON \
    -GNinja /arrow/cpp
ninja -v install

# Copy the bundled static libs from the build to the install dir
find . -regex .*/lib/.*\\.a\$ | xargs -I{} cp {} ./lib

popd
