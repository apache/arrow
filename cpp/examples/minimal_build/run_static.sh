#!/bin/bash
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

set -e

: ${ARROW_DIR:=/arrow}
: ${EXAMPLE_DIR:=/io}
: ${ARROW_BUILD_DIR:=/build/arrow}
: ${EXAMPLE_BUILD_DIR:=/build/example}

echo
echo "=="
echo "== Building Arrow C++ library"
echo "=="
echo

mkdir -p $ARROW_BUILD_DIR
pushd $ARROW_BUILD_DIR

NPROC=$(nproc)

cmake $ARROW_DIR/cpp \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_HDFS=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_JSON=ON \
    -DARROW_ORC=ON \
    -DARROW_PARQUET=ON \
    -DARROW_PLASMA=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    $ARROW_CMAKE_OPTIONS

make -j$NPROC
make install

popd

echo
echo "=="
echo "== Building example project using Arrow C++ library"
echo "=="
echo

mkdir -p $EXAMPLE_BUILD_DIR
pushd $EXAMPLE_BUILD_DIR

cmake $EXAMPLE_DIR -DARROW_LINK_SHARED=OFF
make

popd

echo
echo "=="
echo "== Running example project"
echo "=="
echo

cd $EXAMPLE_DIR

${EXAMPLE_BUILD_DIR}/arrow_example
