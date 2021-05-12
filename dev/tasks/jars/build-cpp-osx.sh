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

set -x

export ARROW_TEST_DATA="$GITHUB_WORKSPACE/arrow/testing/data"
export PARQUET_TEST_DATA="$GITHUB_WORKSPACE/arrow/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

# Builds arrow + gandiva and tests the same.
pushd cpp
  mkdir build
  pushd build
    CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Release \
          -DARROW_GANDIVA=ON \
          -DARROW_GANDIVA_JAVA=ON \
          -DARROW_GANDIVA_STATIC_LIBSTDCPP=ON \
          -DARROW_ORC=ON \
          -DARROW_JNI=ON \
          -DARROW_PLASMA=ON \
          -DARROW_PLASMA_JAVA_CLIENT=ON \
          -DARROW_BUILD_TESTS=ON \
          -DARROW_BUILD_UTILITIES=OFF \
          -DPARQUET_REQUIRE_ENCRYPTION=OFF \
          -DARROW_PARQUET=ON \
          -DPARQUET_BUILD_EXAMPLES=OFF \
          -DPARQUET_BUILD_EXECUTABLES=OFF \
          -DARROW_FILESYSTEM=ON \
          -DARROW_DATASET=ON \
          -DARROW_BOOST_USE_SHARED=OFF \
          -DARROW_PROTOBUF_USE_SHARED=OFF \
          -DARROW_GFLAGS_USE_SHARED=OFF \
          -DARROW_OPENSSL_USE_SHARED=OFF \
          -DARROW_BROTLI_USE_SHARED=OFF \
          -DARROW_BZ2_USE_SHARED=OFF \
          -DARROW_GRPC_USE_SHARED=OFF \
          -DARROW_LZ4_USE_SHARED=OFF \
          -DARROW_SNAPPY_USE_SHARED=OFF \
          -DARROW_THRIFT_USE_SHARED=OFF \
          -DARROW_UTF8PROC_USE_SHARED=OFF \
          -DARROW_ZSTD_USE_SHARED=OFF"

    cmake $CMAKE_FLAGS ..
    make -j4
    ctest

    cp -L release/libgandiva_jni.dylib $GITHUB_WORKSPACE/arrow/dist
    cp -L release/libarrow_dataset_jni.dylib $GITHUB_WORKSPACE/arrow/dist
    cp -L release/libarrow_orc_jni.dylib $GITHUB_WORKSPACE/arrow/dist
  popd
popd
