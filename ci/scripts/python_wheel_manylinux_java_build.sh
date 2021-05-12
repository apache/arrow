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

# Quit on failure
set -e

ARROW_BUILD_DIR=/tmp/arrow-build

echo "=== (${PYTHON_VERSION}) Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf /tmp/arrow-build
rm -rf /arrow-dist

echo "=== (${PYTHON_VERSION}) Building Arrow C++ libraries ==="
: ${ARROW_DATASET:=ON}
: ${ARROW_GANDIVA:=ON}
: ${ARROW_GANDIVA_JAVA:=ON}
: ${ARROW_FILESYSTEM:=ON}
: ${ARROW_JEMALLOC:=ON}
: ${ARROW_RPATH_ORIGIN:=ON}
: ${ARROW_ORC:=ON}
: ${ARROW_PARQUET:=ON}
: ${ARROW_PLASMA:=ON}
: ${ARROW_PLASMA_JAVA_CLIENT:=ON}
: ${ARROW_PYTHON:=OFF}
: ${ARROW_JNI:=ON}
: ${ARROW_BUILD_TESTS:=ON}
: ${CMAKE_BUILD_TYPE:=Release}
: ${CMAKE_UNITY_BUILD:=ON}
: ${CMAKE_GENERATOR:=Ninja}
: ${VCPKG_FEATURE_FLAGS:=-manifests}
: ${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-linux-static-${CMAKE_BUILD_TYPE}}}
: ${PYTHON_VERSION:=3.7}
: ${GANDIVA_CXX_FLAGS:=-isystem;/opt/rh/devtoolset-9/root/usr/include/c++/9;-isystem;/opt/rh/devtoolset-9/root/usr/include/c++/9/x86_64-redhat-linux;-isystem;-lpthread}

mkdir -p "${ARROW_BUILD_DIR}"
pushd "${ARROW_BUILD_DIR}"
  export ARROW_TEST_DATA="/arrow/testing/data"
  export PARQUET_TEST_DATA="/arrow/cpp/submodules/parquet-testing/data"
  export AWS_EC2_METADATA_DISABLED=TRUE

  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DARROW_DEPENDENCY_SOURCE="VCPKG" \
      -DCMAKE_INSTALL_PREFIX=/arrow-dist \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS} \
      -DARROW_BUILD_SHARED=ON \
      -DARROW_BOOST_USE_SHARED=OFF \
      -DARROW_PROTOBUF_USE_SHARED=OFF \
      -DARROW_OPENSSL_USE_SHARED=OFF \
      -DARROW_BROTLI_USE_SHARED=OFF \
      -DARROW_BZ2_USE_SHARED=OFF \
      -DARROW_GRPC_USE_SHARED=OFF \
      -DARROW_LZ4_USE_SHARED=OFF \
      -DARROW_SNAPPY_USE_SHARED=OFF \
      -DARROW_THRIFT_USE_SHARED=OFF \
      -DARROW_UTF8PROC_USE_SHARED=OFF \
      -DARROW_ZSTD_USE_SHARED=OFF \
      -DARROW_GANDIVA_PC_CXX_FLAGS=${GANDIVA_CXX_FLAGS} \
      -DARROW_JEMALLOC=${ARROW_JEMALLOC} \
      -DARROW_RPATH_ORIGIN=${ARROW_RPATH_ORIGIN} \
      -DARROW_PYTHON=${ARROW_PYTHON} \
      -DARROW_PARQUET=${ARROW_PARQUET} \
      -DARROW_DATASET=${ARROW_DATASET} \
      -DARROW_FILESYSTEM=${ARROW_FILESYSTEM} \
      -DPARQUET_REQUIRE_ENCRYPTION=OFF \
      -DPARQUET_BUILD_EXAMPLES=OFF \
      -DPARQUET_BUILD_EXECUTABLES=OFF \
      -DPythonInterp_FIND_VERSION=ON \
      -DPythonInterp_FIND_VERSION_MAJOR=3 \
      -DARROW_GANDIVA=${ARROW_GANDIVA} \
      -DARROW_GANDIVA_JAVA=${ARROW_GANDIVA_JAVA} \
      -DARROW_ORC=${ARROW_ORC} \
      -DARROW_JNI=${ARROW_JNI} \
      -DARROW_PLASMA=${ARROW_PLASMA} \
      -DARROW_PLASMA_JAVA_CLIENT=${ARROW_PLASMA_JAVA_CLIENT} \
      -Dgflags_SOURCE=BUNDLED \
      -DRapidJSON_SOURCE=BUNDLED \
      -DRE2_SOURCE=BUNDLED \
      -DARROW_BUILD_UTILITIES=OFF \
      -DVCPKG_MANIFEST_MODE=OFF \
      -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} \
      -GNinja /arrow/cpp
  ninja install
  CTEST_OUTPUT_ON_FAILURE=1 ninja test
popd

echo "=== (${PYTHON_VERSION}) Copying libraries to the distribution folder ==="
cp -L  /arrow-dist/lib/libgandiva_jni.so /arrow/dist
cp -L  /arrow-dist/lib/libarrow_dataset_jni.so /arrow/dist
cp -L  /arrow-dist/lib/libarrow_orc_jni.so /arrow/dist