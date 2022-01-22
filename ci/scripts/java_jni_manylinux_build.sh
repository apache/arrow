#!/usr/bin/env bash

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

set -ex

arrow_dir=${1}
build_dir=${2}
# The directory where the final binaries will be stored when scripts finish
dist_dir=${3}

echo "=== Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

echo "=== Building Arrow C++ libraries ==="
devtoolset_version=$(rpm -qa "devtoolset-*-gcc" --queryformat %{VERSION} | \
                       grep -o "^[0-9]*")
devtoolset_include_cpp="/opt/rh/devtoolset-${devtoolset_version}/root/usr/include/c++/${devtoolset_version}"
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
: ${ARROW_BUILD_TESTS:=OFF}
: ${CMAKE_BUILD_TYPE:=Release}
: ${CMAKE_UNITY_BUILD:=ON}
: ${VCPKG_ROOT:=/opt/vcpkg}
: ${VCPKG_FEATURE_FLAGS:=-manifests}
: ${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-linux-static-${CMAKE_BUILD_TYPE}}}
: ${GANDIVA_CXX_FLAGS:=-isystem;${devtoolset_include_cpp};-isystem;${devtoolset_include_cpp}/x86_64-redhat-linux;-isystem;-lpthread}

export ARROW_TEST_DATA="${arrow_dir}/testing/data"
export PARQUET_TEST_DATA="${arrow_dir}/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

# NOTE(kszucs): workaround for ARROW-15403 along with the ORC_* cmake variables
vcpkg remove orc

mkdir -p "${build_dir}"
pushd "${build_dir}"

cmake \
  -DARROW_BOOST_USE_SHARED=OFF \
  -DARROW_BROTLI_USE_SHARED=OFF \
  -DARROW_BUILD_SHARED=ON \
  -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS} \
  -DARROW_BUILD_UTILITIES=OFF \
  -DARROW_BZ2_USE_SHARED=OFF \
  -DARROW_DATASET=${ARROW_DATASET} \
  -DARROW_DEPENDENCY_SOURCE="VCPKG" \
  -DARROW_FILESYSTEM=${ARROW_FILESYSTEM} \
  -DARROW_GANDIVA_JAVA=${ARROW_GANDIVA_JAVA} \
  -DARROW_GANDIVA_PC_CXX_FLAGS=${GANDIVA_CXX_FLAGS} \
  -DARROW_GANDIVA=${ARROW_GANDIVA} \
  -DARROW_GRPC_USE_SHARED=OFF \
  -DARROW_JEMALLOC=${ARROW_JEMALLOC} \
  -DARROW_JNI=ON \
  -DARROW_LZ4_USE_SHARED=OFF \
  -DARROW_OPENSSL_USE_SHARED=OFF \
  -DARROW_ORC=${ARROW_ORC} \
  -DARROW_PARQUET=${ARROW_PARQUET} \
  -DARROW_PLASMA_JAVA_CLIENT=${ARROW_PLASMA_JAVA_CLIENT} \
  -DARROW_PLASMA=${ARROW_PLASMA} \
  -DARROW_PROTOBUF_USE_SHARED=OFF \
  -DARROW_PYTHON=${ARROW_PYTHON} \
  -DARROW_RPATH_ORIGIN=${ARROW_RPATH_ORIGIN} \
  -DARROW_SNAPPY_USE_SHARED=OFF \
  -DARROW_THRIFT_USE_SHARED=OFF \
  -DARROW_UTF8PROC_USE_SHARED=OFF \
  -DARROW_ZSTD_USE_SHARED=OFF \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_INSTALL_PREFIX=${build_dir} \
  -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
  -DORC_SOURCE=BUNDLED \
  -DORC_PROTOBUF_EXECUTABLE=${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/tools/protobuf/protoc \
  -DPARQUET_BUILD_EXAMPLES=OFF \
  -DPARQUET_BUILD_EXECUTABLES=OFF \
  -DPARQUET_REQUIRE_ENCRYPTION=OFF \
  -DPythonInterp_FIND_VERSION_MAJOR=3 \
  -DPythonInterp_FIND_VERSION=ON \
  -DVCPKG_MANIFEST_MODE=OFF \
  -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} \
  -GNinja \
  ${arrow_dir}/cpp
ninja install

if [ $ARROW_BUILD_TESTS = "ON" ]; then
    ctest \
        --label-regex unittest \
        --output-on-failure \
        --parallel $(nproc) \
        --timeout 300
fi

popd

echo "=== Copying libraries to the distribution folder ==="
mkdir -p "${dist_dir}"
cp -L ${build_dir}/lib/libgandiva_jni.so ${dist_dir}
cp -L ${build_dir}/lib/libarrow_dataset_jni.so ${dist_dir}
cp -L ${build_dir}/lib/libarrow_orc_jni.so ${dist_dir}

echo "=== Checking shared dependencies for libraries ==="

pushd ${dist_dir}
archery linking check-dependencies \
  --allow ld-linux-x86-64 \
  --allow libc \
  --allow libdl \
  --allow libgcc_s \
  --allow libm \
  --allow libpthread \
  --allow librt \
  --allow libstdc++ \
  --allow libz \
  --allow linux-vdso \
  libgandiva_jni.so \
  libarrow_dataset_jni.so \
  libarrow_orc_jni.so
popd
