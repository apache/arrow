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
normalized_arch=$(arch)
case ${normalized_arch} in
  arm64)
    normalized_arch=aarch_64
    ;;
  i386)
    normalized_arch=x86_64
    ;;
esac
# The directory where the final binaries will be stored when scripts finish
dist_dir=${3}/${normalized_arch}

echo "=== Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

echo "=== Building Arrow C++ libraries ==="
install_dir=${build_dir}/cpp-install
: ${ARROW_ACERO:=ON}
export ARROW_ACERO
: ${ARROW_BUILD_TESTS:=ON}
: ${ARROW_DATASET:=ON}
export ARROW_DATASET
: ${ARROW_GANDIVA:=ON}
export ARROW_GANDIVA
: ${ARROW_ORC:=ON}
export ARROW_ORC
: ${ARROW_PARQUET:=ON}
: ${ARROW_S3:=ON}
: ${ARROW_USE_CCACHE:=OFF}
: ${CMAKE_BUILD_TYPE:=Release}
: ${CMAKE_UNITY_BUILD:=ON}

if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
  echo "=== ccache statistics before build ==="
  ccache -sv 2>/dev/null || ccache -s
fi

export ARROW_TEST_DATA="${arrow_dir}/testing/data"
export PARQUET_TEST_DATA="${arrow_dir}/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

mkdir -p "${build_dir}/cpp"
pushd "${build_dir}/cpp"

cmake \
  -DARROW_ACERO=${ARROW_ACERO} \
  -DARROW_BUILD_SHARED=OFF \
  -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS} \
  -DARROW_CSV=${ARROW_DATASET} \
  -DARROW_DATASET=${ARROW_DATASET} \
  -DARROW_SUBSTRAIT=${ARROW_DATASET} \
  -DARROW_DEPENDENCY_USE_SHARED=OFF \
  -DARROW_GANDIVA=${ARROW_GANDIVA} \
  -DARROW_GANDIVA_STATIC_LIBSTDCPP=ON \
  -DARROW_ORC=${ARROW_ORC} \
  -DARROW_PARQUET=${ARROW_PARQUET} \
  -DARROW_S3=${ARROW_S3} \
  -DARROW_USE_CCACHE=${ARROW_USE_CCACHE} \
  -DAWSSDK_SOURCE=BUNDLED \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_INSTALL_PREFIX=${install_dir} \
  -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
  -DGTest_SOURCE=BUNDLED \
  -DPARQUET_BUILD_EXAMPLES=OFF \
  -DPARQUET_BUILD_EXECUTABLES=OFF \
  -DPARQUET_REQUIRE_ENCRYPTION=OFF \
  -Dre2_SOURCE=BUNDLED \
  -GNinja \
  ${arrow_dir}/cpp
cmake --build . --target install

if [ "${ARROW_BUILD_TESTS}" == "ON" ]; then
  # MinIO is required
  exclude_tests="arrow-s3fs-test"
  # unstable
  exclude_tests="${exclude_tests}|arrow-acero-hash-join-node-test"
  ctest \
    --exclude-regex "${exclude_tests}" \
    --label-regex unittest \
    --output-on-failure \
    --parallel $(sysctl -n hw.ncpu) \
    --timeout 300
fi

popd

export JAVA_JNI_CMAKE_ARGS="-DProtobuf_ROOT=${build_dir}/cpp/protobuf_ep-install"
${arrow_dir}/ci/scripts/java_jni_build.sh \
  ${arrow_dir} \
  ${install_dir} \
  ${build_dir} \
  ${dist_dir}

if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
  echo "=== ccache statistics after build ==="
  ccache -sv 2>/dev/null || ccache -s
fi


echo "=== Checking shared dependencies for libraries ==="
pushd ${dist_dir}
archery linking check-dependencies \
  --allow CoreFoundation \
  --allow Security \
  --allow libSystem \
  --allow libarrow_cdata_jni \
  --allow libarrow_dataset_jni \
  --allow libarrow_orc_jni \
  --allow libc++ \
  --allow libcurl \
  --allow libgandiva_jni \
  --allow libncurses \
  --allow libobjc \
  --allow libz \
  libarrow_cdata_jni.dylib \
  libarrow_dataset_jni.dylib \
  libarrow_orc_jni.dylib \
  libgandiva_jni.dylib
popd
