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
  aarch64)
    normalized_arch=aarch_64
    ;;
esac
# The directory where the final binaries will be stored when scripts finish
dist_dir=${3}

echo "=== Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

echo "=== Building Arrow C++ libraries ==="
: ${ARROW_ACERO:=ON}
export ARROW_ACERO
: ${ARROW_BUILD_TESTS:=ON}
: ${ARROW_DATASET:=ON}
export ARROW_DATASET
: ${ARROW_GANDIVA:=ON}
export ARROW_GANDIVA
: ${ARROW_GCS:=ON}
: ${ARROW_JEMALLOC:=ON}
: ${ARROW_RPATH_ORIGIN:=ON}
: ${ARROW_ORC:=ON}
export ARROW_ORC
: ${ARROW_PARQUET:=ON}
: ${ARROW_S3:=ON}
: ${ARROW_USE_CCACHE:=OFF}
: ${CMAKE_BUILD_TYPE:=release}
: ${CMAKE_UNITY_BUILD:=ON}
: ${VCPKG_ROOT:=/opt/vcpkg}
: ${VCPKG_FEATURE_FLAGS:=-manifests}
: ${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-linux-static-${CMAKE_BUILD_TYPE}}}
: ${GANDIVA_CXX_FLAGS:=-isystem;${devtoolset_include_cpp};-isystem;${devtoolset_include_cpp}/x86_64-redhat-linux;-isystem;-lpthread}

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
  -DARROW_BUILD_TESTS=ON \
  -DARROW_CSV=${ARROW_DATASET} \
  -DARROW_DATASET=${ARROW_DATASET} \
  -DARROW_SUBSTRAIT=${ARROW_DATASET} \
  -DARROW_DEPENDENCY_SOURCE="VCPKG" \
  -DARROW_DEPENDENCY_USE_SHARED=OFF \
  -DARROW_GANDIVA_PC_CXX_FLAGS=${GANDIVA_CXX_FLAGS} \
  -DARROW_GANDIVA=${ARROW_GANDIVA} \
  -DARROW_GCS=${ARROW_GCS} \
  -DARROW_JEMALLOC=${ARROW_JEMALLOC} \
  -DARROW_ORC=${ARROW_ORC} \
  -DARROW_PARQUET=${ARROW_PARQUET} \
  -DARROW_RPATH_ORIGIN=${ARROW_RPATH_ORIGIN} \
  -DARROW_S3=${ARROW_S3} \
  -DARROW_USE_CCACHE=${ARROW_USE_CCACHE} \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DCMAKE_INSTALL_PREFIX=${ARROW_HOME} \
  -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
  -DGTest_SOURCE=BUNDLED \
  -DORC_SOURCE=BUNDLED \
  -DORC_PROTOBUF_EXECUTABLE=${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/tools/protobuf/protoc \
  -DPARQUET_BUILD_EXAMPLES=OFF \
  -DPARQUET_BUILD_EXECUTABLES=OFF \
  -DPARQUET_REQUIRE_ENCRYPTION=OFF \
  -DVCPKG_MANIFEST_MODE=OFF \
  -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} \
  -GNinja \
  ${arrow_dir}/cpp
ninja install

if [ "${ARROW_BUILD_TESTS}" = "ON" ]; then
  # MinIO is required
  exclude_tests="arrow-s3fs-test"
  case $(arch) in
    aarch64)
      # GCS testbench is crashed on aarch64:
      # ImportError: ../grpc/_cython/cygrpc.cpython-38-aarch64-linux-gnu.so:
      # undefined symbol: vtable for std::__cxx11::basic_ostringstream<
      #   char, std::char_traits<char>, std::allocator<char> >
      exclude_tests="${exclude_tests}|arrow-gcsfs-test"
      ;;
  esac
  # unstable
  exclude_tests="${exclude_tests}|arrow-acero-asof-join-node-test"
  exclude_tests="${exclude_tests}|arrow-acero-hash-join-node-test"
  # strptime
  exclude_tests="${exclude_tests}|arrow-utility-test"
  ctest \
    --exclude-regex "${exclude_tests}" \
    --label-regex unittest \
    --output-on-failure \
    --parallel $(nproc) \
    --timeout 300
fi

popd


JAVA_JNI_CMAKE_ARGS=""
JAVA_JNI_CMAKE_ARGS="${JAVA_JNI_CMAKE_ARGS} -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
JAVA_JNI_CMAKE_ARGS="${JAVA_JNI_CMAKE_ARGS} -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}"
export JAVA_JNI_CMAKE_ARGS
${arrow_dir}/ci/scripts/java_jni_build.sh \
  ${arrow_dir} \
  ${ARROW_HOME} \
  ${build_dir} \
  ${dist_dir}

if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
  echo "=== ccache statistics after build ==="
  ccache -sv 2>/dev/null || ccache -s
fi


echo "=== Checking shared dependencies for libraries ==="
pushd ${dist_dir}
archery linking check-dependencies \
  --allow ld-linux-aarch64 \
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
  arrow_cdata_jni/${normalized_arch}/libarrow_cdata_jni.so \
  arrow_dataset_jni/${normalized_arch}/libarrow_dataset_jni.so \
  arrow_orc_jni/${normalized_arch}/libarrow_orc_jni.so \
  gandiva_jni/${normalized_arch}/libgandiva_jni.so
popd
