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

arrow_dir=${1}
build_dir=${2}
# The directory where the final binaries will be stored when scripts finish
distribution_dir=${3}
source_dir=${arrow_dir}/cpp

echo "=== (${PYTHON_VERSION}) Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

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

mkdir -p "${build_dir}"
pushd "${build_dir}"
  export ARROW_TEST_DATA="${arrow_dir}/testing/data"
  export PARQUET_TEST_DATA="${source_dir}/submodules/parquet-testing/data"
  export AWS_EC2_METADATA_DISABLED=TRUE

  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DARROW_DEPENDENCY_SOURCE="VCPKG" \
      -DCMAKE_INSTALL_PREFIX=${build_dir} \
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
      -DARROW_BUILD_UTILITIES=OFF \
      -DVCPKG_MANIFEST_MODE=OFF \
      -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} \
      -GNinja ${source_dir}
  ninja install
  CTEST_OUTPUT_ON_FAILURE=1 ninja test
popd

echo "=== (${PYTHON_VERSION}) Copying libraries to the distribution folder ==="
mkdir -p "${distribution_dir}"
cp -L  ${build_dir}/lib/libgandiva_jni.so ${distribution_dir}
cp -L  ${build_dir}/lib/libarrow_dataset_jni.so ${distribution_dir}
cp -L  ${build_dir}/lib/libarrow_orc_jni.so ${distribution_dir}

echo "=== (${PYTHON_VERSION}) Checking shared dependencies for libraries ==="
source $arrow_dir/ci/scripts/java_bundled_jars_check_dependencies.sh
SO_DEP=ldd

GANDIVA_LIB=$distribution_dir/libgandiva_jni.so
DATASET_LIB=$distribution_dir/libarrow_dataset_jni.so
ORC_LIB=$distribution_dir/libarrow_orc_jni.so
LIBRARIES=($GANDIVA_LIB $ORC_LIB $DATASET_LIB)

WHITELIST=(linux-vdso libz librt libdl libpthread libstdc++ libm libgcc_s libc ld-linux-x86-64)

for library in "${LIBRARIES[@]}"
do
  check_dynamic_dependencies $SO_DEP $library "${WHITELIST[@]}"  
done