#!/usr/bin/env bash
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

set -ex

arch=${1}
source_dir=${2}
build_dir=${3}

echo "=== (${PYTHON_VERSION}) Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}/build
rm -rf ${build_dir}/install
rm -rf ${source_dir}/python/dist
rm -rf ${source_dir}/python/build
rm -rf ${source_dir}/python/pyarrow/*.so
rm -rf ${source_dir}/python/pyarrow/*.so.*

echo "=== (${PYTHON_VERSION}) Set SDK, C++ and Wheel flags ==="
export _PYTHON_HOST_PLATFORM="macosx-${MACOSX_DEPLOYMENT_TARGET}-${arch}"
export MACOSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-10.14}
export SDKROOT=${SDKROOT:-$(xcrun --sdk macosx --show-sdk-path)}

if [ $arch = "arm64" ]; then
  export CMAKE_OSX_ARCHITECTURES="arm64"
elif [ $arch = "x86_64" ]; then
  export CMAKE_OSX_ARCHITECTURES="x86_64"
else
  echo "Unexpected architecture: $arch"
  exit 1
fi

echo "=== (${PYTHON_VERSION}) Install Python build dependencies ==="
export PIP_SITE_PACKAGES=$(python -c 'import site; print(site.getsitepackages()[0])')
export PIP_TARGET_PLATFORM="macosx_${MACOSX_DEPLOYMENT_TARGET//./_}_${arch}"

pip install \
  --upgrade \
  --only-binary=:all: \
  --target $PIP_SITE_PACKAGES \
  --platform $PIP_TARGET_PLATFORM \
  -r ${source_dir}/python/requirements-wheel-build.txt
pip install "delocate>=0.10.3"

echo "=== (${PYTHON_VERSION}) Building Arrow C++ libraries ==="
: ${ARROW_DATASET:=ON}
: ${ARROW_FLIGHT:=ON}
: ${ARROW_GANDIVA:=OFF}
: ${ARROW_GCS:=ON}
: ${ARROW_HDFS:=ON}
: ${ARROW_JEMALLOC:=ON}
: ${ARROW_MIMALLOC:=ON}
: ${ARROW_ORC:=ON}
: ${ARROW_PARQUET:=ON}
: ${PARQUET_REQUIRE_ENCRYPTION:=ON}
: ${ARROW_PLASMA:=ON}
: ${ARROW_SUBSTRAIT:=ON}
: ${ARROW_S3:=ON}
: ${ARROW_SIMD_LEVEL:="SSE4_2"}
: ${ARROW_TENSORFLOW:=ON}
: ${ARROW_WITH_BROTLI:=ON}
: ${ARROW_WITH_BZ2:=ON}
: ${ARROW_WITH_LZ4:=ON}
: ${ARROW_WITH_SNAPPY:=ON}
: ${ARROW_WITH_ZLIB:=ON}
: ${ARROW_WITH_ZSTD:=ON}
: ${CMAKE_BUILD_TYPE:=release}
: ${CMAKE_GENERATOR:=Ninja}
: ${CMAKE_UNITY_BUILD:=ON}
: ${VCPKG_ROOT:=/opt/vcpkg}
: ${VCPKG_FEATURE_FLAGS:=-manifests}
: ${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-osx-static-${CMAKE_BUILD_TYPE}}}

mkdir -p ${build_dir}/build
pushd ${build_dir}/build

cmake \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=${ARROW_DATASET} \
    -DARROW_DEPENDENCY_SOURCE="VCPKG" \
    -DARROW_DEPENDENCY_USE_SHARED=OFF \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=${ARROW_FLIGHT} \
    -DARROW_GANDIVA=${ARROW_GANDIVA} \
    -DARROW_GCS=${ARROW_GCS} \
    -DARROW_HDFS=${ARROW_HDFS} \
    -DARROW_JEMALLOC=${ARROW_JEMALLOC} \
    -DARROW_JSON=ON \
    -DARROW_MIMALLOC=${ARROW_MIMALLOC} \
    -DARROW_ORC=${ARROW_ORC} \
    -DARROW_PACKAGE_KIND="python-wheel-macos" \
    -DARROW_PARQUET=${ARROW_PARQUET} \
    -DARROW_PLASMA=${ARROW_PLASMA} \
    -DARROW_RPATH_ORIGIN=ON \
    -DARROW_S3=${ARROW_S3} \
    -DARROW_SIMD_LEVEL=${ARROW_SIMD_LEVEL} \
    -DARROW_SUBSTRAIT=${ARROW_SUBSTRAIT} \
    -DARROW_TENSORFLOW=${ARROW_TENSORFLOW} \
    -DARROW_USE_CCACHE=ON \
    -DARROW_WITH_BROTLI=${ARROW_WITH_BROTLI} \
    -DARROW_WITH_BZ2=${ARROW_WITH_BZ2} \
    -DARROW_WITH_LZ4=${ARROW_WITH_LZ4} \
    -DARROW_WITH_SNAPPY=${ARROW_WITH_SNAPPY} \
    -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB} \
    -DARROW_WITH_ZSTD=${ARROW_WITH_ZSTD} \
    -DCMAKE_APPLE_SILICON_PROCESSOR=arm64 \
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=${build_dir}/install \
    -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES} \
    -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
    -DORC_PROTOBUF_EXECUTABLE=${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/tools/protobuf/protoc \
    -DORC_SOURCE=BUNDLED \
    -DPARQUET_REQUIRE_ENCRYPTION=${PARQUET_REQUIRE_ENCRYPTION} \
    -DVCPKG_MANIFEST_MODE=OFF \
    -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} \
    -G ${CMAKE_GENERATOR} \
    ${source_dir}/cpp
cmake --build . --target install
popd

echo "=== (${PYTHON_VERSION}) Building wheel ==="
export PYARROW_BUILD_TYPE=${CMAKE_BUILD_TYPE}
export PYARROW_BUNDLE_ARROW_CPP=1
export PYARROW_CMAKE_GENERATOR=${CMAKE_GENERATOR}
export PYARROW_INSTALL_TESTS=1
export PYARROW_WITH_DATASET=${ARROW_DATASET}
export PYARROW_WITH_FLIGHT=${ARROW_FLIGHT}
export PYARROW_WITH_GANDIVA=${ARROW_GANDIVA}
export PYARROW_WITH_GCS=${ARROW_GCS}
export PYARROW_WITH_HDFS=${ARROW_HDFS}
export PYARROW_WITH_ORC=${ARROW_ORC}
export PYARROW_WITH_PARQUET=${ARROW_PARQUET}
export PYARROW_WITH_PARQUET_ENCRYPTION=${PARQUET_REQUIRE_ENCRYPTION}
export PYARROW_WITH_PLASMA=${ARROW_PLASMA}
export PYARROW_WITH_SUBSTRAIT=${ARROW_SUBSTRAIT}
export PYARROW_WITH_S3=${ARROW_S3}
export PYARROW_CMAKE_OPTIONS="-DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES} -DARROW_SIMD_LEVEL=${ARROW_SIMD_LEVEL}"
export ARROW_HOME=${build_dir}/install
# PyArrow build configuration
export CMAKE_PREFIX_PATH=${build_dir}/install
# Set PyArrow version explicitly
export SETUPTOOLS_SCM_PRETEND_VERSION=${PYARROW_VERSION}

pushd ${source_dir}/python
python setup.py bdist_wheel
popd

echo "=== (${PYTHON_VERSION}) Show dynamic libraries the wheel depend on ==="
deps=$(delocate-listdeps ${source_dir}/python/dist/*.whl)

if echo $deps | grep -v "^pyarrow/lib\(arrow\|gandiva\|parquet\|plasma\)"; then
  echo "There are non-bundled shared library dependencies."
  exit 1
fi

# Move the verified wheels
mkdir -p ${source_dir}/python/repaired_wheels
mv ${source_dir}/python/dist/*.whl ${source_dir}/python/repaired_wheels/
